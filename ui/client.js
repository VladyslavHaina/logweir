// client.js -- which API is in front of this page, decided ONCE, and the one
// object every page reads through.
//
// ONE DEPLOYMENT. The page is served by `logweir-api` and talks to its bounded
// product API at `/api/v1/...` on the same origin -- CONSOLE MODE. The legacy
// `kubectl proxy` mode was withdrawn in v1.
//
// THE STATE IS DECIDED ONCE, BY ONE REQUEST. At boot the page asks
// `GET /api/v1/session`. An answer that decodes as a session document is
// console mode, and that document is also where the namespace grants come
// from. A sign-in refusal is signed out; anything else -- a body that is not a
// session, no answer inside the bound below -- is the service not answering.
// The result is recorded in this module and never asked again: a state decided
// per request is a page that can change APIs between a read and the write that
// follows it.
//
// NO TOKEN IS EVER STORED IN THE BROWSER. Console mode authenticates with a
// session cookie the browser attaches by itself to a same-origin request; this
// page neither reads nor writes it. The synchroniser token the session
// document carries is held in THIS MODULE'S MEMORY for the life of the loaded
// page -- the same place a draft lives -- and is gone on reload. There is no
// browser storage anywhere in this tree, and `scripts/check-ui-offline.sh`
// fails the build over the byte sequences that would introduce one.
//
// WHAT A PAGE SEES. `{list, get, create, patchSuspend, listCluster}` over
// Kubernetes-shaped custom resources: the product DTO is decoded strictly and
// then PROJECTED BACK onto the custom resource's shape, because the product
// API's objects ARE projections of those resources and the pages are views of
// them.
// Every field the projection cannot supply is named in `ui/README.md` and
// recorded on the object under `__contract`, never quietly defaulted.
//
// THIS MODULE ISSUES NO REQUEST OF ITS OWN. Every identifier is built and
// every response is read by `api.js`, which holds the one `fetch` in the tree.

import {
  cadencePreview,
  consoleAction,
  consoleApprovalPolicy,
  consoleApprovalRequest,
  consoleCreate,
  consoleGet,
  consoleList,
  consoleLogout,
  consoleOperation,
  consoleSchedulePolicy,
  consoleSetSuspension,
  consoleSub,
  session,
} from "./api.js";
import {
  contractFailure,
  decodeApprovalPacket,
  decodeApprovalPolicy,
  decodeApprovalRequest,
  decodeCadencePreview,
  decodeCancel,
  decodeCheckOperation,
  decodeConsoleItem,
  decodeConsoleList,
  decodeDestinationUsage,
  decodeDetailPage,
  decodeDiscoveryLatest,
  decodeManualBackup,
  decodeOperation,
  decodePolicyChanged,
  decodeRequest,
  decodeSession,
  decodeTopicPage,
  isContractFailure,
} from "./contract.js";
import { preparedFor } from "./plan.js";
import { SIGN_IN_CODES, basisAllowsGreen, TRUST_BASIS_NOT_OBSERVED } from "./render.js";
import { causesFrom, validateRequest } from "./validate.js";

/** The mode a page with a session is in. */
export const CONSOLE = "console";

/** THE SHARED CONSOLE WITH NOBODY SIGNED IN. Not a third API: the page IS
 *  behind `logweir-api`, and `GET /api/v1/session` said so by answering `401`
 *  with its own problem document. In this mode the shell renders a sign-in
 *  page on every route and no page is mounted; every read and write is
 *  refused here, by name, before the network. */
export const SIGNED_OUT = "signedOut";

/** The two problem codes a `401` from `/api/v1/session` carries (`docs/api.md`,
 *  *The session and the CSRF token*): no session cookie at all, and one that
 *  expired or no longer authenticates. Anything else -- an HTML page, a body
 *  that is not a problem document -- is NOT the product API asking for a
 *  sign-in. */
export const SIGNED_OUT_CODES = SIGN_IN_CODES;

/** THE CONSOLE THAT COULD NOT BE REACHED. The session
 *  probe did not give the page a session or a sign-in refusal: it timed out,
 *  failed on the network, answered a 5xx or a 429, or answered a body this
 *  page could not decode. That is "the service is not answering right now",
 *  and the shell says so with a Retry. */
export const UNAVAILABLE = "unavailable";

/** How long the boot probe waits before deciding the service is not
 *  answering. A missing answer cannot be allowed to hang the shell. */
export const PROBE_TIMEOUT_MS = 5000;

/** The Kubernetes plural each product plural stands for, and back. ONE table,
 *  so a page names `kafkaclusters` and nothing else translates. */
const CONSOLE_PLURAL = Object.freeze({
  kafkaclusters: "connections",
  backupschedules: "schedules",
  backups: "backups",
  restores: "restores",
  approvals: "approvals",
});

const KIND_OF = Object.freeze({
  kafkaclusters: "KafkaCluster",
  backupschedules: "BackupSchedule",
  backups: "Backup",
  restores: "Restore",
  approvals: "Approval",
});

/** The capability flag each read needs, so a page is refused by NAME rather
 *  than by a 403 from a route the grant never covered. */
const READ_CAPABILITY = Object.freeze({
  connections: "connectionsRead",
  schedules: "schedulesRead",
  backups: "backupsRead",
  restores: "restoresRead",
  approvals: "approvalsRead",
});

const CREATE_CAPABILITY = Object.freeze({
  connections: "connectionCreate",
  schedules: "scheduleCreate",
  restores: "restoreCreate",
});

let decided = null;
let probing = null;

/** The mode, or `null` before the boot probe has answered. */
export function mode() {
  return decided === null ? null : decided.mode;
}

/** The decoded session document in console mode, `null` in every other state
 *  and before the probe. Never carries a credential: the product API's session
 *  document carries an actor, grants and flags. */
export function sessionDocument() {
  return decided === null ? null : decided.session;
}

/** The namespaces this actor is granted, from `/session` in console mode, and
 *  an empty list in every other state. */
export function grantedNamespaces() {
  return decided === null ? [] : decided.namespaces.slice();
}

/** Whether a signed-in console session holds NO product role in any
 *  namespace. False outside console mode, before the probe, and for the
 *  localAdmin actor, who is the administrator by construction. */
export function holdsNoRole() {
  if (decided === null || decided.mode !== CONSOLE || decided.session === null) {
    return false;
  }
  if (decided.session.authenticationMode === "localAdmin") {
    return false;
  }
  return Object.keys(decided.roles || {}).every((ns) => (decided.roles[ns] || []).length === 0);
}

/** The PRODUCT roles the actor holds in `ns`, as the session reported them.
 *
 *  Empty outside console mode and in localAdmin mode, and empty is not "none of the
 *  above": it is a mode with no roles at all. Nothing on this page branches on
 *  a role -- `granted` below is what decides whether a call is made, because a
 *  capability flag is `implemented && allowed` and a role is neither. This is
 *  here so a view can SAY who the actor is without guessing it from the flags. */
export function rolesFor(ns) {
  const held = decided === null ? undefined : decided.roles[ns];
  return held === undefined ? [] : held.slice();
}

/** The revision of the administrator's binding table that produced the current
 *  grants, or the empty string when there is none (localAdmin). The
 *  product API records it in every audit line; carrying it here is what lets a
 *  reader tie what this page shows to the configuration that allowed it. */
export function bindingRevision() {
  return decided === null ? "" : decided.bindingRevision;
}

/** Whether `flag` is granted for `ns`: the session's grant in console mode,
 *  nothing signed out or unreachable, and everything before the probe has
 *  answered (every call waits for it, so nothing is sent on that answer). */
export function granted(ns, flag) {
  if (decided !== null && (decided.mode === SIGNED_OUT || decided.mode === UNAVAILABLE)) {
    return false;
  }
  if (decided === null) {
    return true;
  }
  const grant = decided.grants[ns];
  return grant !== undefined && grant[flag] === true;
}

/** Forgets the recorded mode. THE SUITE'S SEAM, and nothing else calls it: the
 *  page decides once per load, and a page that could re-decide would be a page
 *  whose reads and writes can disagree about which API they are talking to. */
export function resetMode() {
  decided = null;
  probing = null;
}

/** Decides the mode, once. Concurrent callers share the one probe; every later
 *  caller reads the record.
 *
 *  `deps.probe` replaces the one request for the suite. `deps.controller`
 *  replaces the abort controller the bound below is built on. */
export function selectMode(deps) {
  if (decided !== null) {
    return Promise.resolve(decided);
  }
  if (probing !== null) {
    return probing;
  }
  probing = probe(deps || {}).then((record) => {
    decided = record;
    probing = null;
    return record;
  });
  return probing;
}

/** The record, waiting for the probe when it has not answered yet. Every
 *  method of the client object below starts here, so no request can be issued
 *  before the mode is known and none can change it afterwards. */
async function ensure() {
  return decided !== null ? decided : selectMode();
}

async function probe(deps) {
  const ask = typeof deps.probe === "function" ? deps.probe : session;
  const Controller = deps.controller || globalThis.AbortController;
  let controller = null;
  let timer = null;
  try {
    let options;
    // THE BOUND IS A RACE AND NOT ONLY AN ABORT. Aborting the request is the
    // right thing to do and it is not enough: a server that accepted the
    // connection and answers nothing leaves the promise pending whatever this
    // page asked of it, and the shell would never render. So the timer
    // RESOLVES the decision -- the service is not answering -- and aborts the
    // request on its way out.
    if (typeof Controller === "function") {
      controller = new Controller();
      options = { signal: controller.signal };
    }
    const asked = ask(options);
    const bounded = new Promise((resolve) => {
      timer = globalThis.setTimeout(() => {
        if (controller !== null) {
          controller.abort();
        }
        resolve(null);
      }, typeof deps.timeoutMs === "number" ? deps.timeoutMs : PROBE_TIMEOUT_MS);
    });
    const answer = await Promise.race([asked, bounded]);
    if (answer !== null && answer !== undefined && answer.ok !== true &&
      signedOutCode(answer) !== null) {
      return signedOutRecord(signedOutCode(answer));
    }
    if (answer === null || answer === undefined || answer.ok !== true) {
      // A MISSING SESSION IS AN OUTAGE AND NOT A MODE.
      return unavailableRecord(answer);
    }
    const decoded = decodeSession(answer.body);
    return consoleRecord(decoded.value, decoded.unknown);
  } catch (refused) {
    // A body that is not a session document, a decode that found a required
    // field missing, a failed request: each is the service not answering.
    return unavailableRecord(null, refused);
  } finally {
    if (timer !== null) {
      globalThis.clearTimeout(timer);
    }
  }
}

/** The problem code of a `401` the PRODUCT API gave `/session`, or `null`.
 *
 *  Read narrowly on purpose: a `401` alone is not enough. The product API's
 *  refusal is a problem document whose `code` is one of
 *  [`SIGNED_OUT_CODES`]. */
export function signedOutCode(answer) {
  const a = answer || {};
  const body = a.body;
  if (a.status !== 401 || body === null || typeof body !== "object") {
    return null;
  }
  return typeof body.code === "string" && SIGNED_OUT_CODES.indexOf(body.code) !== -1
    ? body.code
    : null;
}

function signedOutRecord(code) {
  return Object.freeze({
    mode: SIGNED_OUT,
    session: null,
    namespaces: Object.freeze([]),
    grants: Object.freeze(Object.create(null)),
    roles: Object.freeze(Object.create(null)),
    bindingRevision: "",
    token: null,
    unknown: Object.freeze([]),
    // Which of the two refusals it was, so the sign-in page can say "your
    // session ended" rather than "you are not signed in" when that is true.
    signedOut: code,
  });
}

// What the probe got instead of a session, in words for the shell's page:
// the status and the problem document's own code and detail when there was an
// answer, "did not answer in time" when the bound fired, and the error's own
// message when the request itself failed. Never a credential: the probe
// carries none.
function unavailableRecord(answer, error) {
  let said;
  if (answer === null || answer === undefined) {
    said = error === undefined || error === null
      ? "the service did not answer within " + String(PROBE_TIMEOUT_MS / 1000) + " seconds"
      : (error.kind === "contract" || error.contract !== undefined
        ? "the service answered with a session this page could not read"
        : "the request failed: " + String(error.message || error));
  } else {
    const body = answer.body !== null && typeof answer.body === "object" ? answer.body : {};
    said = "the service answered " + String(answer.status) +
      (typeof body.code === "string" ? " " + body.code : "") +
      (typeof body.detail === "string" && body.detail.length > 0 ? ": " + body.detail : "");
  }
  return Object.freeze({
    mode: UNAVAILABLE,
    session: null,
    namespaces: Object.freeze([]),
    grants: Object.freeze(Object.create(null)),
    roles: Object.freeze(Object.create(null)),
    bindingRevision: "",
    token: null,
    unknown: Object.freeze([]),
    unavailable: said,
  });
}

/** Why the console could not start, in words, when it could not; `null` in
 *  every other state. */
export function unavailableReason() {
  return decided === null || decided.mode !== UNAVAILABLE ? null : decided.unavailable;
}

/** `unauthenticated` or `session_expired` when the shared console has nobody
 *  signed in, and `null` in every other mode and before the probe. */
export function signedOutReason() {
  return decided === null || decided.mode !== SIGNED_OUT ? null : decided.signedOut;
}

/** WHO THIS PAGE IS SIGNED IN AS, for the header, or `null` when there
 *  is no session to describe (signed out, unreachable, before the probe).
 *
 *  Every value is the session document's own: the display claim, the subject,
 *  the authentication mode and the product roles the binding table granted in
 *  `ns`. Nothing is inferred from capability flags. */
export function sessionIdentity(ns) {
  if (decided === null || decided.mode !== CONSOLE || decided.session === null) {
    return null;
  }
  const actor = decided.session.actor || {};
  const display = typeof actor.displayName === "string" && actor.displayName.length > 0
    ? actor.displayName
    : String(actor.subject || actor.id || "");
  return Object.freeze({
    displayName: display,
    subject: String(actor.subject || ""),
    // The `<issuer>#<subject>` an Approval names its approver by, so a page
    // can say "you" for the signed-in actor's own principal.
    actorId: String(actor.id || ""),
    authenticationMode: String(decided.session.authenticationMode || ""),
    roles: Object.freeze(rolesFor(ns)),
    // NO ROLE ANYWHERE: the header says what the no-role card says, instead
    // of asking for a namespace no choice will give a role in.
    noRole: holdsNoRole(),
    namespace: typeof ns === "string" ? ns : "",
    // A localAdmin console has no sign-in and no sign-out: the one actor is the
    // administrator who started it on loopback (`docs/api.md`).
    canSignOut: decided.session.authenticationMode === "oidc" && decided.token !== null,
  });
}

/** Whether the session publishes `flag` in ANY granted namespace (the
 *  document's top-level `capabilities`, the union the API computes); `false`
 *  signed out or unreachable. For the NAVIGATION only: a hidden tab is a
 *  convenience, and every route still refuses by name when it is reached. */
export function grantedAnywhere(flag) {
  if (decided === null) {
    return true;
  }
  if (decided.mode === SIGNED_OUT || decided.mode === UNAVAILABLE) {
    return false;
  }
  const top = (decided.session || {}).capabilities || {};
  return top[flag] === true;
}

/** SIGN OUT: `POST /api/v1/session/logout`, an unsafe method, so it
 *  carries the session's synchroniser token exactly as every other write does
 *  (`docs/api.md`: "it needs the exact `Origin`, `application/json` and the
 *  token like any other mutation"). Resolves once the product API has cleared
 *  the cookie; the caller reloads, and the next probe answers `401`. Refused
 *  by name in any mode with no session to end. */
export async function signOut() {
  if (decided === null || decided.mode !== CONSOLE || decided.token === null) {
    throw noRoute("there is no signed-in session on this page to sign out of.");
  }
  await consoleLogout({ token: tokenNow() });
  return true;
}

/** The refusal every call gets while nobody is signed in. `status` is 401, the
 *  answer the product API would give, and nothing was sent. */
export function signedOutError() {
  const error = new Error(
    "You are not signed in to the Logweir console, so nothing was read or sent. Sign in to " +
      "continue.",
  );
  error.kind = "refused";
  error.status = 401;
  error.reason = "unauthenticated";
  error.code = "unauthenticated";
  return error;
}

function consoleRecord(document, unknown) {
  const grants = Object.create(null);
  const roles = Object.create(null);
  const names = [];
  for (const grant of document.namespaces) {
    grants[grant.name] = grant.capabilities;
    // THE ROLES COME FROM THE SESSION AND ARE NEVER DERIVED HERE.
    // A page that inferred "this actor is an operator" from which capability
    // flags happen to be true would be inventing an authorisation decision the
    // server already made, and would invent a different one the moment a
    // domain's route lands. `roles` is what the binding table said; the flags
    // remain what this page BRANCHES on, because a flag is `implemented &&
    // allowed` and a role is not.
    roles[grant.name] = Object.freeze(grant.roles.slice());
    names.push(grant.name);
  }
  return Object.freeze({
    mode: CONSOLE,
    session: document,
    namespaces: Object.freeze(names),
    grants: Object.freeze(grants),
    roles: Object.freeze(roles),
    // Empty in localAdmin mode, which has no binding table.
    bindingRevision: document.bindingRevision,
    // IN MEMORY, FOR THE LIFE OF THE LOADED PAGE, AND NOWHERE ELSE.
    token: typeof document.csrfToken === "string" ? document.csrfToken : null,
    unknown: Object.freeze(unknown.slice()),
  });
}

/** Copies the session's namespace grants into a namespace context the shell
 *  already holds, and says whether anything changed. Console mode's grants
 *  REPLACE whatever the runtime file said: in that mode the server knows what
 *  this actor may reach and a page-side list would be a second answer to a
 *  question that has one. */
export function applyGrants(context, names) {
  if (context === null || typeof context !== "object" || !Array.isArray(names) ||
    names.length === 0) {
    return false;
  }
  const before = (context.allowed || []).join(",");
  if (before === names.join(",")) {
    return false;
  }
  context.allowed = names.slice();
  if (typeof context.selected !== "string" || names.indexOf(context.selected) === -1) {
    context.selected = names.length === 1 ? names[0] : "";
  }
  return true;
}

/** An error for something this page will not attempt, named rather than sent.
 *  `status` is 0 because nothing was asked of any server. */
function noRoute(said) {
  const error = new Error(said);
  error.kind = "refused";
  error.status = 0;
  error.reason = "NoConsoleRoute";
  return error;
}

function notGranted(ns, flag) {
  const error = new Error(
    "this session is not granted " + flag + " in namespace " + String(ns) +
      ". The grants come from the product API's own session document; nothing on this page " +
      "decides them.",
  );
  error.kind = "rejected";
  error.status = 403;
  error.reason = "namespace_forbidden";
  return error;
}

/** Attaches the problem document's field errors to an error in the shape
 *  `lifecycle.js`'s `fieldErrors` already reads, with every path translated
 *  into the custom resource's vocabulary. The messages are the server's own,
 *  verbatim. */
function withCauses(error, plural) {
  const problem = (error || {}).problem;
  if (problem === null || problem === undefined || !Array.isArray(problem.errors)) {
    return error;
  }
  const causes = causesFrom(plural, problem.errors);
  if (causes.length > 0) {
    error.details = { causes: causes };
  }
  return error;
}

// THE KEY IS A FUNCTION OF WHAT IS BEING CREATED, NOT OF WHEN. A retry after a
// lost response, a double click and a reload all compose the same key, so the
// product API answers the second request with the object the first one made.
// A DELIBERATELY NEW operation is a new name, and a new name is a new key.
function idempotencyKey(plural, ns, name) {
  const composed = "logweir-ui." + plural + "." + String(ns) + "." + String(name);
  if (composed.length <= 128) {
    return composed;
  }
  // The rare long name, shortened deterministically rather than truncated:
  // truncation makes two different names one key. The scope is already bound
  // server side to the actor, the namespace and the route, so what has to be
  // distinct here is the name alone.
  return "logweir-ui." + plural + "." + String(ns) + ".h" + digest32(String(name));
}

// FNV-1a over the name, as eight lowercase hex digits. Not a security hash and
// not asked to be one: it distinguishes names within one namespace and one
// route, and the server's own scope hash does the rest.
function digest32(text) {
  let hash = 0x811c9dc5;
  for (let i = 0; i < text.length; i += 1) {
    hash = (hash ^ text.charCodeAt(i)) >>> 0;
    hash = (hash + ((hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24))) >>> 0;
  }
  return hash.toString(16).padStart(8, "0");
}

function meta(item, kind) {
  const m = {
    name: item.name,
    namespace: item.namespace,
    uid: item.uid,
    resourceVersion: item.resourceVersion,
  };
  if (item.createdAt !== null) {
    m.creationTimestamp = item.createdAt;
  }
  return { apiVersion: "logweir.dev/v1alpha1", kind: kind, metadata: m };
}

function nameRef(ref) {
  return ref === null || ref === undefined ? null : { name: ref.name };
}

function archive(view) {
  const out = { url: view.url };
  const ref = nameRef(view.credentialRef);
  if (ref !== null) {
    out.secretRef = ref;
  }
  return out;
}

// THE ONE PLACE A NORMALIZED OPERATION STATE BECOMES A WORD ON THE PAGE. Four
// of the ten states are the custom resource's own phase, byte for byte. The
// other six have no phase to be: they are distinctions `logweir-api`'s status
// normalization draws that the resource does not record, and this table shows
// the API's own word rather than rounding it to a phase the controller never
// wrote. `schemas/logweir-api-v1.openapi.json` lists all ten.
const PHASE_OF = Object.freeze({
  pending: "Pending",
  running: "Running",
  succeeded: "Succeeded",
  failed: "Failed",
  queued: "Queued",
  preparing: "Preparing",
  verifying: "Verifying",
  refused: "Refused",
  cancelled: "Cancelled",
  unknown: "Unknown",
});

// The recorded verification verdict, in the custom resource's spelling. Three
// of the six are the resource's own; the other three are states the resource
// has no word for, and an absent verdict is how it spells "nothing recorded".
const VERIFICATION_OF = Object.freeze({
  valid: "Valid",
  invalid: "Invalid",
  notAttempted: "NotAttempted",
});

/** The recorded result a console DETAIL folds in, with the API's combined
 *  trust word applied where the signature column alone would say too much or
 *  nothing.
 *
 *  `OperationVerification.state` is the SIGNATURE result and has no word for
 *  the controller's `result: Untrusted` (it arrives as `unknown`);
 *  `OperationTrust.state` does, so an `untrusted` word restores the
 *  resource's own `Untrusted`. A `Valid` signature the API judged `untrusted`
 *  on a basis this page would read as green (a `None` basis, which the DTO
 *  spells like an absent block) folds as `Untrusted` too, so the detail and
 *  the operation view agree. A basis the page already refuses keeps `Valid`
 *  beside it, and with it the case the badge names. */
function foldedResult(state, trust) {
  const recorded = VERIFICATION_OF[state];
  if (trust === null || trust === undefined || trust.state !== "untrusted") {
    return recorded;
  }
  if (recorded === undefined) {
    return "Untrusted";
  }
  if (recorded === "Valid" && basisAllowsGreen(trust.basis)) {
    return "Untrusted";
  }
  return recorded;
}

function operationStatus(summary) {
  const status = { phase: PHASE_OF[summary.state] };
  if (summary.stateReason !== null) {
    status.reason = summary.stateReason;
  }
  // THE LIST'S OWN VERDICT, CARRIED AND NOT DROPPED. A list item's
  // `OperationSummary` publishes `verificationState` and `verifiedSuccess`
  // (the controller's green-badge rule, computed by the API) and no key id,
  // instant or exit code. They ride under `__summary` (a projection field,
  // like `__contract`), and the badge rules read it only when the object
  // carries no recorded verification of its own.
  status.__summary = {
    verificationState: summary.verificationState,
    verifiedSuccess: summary.verifiedSuccess === true,
    terminal: summary.terminal === true,
  };
  // THE RESULT, WHERE THE API PUBLISHES IT ON THE LIST: the EXIT and
  // RESULT columns read `status.exitCode` and `status.outcome` exactly as they
  // read the custom resource's, and a Restore's outcome keeps its "unverified
  // scorecard claim" label unless the summary is green.
  if (typeof summary.exitCode === "number") {
    status.exitCode = summary.exitCode;
  }
  if (typeof summary.outcome === "string" && summary.outcome.length > 0) {
    status.outcome = summary.outcome;
  }
  return status;
}

/** The fields a console projection of `plural` cannot supply, named. Recorded
 *  on every projected object under `__contract.absent` so a page, a test or a
 *  reader can see what is missing instead of reading an empty cell and
 *  guessing why. */
const ABSENT_IN_CONSOLE = Object.freeze({
  // CONNECTION CONTRACT v1's DATA-KEY REFERENCE IS NOT IN THE PRODUCT API.
  // `ConnectionAuthView` carries the mode, the username, the credential and
  // client-certificate Secret NAMES, `tls` and the CA reference, so a
  // console-mode projection cannot say which data key of a hand-made Secret
  // the controller projects. It is NAMED here rather than left as an empty
  // cell, and `requestBody` refuses a create that carries one.
  kafkaclusters: Object.freeze([
    "status.conditions",
    "spec.auth.secretRef.passwordKey",
  ]),
  // THE CONTROLLER RECORDS THESE AND THE PRODUCT API DOES NOT PUBLISH THEM
  // (`ScheduleStatusView` carries the policy, the next and active runs, the
  // last slot and the missed slots).
  backupschedules: Object.freeze([
    "status.pendingRun",
    "status.history",
  ]),
  // A RUN'S COVERAGE IS NOT PUBLISHED: the `Backup` view carries no
  // `status.selection`, so the coverage label `weirkeeper` writes on every
  // dynamic run (whether "all topics" was attested or only visible) is not
  // reachable in console mode, and the page will not infer one.
  backups: Object.freeze([
    "status.manifestSha256",
    "status.jobRef",
    "status.selection",
    "status.conditions",
    // A LIST ITEM carries the verdict as a summary (`__summary`), not the
    // recorded verification block or the exit: the detail view reads both from
    // the operation route, and `mergeOperation` takes these two off the list
    // when it supplies them.
    "status.exitCode",
    "status.evidence",
  ]),
  // A RESTORE'S SCORECARD FACTS THE API DOES NOT PUBLISH. `integrity.level`
  // comes with the operation route a DETAIL reads (`completion.integrityLevel`;
  // `mergeOperation` takes it off this list when it supplies it); the result,
  // partial reason, target topic preflight and old topics are on the Restore
  // view. The objectives and the measured values are published by NO route:
  // the page says so once and points at the signed scorecard, which carries
  // them.
  restores: Object.freeze([
    "status.integrity.level",
    "status.objectives",
    "status.measured",
    "status.jobRef",
    "status.outcome",
    "status.evidence",
  ]),
  approvals: Object.freeze([]),
});

function note(object, plural, unknown) {
  object.__contract = {
    mode: CONSOLE,
    // A FIELD THE PROJECTION DID SUPPLY IS NOT ABSENT: an object that carries
    // one of the table's paths anyway (a list row whose summary published the
    // exit code or outcome) is not said to lack it.
    absent: (ABSENT_IN_CONSOLE[plural] || []).filter((path) => valueAt(object, path) === undefined),
    unknown: unknown,
  };
  return object;
}

// The value at a dotted path, or `undefined`. A path with a list step (`[]`)
// is never resolved here, so such a field stays named.
function valueAt(object, path) {
  let at = object;
  for (const step of String(path).split(".")) {
    if (step.indexOf("[") !== -1 || at === null || typeof at !== "object") {
      return undefined;
    }
    at = at[step];
  }
  return at;
}

function projectConnection(item) {
  const object = meta(item, "KafkaCluster");
  const auth = { mode: item.auth.mode, tls: item.auth.tls };
  if (item.auth.username !== null) {
    auth.username = item.auth.username;
  }
  const credential = nameRef(item.auth.credentialRef);
  if (credential !== null) {
    auth.secretRef = credential;
  }
  // The mTLS Secret's NAME and the CA reference, in the custom resource's own
  // spelling, so both modes render one shape.
  const certificate = nameRef(item.auth.clientCertificateRef);
  if (certificate !== null) {
    auth.clientCertificate = certificate;
  }
  const ca = item.auth.tlsCa;
  if (ca !== null && ca !== undefined && typeof ca.name === "string") {
    const reference = { name: ca.name, key: ca.key };
    auth.tlsCa = ca.kind === "secret"
      ? { secretKeyRef: reference }
      : { configMapKeyRef: reference };
  }
  object.spec = { bootstrapServers: item.bootstrapServers.slice(), auth: auth, role: item.role };
  if (item.markerTopic !== null) {
    object.spec.markerTopic = item.markerTopic;
  }
  const status = {};
  if (item.reachability.state === "reachable") {
    status.reachable = true;
  } else if (item.reachability.state === "unreachable") {
    status.reachable = false;
  }
  for (const field of ["clusterId", "observedAt", "reason"]) {
    if (item.reachability[field] !== null) {
      status[field] = item.reachability[field];
    }
  }
  object.status = status;
  // THE LAST CONNECTIVITY CHECK IS NOT PART OF THE `KafkaCluster`, and it is
  // deliberately not folded into `status`. `status` here is a projection of
  // the CR's own status, which the legacy mode renders from the real object;
  // a field the cluster does not have would make the two modes disagree about
  // what a `KafkaCluster` is. It rides beside it under the product API's own
  // name, and the page reads it only when the console served it.
  if (item.lastTest !== null && item.lastTest !== undefined) {
    object.lastTest = item.lastTest;
  }
  return object;
}

function projectSchedule(item) {
  const object = meta(item, "BackupSchedule");
  object.spec = {
    schedule: item.schedule,
    sourceRef: nameRef(item.sourceRef),
    topics: item.topics.slice(),
    archive: archive(item.archive),
    suspend: item.suspended,
    concurrencyPolicy: item.concurrencyPolicy,
  };
  if (item.retention !== null) {
    const retention = {};
    if (item.retention.keepLast !== null) {
      retention.keepLast = item.retention.keepLast;
    }
    if (item.retention.keepDays !== null) {
      retention.keepDays = item.retention.keepDays;
    }
    object.spec.retention = retention;
  }
  // `spec.destinationRef` is the saved destination this schedule writes to,
  // and `spec.allUserTopics` is its dynamic-selection policy, under the names
  // the CRD uses. Both are absent on older schedules, and absent keeps meaning
  // what it meant: an inline archive, and a named allowlist.
  if (item.destinationRef !== null) {
    object.spec.destinationRef = nameRef(item.destinationRef);
  }
  if (item.allUserTopics !== null) {
    const dynamic = { incompleteDiscovery: item.allUserTopics.incompleteDiscovery };
    if (item.allUserTopics.exclude !== null) {
      const exclude = {};
      if (item.allUserTopics.exclude.topics !== null) {
        exclude.topics = item.allUserTopics.exclude.topics.slice();
      }
      if (item.allUserTopics.exclude.prefixes !== null) {
        exclude.prefixes = item.allUserTopics.exclude.prefixes.slice();
      }
      dynamic.exclude = exclude;
    }
    object.spec.allUserTopics = dynamic;
  }
  // THE EDITABLE FUTURE POLICY, UNDER THE CRD's OWN NAMES. Every one of these
  // is optional on the wire and absent on older schedules, and absent keeps
  // meaning what it meant: UTC evaluation, a one-hour starting deadline, no
  // catch-up, no retry and a 3600 s run deadline. The projection invents none
  // of them: the form renders "the default, N" for an absent field and sends
  // nothing for it.
  for (const field of ["timeZone", "startingDeadlineSeconds", "catchUpPolicy",
    "activeDeadlineSeconds"]) {
    if (item[field] !== null) {
      object.spec[field] = item[field];
    }
  }
  if (item.retry !== null) {
    const retry = { maxRetries: item.retry.maxRetries };
    if (item.retry.delaySeconds !== null) {
      retry.delaySeconds = item.retry.delaySeconds;
    }
    object.spec.retry = retry;
  }
  // THE REVISION. `metadata.generation` is Kubernetes' own field and the
  // number a manual run records, so it is projected where it belongs rather
  // than into the spec. The product API always emits it and declares it
  // OPTIONAL -- see `ui/contract.js`'s note on the cross-side tightening -- so
  // an absent one is "revision not recorded" and never revision 0.
  if (item.generation !== null) {
    object.metadata.generation = item.generation;
  }
  // A PRESET IS NOT A FIELD OF THE OBJECT. `spec.schedule` is the single
  // source of truth; this is the catalogue entry that expression IS, computed
  // by the server so the browser never has to match cron. It therefore rides
  // OUTSIDE `spec`, beside `__contract`, where nothing can mistake it for
  // something the CRD stores or something a form may send.
  if (item.preset !== null) {
    object.__preset = item.preset;
  }
  const view = item.status;
  const status = {};
  for (const field of ["lastFireTime", "nextFireTime", "lastMissedSlot"]) {
    if (view[field] !== null) {
      status[field] = view[field];
    }
  }
  if (view.observedGeneration !== null) {
    status.observedGeneration = view.observedGeneration;
  }
  if (view.policy !== null) {
    status.policy = {
      generation: view.policy.generation,
      runPolicySha256: view.policy.runPolicySha256,
      timeZone: view.policy.timeZone,
      tzdb: view.policy.tzdb,
      effectiveSince: view.policy.effectiveSince,
      evaluatedAt: view.policy.evaluatedAt,
    };
  }
  // AN ABSENT LIST IS NOT AN EMPTY ONE, AND THE TWO ARE PROJECTED APART.
  // `nextRuns` absent means the controller has computed none (a suspended or
  // invalid schedule); `activeRuns` absent means NOT YET COMPUTED and never
  // "none are running", which is why neither is defaulted to `[]` here.
  if (view.nextRuns !== null) {
    status.nextRuns = view.nextRuns.map((run) => {
      const out = { at: run.at, localTime: run.localTime };
      if (run.adjustment !== null) {
        out.adjustment = run.adjustment;
      }
      return out;
    });
  }
  if (view.activeRuns !== null) {
    status.activeRuns = view.activeRuns.map(activeRun);
  }
  // WHAT THE LAST SLOT DID AND HOW MANY WERE SKIPPED, in the custom
  // resource's own shape, so `renderLastSlot` reads them in both modes. The
  // decoder hands an absent optional as `null`; a nested absent one is
  // dropped rather than rendered as a value.
  if (view.lastSlot !== null && view.lastSlot !== undefined) {
    const slot = view.lastSlot;
    status.lastSlot = {
      slot: slot.slot, dueAt: slot.dueAt, attempt: slot.attempt,
      disposition: slot.disposition, reason: slot.reason, decidedAt: slot.decidedAt,
    };
    if (slot.backupRef !== null && slot.backupRef !== undefined) {
      status.lastSlot.backupRef = { name: slot.backupRef.name };
    }
  }
  if (view.missedSlots !== null && view.missedSlots !== undefined) {
    const missed = view.missedSlots;
    status.missedSlots = { count: missed.count, countCapped: missed.countCapped === true };
    if (typeof missed.lastEvaluatedSlot === "string") {
      status.missedSlots.lastEvaluatedSlot = missed.lastEvaluatedSlot;
    }
    if (Array.isArray(missed.recent)) {
      status.missedSlots.recent = missed.recent.map((r) => ({
        slot: r.slot, reason: r.reason, recordedAt: r.recordedAt,
      }));
    }
  }
  if (view.activeBackup !== null) {
    status.activeBackupRef = { name: view.activeBackup };
  }
  if (view.pendingBackup !== null) {
    status.pendingBackupRef = { name: view.pendingBackup };
  }
  if (view.ready !== null) {
    status.conditions = [condition(view.ready)];
  }
  object.status = status;
  return object;
}

function activeRun(run) {
  return { name: run.name, kind: run.kind, attempt: run.attempt };
}

function condition(view) {
  const out = { type: view.type, status: view.status };
  for (const field of ["reason", "message", "lastTransitionTime"]) {
    if (view[field] !== null) {
      out[field] = view[field];
    }
  }
  return out;
}

function projectBackup(item) {
  const object = meta(item, "Backup");
  object.spec = {
    sourceRef: nameRef(item.sourceRef),
    topics: item.topics.slice(),
    archive: archive(item.archive),
    triggeredBy: item.triggeredBy,
    deadlineSeconds: item.deadlineSeconds,
  };
  if (item.destinationRef !== null) {
    object.spec.destinationRef = { name: item.destinationRef.name };
    if (item.destinationRef.uid !== null) {
      object.spec.destinationRef.uid = item.destinationRef.uid;
    }
  }
  // `schedule` IS THE OLD, COARSER FACT AND `scheduleRef` IS THE NEW ONE, and
  // the projection keeps them in one field the way the CRD does: the name
  // alone for an older run, and the name with the uid, generation and
  // run-policy digest for a newer one. A run that carries neither has no
  // schedule, which is what a manual ad-hoc run is.
  if (item.scheduleRef !== null) {
    const ref = { name: item.scheduleRef.name };
    for (const field of ["uid", "generation", "runPolicySha256"]) {
      if (item.scheduleRef[field] !== null) {
        ref[field] = item.scheduleRef[field];
      }
    }
    object.spec.scheduleRef = ref;
  } else if (item.schedule !== null) {
    object.spec.scheduleRef = { name: item.schedule };
  }
  // WHICH KIND OF RUN THIS IS. ABSENT on every run
  // frozen before `trigger` existed, and the page renders that absence as
  // "trigger not recorded" rather than reading `triggeredBy` as a kind: the
  // older field says `manual` or `schedule`, which cannot tell a catch-up or a
  // retry from an ordinary slot.
  if (item.trigger !== null) {
    const trigger = { kind: item.trigger.kind, attempt: item.trigger.attempt };
    if (item.trigger.retryOf !== null) {
      trigger.retryOf = { name: item.trigger.retryOf.name };
    }
    if (item.trigger.timeZone !== null) {
      trigger.timeZone = item.trigger.timeZone;
    }
    object.spec.trigger = trigger;
  }
  if (item.slot !== null) {
    object.spec.slot = item.slot;
  }
  const status = operationStatus(item.operation);
  for (const field of ["backupId", "manifestKey", "records"]) {
    if (item[field] !== null) {
      status[field] = item[field];
    }
  }
  if (item.windowCovered !== null) {
    status.windowCovered = { fromMs: item.windowCovered.fromMs, toMs: item.windowCovered.toMs };
  }
  if (item.locationDigest !== null) {
    status.locationDigest = item.locationDigest;
  }
  queueOf(item, status);
  if (item.observedAuth !== null) {
    const auth = {};
    if (item.observedAuth.mode !== null) {
      auth.mode = item.observedAuth.mode;
    }
    if (item.observedAuth.username !== null) {
      auth.username = item.observedAuth.username;
    }
    status.auth = auth;
  }
  object.status = status;
  return object;
}

function projectRestore(item) {
  const object = meta(item, "Restore");
  object.spec = {
    approvalRef: nameRef(item.approvalRef),
    sourceArchive: archive(item.sourceArchive),
    backupSetRef: item.backupSetRef,
    pointInTime: item.pointInTime,
    target: {
      clusterRef: nameRef(item.target.clusterRef),
      mode: item.target.mode,
      topicNaming: { prefix: item.target.topicPrefix },
    },
    deadlineSeconds: item.deadlineSeconds,
  };
  // WHETHER THE RESTORE WRITES UNDER THE ORIGINAL TOPIC NAMES, where the
  // custom resource keeps it (`spec.target.topicNaming.originalName`), so the
  // approval page and the wizard's "start from this Restore" read one field in
  // both modes. Written only when it is so, as the CRD stores it.
  //
  // AND THE APPROVAL SUBJECT THE RESTORE NEEDS, under the product API's own
  // name beside the object (as a connection's `lastTest` rides): the custom
  // resource has no such field, and `approvals.js` reads this one first.
  if (item.target.originalName === true) {
    object.spec.target.topicNaming.originalName = true;
  }
  object.approvalSubject = item.approvalSubject;
  const sourceDestination = nameRef(item.sourceDestinationRef);
  if (sourceDestination !== null) {
    object.spec.sourceDestinationRef = sourceDestination;
  }
  const evidenceDestination = nameRef(item.evidenceDestinationRef);
  if (evidenceDestination !== null) {
    object.spec.evidenceDestinationRef = evidenceDestination;
  }
  // THE PLAN BYTES ARE CARRIED VERBATIM OR NOT AT ALL. A list omits them --
  // that is the contract, and `planBytesLength` is what a list may say about
  // them. Inventing a placeholder would put a string under a field whose
  // sha256 is what an approval binds.
  if (item.planBytes !== null) {
    object.spec.planBytes = item.planBytes;
  }
  const status = operationStatus(item.operation);
  status.planHash = item.planHash;
  status.planBytesLength = item.planBytesLength;
  if (item.newTopics.length > 0) {
    status.newTopics = item.newTopics.slice();
  }
  queueOf(item, status);
  // THE SIGNED TIME BASIS, under the custom resource's own
  // name (`status.timeBasis`), so the Restore detail reads it the same way in
  // both modes. Absent stays absent: not recorded, never "none".
  if (item.timeBasis !== null && item.timeBasis !== undefined) {
    status.timeBasis = {
      producerTime: item.timeBasis.producerTime.slice(),
      notRecorded: item.timeBasis.notRecorded.slice(),
    };
    if (typeof item.timeBasis.plan === "string") {
      status.timeBasis.plan = item.timeBasis.plan;
    }
  }
  // A STOPPED CREATION STEP'S THREE LISTS, under the custom
  // resource's own names (`status.targetTopicsAppeared`): the names someone
  // else created, the topics this restore created and LEFT, and the names it
  // asked for and cannot account for, each with how many there are in all.
  // Nothing is deleted, so no list says "removed".
  const stopped = item.targetTopicsAppeared;
  if (stopped !== null && stopped !== undefined) {
    status.targetTopicsAppeared = {
      appeared: stopped.appeared.slice(),
      left: stopped.left.slice(),
      unconfirmed: stopped.unconfirmed.slice(),
      appearedCount: stopped.appearedCount,
      leftCount: stopped.leftCount,
      unconfirmedCount: stopped.unconfirmedCount,
    };
    if (typeof stopped.unconfirmedSeen === "boolean") {
      status.targetTopicsAppeared.unconfirmedSeen = stopped.unconfirmedSeen;
    }
  }
  // THE COVERAGE, under the custom resource's own names, so one
  // badge rule and one list line read it in both modes: what the Restore asks
  // for on `spec` (absent when sampled, as the CRD stores it), what its signed
  // scorecard recorded on `status.integrity`. A list row carries `covered` and
  // its reason; the full block arrives with the detail's operation route.
  const coverage = item.coverage || null;
  if (coverage !== null) {
    if (coverage.requested === "complete") {
      object.spec.coverage = "complete";
    }
    if (typeof coverage.completeMaxRecords === "number") {
      object.spec.completeMaxRecords = coverage.completeMaxRecords;
    }
    if (typeof coverage.recorded === "string") {
      status.integrity = { coverage: coverage.recorded };
      if (typeof coverage.covered === "boolean") {
        status.integrity.complete = { covered: coverage.covered };
        if (typeof coverage.incompleteReason === "string") {
          status.integrity.complete.incompleteReason = coverage.incompleteReason;
        }
      }
    }
  }
  // THE SIGNED SELECTION, under the custom resource's own name
  // (`status.integrity.selection`), so one list line and one detail row read
  // it in both modes. Absent stays absent (every partition, from the floor);
  // an empty block is still a selection.
  if (item.selection !== null && item.selection !== undefined) {
    status.integrity = status.integrity || {};
    status.integrity.selection = copySelection(item.selection);
  }
  // THE SCORECARD'S RESULT, PHASE 0'S TARGET PREFLIGHT AND THE OLD TOPICS,
  // under the custom resource's own names. Absent stays absent: not recorded.
  for (const [from, to] of [["integrityResult", "result"],
    ["integrityPartialReason", "partialReason"]]) {
    if (typeof item[from] === "string") {
      status.integrity = status.integrity || {};
      status.integrity[to] = item[from];
    }
  }
  if (item.topicPreflight !== null && item.topicPreflight !== undefined) {
    status.topicPreflight = {};
    for (const field of ["timestampType", "retentionMs", "timestampBound"]) {
      if (item.topicPreflight[field] !== null) {
        status.topicPreflight[field] = item.topicPreflight[field];
      }
    }
  }
  if (Array.isArray(item.oldTopics) && item.oldTopics.length > 0) {
    status.oldTopics = item.oldTopics.slice();
  }
  object.status = status;
  return object;
}

/** A decoded `RestoreSelectionView`, with the decoder's `null`s
 *  (absent optional fields) left out, as the custom resource stores it. */
function copySelection(selection) {
  // `scope` is always "partial" on the API: presence IS the marker, under
  // the custom resource's name for it.
  const out = { scope: "partial" };
  for (const field of ["windowStartMs", "windowEndMs", "narrowedTopics", "engineRuns"]) {
    if (typeof selection[field] === "number") {
      out[field] = selection[field];
    }
  }
  if (Array.isArray(selection.partitions)) {
    out.partitions = selection.partitions.map((row) => ({
      topic: row.topic,
      partitions: row.partitions.slice(),
    }));
  }
  return out;
}

// THE CEILING A QUEUED MANUAL RUN WAITS BEHIND, under the custom
// resource's own name (`status.queue.limit`), so one `runPhaseBadge` reads it
// in both modes. The API publishes the block only while the run is queued.
function queueOf(item, status) {
  if (item.queue !== null && item.queue !== undefined) {
    status.queue = { limit: item.queue.limit };
    if (item.queue.authorizationExpiresAt !== null && item.queue.authorizationExpiresAt !== undefined) {
      status.queue.authorizationExpiresAt = item.queue.authorizationExpiresAt;
    }
  }
}

function projectApproval(item) {
  const object = meta(item, "Approval");
  object.spec = {
    subjectRef: { kind: item.subjectRef.kind, name: item.subjectRef.name },
    planHash: item.planHash,
  };
  const status = {};
  for (const field of ["verified", "matchedKeyId", "approver", "ticket", "selfAttestedRisk"]) {
    if (item[field] !== null) {
      status[field] = item[field];
    }
  }
  status.conditions = item.conditions.map(condition);
  // The CRD's own spelling, `status.verifiedSubjectRef`, which `approvals.js`'s
  // `approvalState` reads in both modes.
  if (item.verifiedSubject !== null) {
    status.verifiedSubjectRef = {
      kind: item.verifiedSubject.kind,
      name: item.verifiedSubject.name,
      namespace: item.verifiedSubject.namespace,
      uid: item.verifiedSubject.uid,
    };
  }
  // The Approval CRD's own `status.authorization` spelling, so a
  // page reads one shape in both modes.
  if (item.authorization !== null) {
    status.authorization = {
      mode: item.authorization.mode,
      policyName: item.authorization.policyName,
      policyDigest: item.authorization.policyDigest,
      requester: item.authorization.requester,
      confirmationKeyId: item.authorization.confirmationKeyId,
    };
  }
  object.status = status;
  object.__lengths = {
    approvalBytes: item.approvalBytesLength,
    sidecarBytes: item.sidecarBytesLength,
  };
  // THE APPROVAL SUBJECT THE SIGNED DOCUMENT CARRIES, as the product API read
  // it (`originalName`, `ordinary`, or `unknown` for a document it could not
  // read), beside the object under the API's own name: a list row has no
  // bytes to read a subject from.
  object.approvalSubject = item.approvalSubject;
  return object;
}

const PROJECT = Object.freeze({
  kafkaclusters: projectConnection,
  backupschedules: projectSchedule,
  backups: projectBackup,
  restores: projectRestore,
  approvals: projectApproval,
});

/** Merges one operation's normalized status into a projected object: the
 *  evidence keys, the recorded verification, the exit and the conditions. The
 *  product API keeps these on their own route, so a DETAIL view reads them and
 *  a list does not.
 *
 *  THE TRUST BASIS TRAVELS WITH THE VERDICT. `trust` (the decoded
 *  `OperationTrust`, or `null`) has its `basis` written where the custom
 *  resource keeps it, `status.evidence.verification.trust.basis`, so the ONE
 *  badge rule (`validVerification`, `verificationCase`) judges a console
 *  detail exactly as a custom resource: `RecordedBeforeRevocation` is not
 *  green, `Historical` is green with the retired-key qualifier, and an absent
 *  block keeps the pre-existing rule. The basis only refines a recorded
 *  `result`, so it is not written where none was recorded.
 *
 *  `operation` IS THE WHOLE PUBLISHED VIEW: `trust`, `verificationScope` and
 *  `completion` arrive on the one decoded item, each `null` when the body
 *  carried none. */
function mergeOperation(object, operation) {
  const status = object.status;
  const trust = operation.trust;
  const scope = operation.verificationScope;
  // HOW MUCH OF A RESTORE WAS COMPARED TRAVELS WITH THE DETAIL, under the
  // name the History detail reads first (`history.js` `scopeOf`). A Backup
  // has no scope, and a Restore whose route published none keeps the
  // absent-scope sentence.
  if (operation.kind === "restore" && scope !== null && scope !== undefined) {
    const copied = { level: scope.level };
    // The signed coverage, a complete verification's exact counts and the
    // topics a sampled check left unsampled travel with the scope they qualify.
    for (const field of ["recordsSampled", "recordsSampledMatching", "recordsExpected",
      "coverage", "complete", "unsampledTopics"]) {
      if (scope[field] !== null && scope[field] !== undefined) {
        copied[field] = scope[field];
      }
    }
    // The selection every count above is over.
    if (scope.selection !== null && scope.selection !== undefined) {
      copied.selection = copySelection(scope.selection);
    }
    status.verificationScope = copied;
  }
  // THE INTEGRITY LEVEL TRAVELS WITH THE DETAIL TOO.
  // `completion.integrityLevel` is the signed scorecard's own word
  // (`byte-fingerprint`, `consume-only` or `not-attempted`, verbatim), written
  // where the custom resource keeps it, `status.integrity.level`. It is
  // copied and never derived: with no completion block the level stays
  // absent, and the page says the API published none. The result and the
  // partial reason come with the Restore view (`projectRestore`).
  const completion = operation.completion;
  if (operation.kind === "restore" && completion !== null && completion !== undefined &&
    typeof completion.integrityLevel === "string" && completion.integrityLevel.length > 0) {
    status.integrity = status.integrity || {};
    status.integrity.level = completion.integrityLevel;
  }
  status.phase = PHASE_OF[operation.state];
  if (operation.stateReason !== null) {
    status.reason = operation.stateReason;
  }
  const result = operation.result;
  for (const field of ["exitCode", "exitReason", "outcome", "lastPhaseCompleted"]) {
    if (result[field] !== null) {
      status[field] = result[field];
    }
  }
  const evidence = {};
  const e = operation.evidence;
  if (e.payloadKey !== null) {
    evidence[operation.kind === "backup" ? "receiptKey" : "scorecardKey"] = e.payloadKey;
  }
  if (e.payloadSha256 !== null) {
    evidence[operation.kind === "backup" ? "receiptSha256" : "scorecardSha256"] = e.payloadSha256;
  }
  for (const pair of [["sidecarKey", "sidecarKey"], ["offsetReportKey", "offsetReportKey"],
    ["offsetReportSha256", "offsetReportSha256"]]) {
    if (e[pair[0]] !== null) {
      evidence[pair[1]] = e[pair[0]];
    }
  }
  const verification = {};
  const recorded = foldedResult(operation.verification.state, trust);
  if (recorded !== undefined) {
    verification.result = recorded;
  }
  for (const field of ["payloadType", "matchedKeyId", "verifiedAt", "detail"]) {
    if (operation.verification[field] !== null) {
      verification[field] = operation.verification[field];
    }
  }
  // AN ABSENT BLOCK STAYS ABSENT. The DTO spells a
  // missing `trust` block as `basis: None`; written here as a block, the
  // custom-resource rule (`trustBlockAllowsGreen`) would read it as a PRESENT
  // `None` and turn every object an older controller wrote red. A present
  // `None` block the API judged `untrusted` has already folded to `Untrusted`
  // above, so nothing that needs the block loses it.
  if (verification.result !== undefined && trust !== null && trust !== undefined &&
    trust.basis !== TRUST_BASIS_NOT_OBSERVED) {
    verification.trust = { basis: trust.basis };
    if (trust.keyState !== null) {
      verification.trust.keyState = trust.keyState;
    }
  }
  if (Object.keys(verification).length > 0) {
    evidence.verification = verification;
  }
  if (Object.keys(evidence).length > 0) {
    status.evidence = evidence;
  }
  if (operation.conditions.length > 0) {
    status.conditions = operation.conditions.map(condition);
  }
  // WHAT THE OPERATION ROUTE SUPPLIED IS NO LONGER ABSENT.
  const contract = object.__contract;
  if (contract && Array.isArray(contract.absent)) {
    const now = { "status.exitCode": status.exitCode, "status.outcome": status.outcome,
      "status.evidence": status.evidence,
      "status.integrity.level": (status.integrity || {}).level };
    contract.absent = contract.absent.filter((field) =>
      !(field in now) || now[field] === undefined);
  }
  return object;
}

// THE MANUAL-RUN NAME. "Back up now" is the only create whose object has NO
// NAME UNTIL THE SCOPE IS HASHED, so the name IS the idempotency record:
// `logweir-manual-` plus the first 26 characters of the lowercase, unpadded
// RFC 4648 base32 of `sha256` over the length-prefixed tuple `(issuer,
// subject, namespace, route, key)`. A double click, a lost response and a
// retry target the SAME name, and `AlreadyExists` makes a second object
// impossible.
//
// IT MATCHES `crates/logweir-api/src/idempotency.rs` (`identity`,
// `base32_lower`, `NAME_HASH_CHARS`) BYTE FOR BYTE:
//
//   * the scope opens with the format line below, INSIDE the hashed bytes,
//     so a format change is a name change;
//   * each field is prefixed with its BYTE length as a 64-bit big-endian
//     integer, so no pair of fields can be re-cut into a different pair;
//   * the digest is rendered in the base32 alphabet below (`a`-`z`, `2`-`7`,
//     DNS-label safe), and exactly 26 characters are kept: 130 bits and a
//     41-character DNS-1123 name.
//
// `ui/tests/fixtures/manual-backup-names.json` pins both sides:
// `ui/tests/d1.spec.js` drives this function over every row, and
// `scripts/live/d1/run.py`'s `L-06-2-cli` drives `logweir-api` over the
// same rows.

/** The route identifier inside a manual run's idempotency scope: the PRODUCT
 *  API's route. */
export const MANUAL_BACKUP_ROUTE = "POST /api/v1/namespaces/{ns}/backups";

/** `crates/logweir-api/src/routes/backups.rs`'s `NAME_PREFIX`. */
export const MANUAL_BACKUP_PREFIX = "logweir-manual-";

/** The first line of the hashed scope document, newline included. */
export const MANUAL_SCOPE_FORMAT = "logweir-api/idempotency-scope/v1\n";

/** How many base32 characters of the scope digest the name carries. */
export const MANUAL_NAME_HASH_CHARS = 26;

/** RFC 4648 base32, lowercase, unpadded -- `idempotency.rs::base32_lower`. */
function base32Lower(bytes) {
  const alphabet = "abcdefghijklmnopqrstuvwxyz234567";
  let out = "";
  let buffer = 0;
  let bits = 0;
  for (const byte of bytes) {
    // `* 256` rather than `<< 8`: a JavaScript bitwise operator coerces to a
    // SIGNED 32-bit integer, and a 13-bit accumulator shifted left by 8 would
    // be fine today and wrong the moment anyone widened it. Arithmetic keeps
    // the accumulator an exact integer.
    buffer = buffer * 256 + byte;
    bits += 8;
    while (bits >= 5) {
      bits -= 5;
      out += alphabet.charAt(Math.floor(buffer / Math.pow(2, bits)) % 32);
    }
    buffer = buffer % Math.pow(2, bits);
  }
  if (bits > 0) {
    out += alphabet.charAt((buffer * Math.pow(2, 5 - bits)) % 32);
  }
  return out;
}

/** One length-prefixed field: eight bytes of big-endian length, then the
 *  UTF-8 bytes. `idempotency.rs::push_field`. */
function pushField(out, value) {
  const bytes = new TextEncoder().encode(String(value));
  const length = bytes.length;
  for (let i = 7; i >= 0; i -= 1) {
    // Byte i of a 64-bit big-endian length. A JavaScript string is bounded far
    // below 2^32 bytes, so the top four bytes are always zero; they are
    // written anyway because the Rust side writes eight and the digest is over
    // the bytes, not over the intention.
    out.push(Math.floor(length / Math.pow(2, 8 * i)) % 256);
  }
  for (const byte of bytes) {
    out.push(byte);
  }
}

/** The deterministic name of the manual run at `scope`
 *  `{issuer, subject, namespace, route, key}`.
 *
 *  `route` defaults to [`MANUAL_BACKUP_ROUTE`] because there is exactly one
 *  route this rule is used for; it is a parameter so the fixture can pin the
 *  field's PLACE in the scope rather than only its value. */
export async function manualBackupName(scope) {
  const s = scope || {};
  const document = [];
  for (const byte of new TextEncoder().encode(MANUAL_SCOPE_FORMAT)) {
    document.push(byte);
  }
  pushField(document, s.issuer === undefined ? "" : s.issuer);
  pushField(document, s.subject === undefined ? "" : s.subject);
  pushField(document, s.namespace === undefined ? "" : s.namespace);
  pushField(document, s.route === undefined ? MANUAL_BACKUP_ROUTE : s.route);
  pushField(document, s.key === undefined ? "" : s.key);
  const digest = await sha256Of(new Uint8Array(document));
  return MANUAL_BACKUP_PREFIX + base32Lower(digest).slice(0, MANUAL_NAME_HASH_CHARS);
}

/** SHA-256 over exactly these bytes, as bytes.
 *
 *  `ui/plan.js` already refuses to load outside a secure context, and this
 *  module imports it, so `crypto.subtle` is present wherever this file runs.
 *  The refusal is repeated here anyway rather than assumed: this one decides a
 *  NAME, and a name computed some other way is a second run. */
async function sha256Of(bytes) {
  const subtle = (globalThis.crypto || {}).subtle;
  if (subtle === undefined || subtle === null) {
    const error = new Error(
      "this page will not create a manual backup here: deriving the run's name needs the " +
        "platform's hash, and it is unavailable. A name computed any other way would be a " +
        "different run from the one a second click is meant to find.",
    );
    error.kind = "refused";
    error.status = 0;
    error.reason = "NoHash";
    throw error;
  }
  return new Uint8Array(await subtle.digest("SHA-256", bytes));
}

/** The run deadline a manual run inherits when the schedule names none --
 *  `weirkeeper::controllers::backup_schedule::SCHEDULED_DEADLINE_SECONDS` and
 *  `weirkeeper::cadence::DEFAULT_ACTIVE_DEADLINE_SECONDS`, the same number for
 *  the same reason: the deadline is INSIDE the digested document, so a
 *  schedule that says nothing and one that says 3600 ask for the same run and
 *  must digest the same. */
export const DEFAULT_MANUAL_DEADLINE_SECONDS = 3600;

/** The four labels a manual run carries -- selection hints, never authority.
 *  Membership of a schedule is `spec.scheduleRef.uid`, which is in the sealed
 *  spec. `crates/weirkeeper/src/identity.rs` holds the same four names. */
const MANUAL_LABELS = Object.freeze({
  trigger: "logweir.dev/trigger",
  attempt: "logweir.dev/attempt",
  schedule: "logweir.dev/schedule",
  scheduleUid: "logweir.dev/schedule-uid",
});

/** A structured clone of `value`, or `fallback` when there is nothing to copy.
 *
 *  NOT A REFERENCE: the schedule object this page read is rendered elsewhere on
 *  the same card, and a create body that shared a sub-object with it could be
 *  mutated from either side. NOT A FIELD-BY-FIELD REBUILD either, for the
 *  reason `manualBackupObject`'s `archive` line gives: a rebuild drops fields
 *  nobody has thought of yet, silently. */
function clone(value, fallback) {
  return value === undefined || value === null
    ? JSON.parse(JSON.stringify(fallback))
    : JSON.parse(JSON.stringify(value));
}

function copyRef(reference) {
  const r = reference || null;
  return r === null || typeof r.name !== "string" ? null : { name: r.name };
}

/** The `Backup` a "Back up now" on `schedule` becomes: the canonical object,
 *  with no name and no annotations yet.
 *
 *  THE POLICY HALF IS A STRUCTURAL COPY AND NOTHING CLEVERER, because that is
 *  exactly what `weirkeeper::controllers::backup_schedule::run_policy_spec`
 *  does -- the one builder the scheduler validates, digests and POSTs, and the
 *  one `crates/logweir-api/src/routes/backups.rs::build` calls for it.
 *  Source, topic selection, archive or destination, and the deadline with its
 *  absent default resolved. Nothing is canonicalised here: sorting the topics
 *  or dropping a duplicate would change the object without changing the policy
 *  it digests to, and `Backup.spec.topics` keeps the operator's order verbatim
 *  by design.
 *
 *  THE IDENTITY HALF IS WRITTEN LAST AND IS ALWAYS THE SAME: `triggeredBy:
 *  manual`, `trigger {kind: Manual, attempt: 0}`, no slot. */
export function manualBackupObject(ns, schedule, digest) {
  const meta = (schedule || {}).metadata || {};
  const policy = (schedule || {}).spec || {};
  const name = String(meta.name || "");
  const uid = String(meta.uid || "");
  const labels = {};
  labels[MANUAL_LABELS.trigger] = "manual";
  labels[MANUAL_LABELS.attempt] = "0";
  labels[MANUAL_LABELS.schedule] = name;
  if (uid.length > 0) {
    labels[MANUAL_LABELS.scheduleUid] = uid;
  }
  const spec = {
    sourceRef: copyRef(policy.sourceRef) || { name: "" },
    topics: Array.isArray(policy.topics) ? policy.topics.slice() : [],
    // A STRUCTURAL CLONE OF THE WHOLE `archive`, NOT A REBUILT `{url,
    // secretRef}`: `run_policy_spec` clones the whole `ArchiveRef`, and a field
    // added to it later and dropped here would surface only as a terminal
    // `RunPolicyDigestMismatch` (the controller digests the fields the object
    // carries).
    archive: clone(policy.archive, { url: "" }),
    triggeredBy: "manual",
    trigger: { kind: "Manual", attempt: 0 },
    deadlineSeconds: typeof policy.activeDeadlineSeconds === "number"
      ? policy.activeDeadlineSeconds
      : DEFAULT_MANUAL_DEADLINE_SECONDS,
    scheduleRef: {
      name: name,
      uid: uid,
      generation: meta.generation,
      runPolicySha256: digest,
    },
  };
  spec.archive.url = String((policy.archive || {}).url || "");
  const destination = copyRef(policy.destinationRef);
  if (destination !== null) {
    spec.destinationRef = destination;
  }
  if (policy.allUserTopics !== undefined && policy.allUserTopics !== null) {
    spec.allUserTopics = clone(policy.allUserTopics, {});
  }
  return {
    apiVersion: "logweir.dev/v1alpha1",
    kind: "Backup",
    metadata: { name: "", namespace: String(ns), labels: labels },
    spec: spec,
  };
}

// THE CLIENT'S OWN CHECKS RUN over the body about to be sent,
// and produce the product API's own `FieldError[]` shape. A refusal here is
// `invalid` with the same `details.causes` a server 422 carries, so a form
// renders it beside the field it is about without knowing where it came from.
function checked(plural, object, send) {
  const errors = validateRequest(plural, object);
  if (errors.length === 0) {
    return send();
  }
  const error = new Error(
    "this page did not send the request: " + String(errors.length) +
      " field(s) do not satisfy the contract",
  );
  error.kind = "invalid";
  error.status = 422;
  error.reason = "ClientValidation";
  error.details = { causes: errors.map((e) => ({ field: e.field, message: e.message, reason: e.code })) };
  return Promise.reject(error);
}

/** The page size the console client asks for. The product API's documented
 *  maximum; a smaller one only means more round trips for the same rows. */
export const LIST_PAGE_SIZE = 200;

/** How many pages one list may follow before this page refuses to go on.
 *  [`LIST_PAGE_SIZE`] times this is the most rows a console list will read. */
export const LIST_PAGE_BUDGET = 25;

const consoleApi = Object.freeze({
  async list(ns, plural, options) {
    const route = consolePlural(plural);
    requireGrant(ns, READ_CAPABILITY[route]);
    const project = PROJECT[plural];
    // THE CURSOR IS FOLLOWED, AND A LIST IS NEVER SILENTLY SHORTENED: a page
    // that stopped at the first rows would show fewer Backups with nothing
    // saying so, and `#/history` is the inventory read during an incident.
    //
    // AND THE END IS BOUNDED: a namespace larger than the budget raises an error
    // the page RENDERS, naming how many rows were read and what to run instead.
    // A prefix presented as the whole is the failure this arm prevents.
    const items = [];
    const unknown = [];
    let page = null;
    let cursor = null;
    for (let read = 0; read < LIST_PAGE_BUDGET; read += 1) {
      const query = Object.assign({}, options || {}, { limit: LIST_PAGE_SIZE });
      delete query.onPage;
      if (cursor !== null) {
        query.cursor = cursor;
      }
      const decoded = decodeConsoleList(route, await consoleList(ns, route, query));
      for (const item of decoded.value.items) {
        items.push(note(project(item), plural, decoded.unknown));
      }
      for (const path of decoded.unknown) {
        if (unknown.indexOf(path) === -1) {
          unknown.push(path);
        }
      }
      page = decoded.value.page;
      cursor = page.nextCursor;
      if (cursor === null) {
        return {
          apiVersion: "logweir.dev/v1alpha1",
          kind: KIND_OF[plural] + "List",
          metadata: { resourceVersion: page.snapshot || "" },
          items: items,
          __page: { limit: page.limit, nextCursor: null, snapshot: page.snapshot },
          __unknown: unknown,
        };
      }
      // A PAGE CAN BE SHOWN BEFORE THE LAST ONE ARRIVES. A caller that
      // passes `onPage` is handed the rows read so far, marked `partial`, after
      // every page but the last, so a list of hundreds renders its first 200
      // rows while the rest are read. The list it RESOLVES with is still the
      // whole namespace; a partial one is never returned as if it were.
      if (typeof (options || {}).onPage === "function") {
        try {
          options.onPage({
            apiVersion: "logweir.dev/v1alpha1",
            kind: KIND_OF[plural] + "List",
            metadata: { resourceVersion: page.snapshot || "" },
            items: items.slice(),
            __page: { limit: page.limit, nextCursor: cursor, snapshot: page.snapshot, partial: true },
            __unknown: unknown.slice(),
          });
        } catch (painting) {
          // A caller's paint that failed is not a failed read.
        }
      }
    }
    throw tooMany(ns, plural, items.length);
  },
  async get(ns, plural, name, options) {
    const route = consolePlural(plural);
    requireGrant(ns, READ_CAPABILITY[route]);
    const decoded = decodeConsoleItem(route, await consoleGet(ns, route, name, options));
    const object = note(PROJECT[plural](decoded.value.item), plural, decoded.unknown);
    return enrich(ns, plural, name, object, options);
  },
  async create(ns, plural, object) {
    const route = consolePlural(plural);
    // RECORDING AN APPROVAL IS THE RESTORE'S OWN ROUTE. The
    // product API has no top-level Approval create; an approver records the
    // files `logweir drill approve` wrote through `POST .../restores/{name}/
    // approval`, named by the Restore the Approval is about, which stores them
    // byte-for-byte under that Restore's `spec.approvalRef`.
    if (plural === "approvals") {
      const spec = (object || {}).spec || {};
      const subject = ((spec.subjectRef || {}).name) || "";
      return this.submitGovernedApproval(ns, String(subject), String(spec.sidecarBytes || ""),
        String(spec.approvalBytes || ""));
    }
    if (!Object.prototype.hasOwnProperty.call(CREATE_CAPABILITY, route)) {
      throw noRoute(
        "the product API has no create route for " + KIND_OF[plural] + ", so this console " +
          "cannot create one. An administrator with access to the cluster creates it, or the " +
          "logweir CLI does; this page will read it once it exists.",
      );
    }
    requireGrant(ns, CREATE_CAPABILITY[route]);
    return checked(plural, object, async () => {
      const name = ((object || {}).metadata || {}).name;
      let answer;
      try {
        answer = await consoleCreate(ns, route, requestBody(plural, object), {
          idempotencyKey: idempotencyKey(route, ns, name),
          token: decided === null ? null : decided.token,
        });
      } catch (refused) {
        throw withCauses(conflictKind(refused), route);
      }
      const decoded = decodeConsoleItem(route, answer);
      const made = note(PROJECT[plural](decoded.value.item), plural, decoded.unknown);
      // THE API ANSWERED "THIS ALREADY HAPPENED", AND THAT TRAVELS WITH THE
      // OBJECT. `lifecycle.js`'s `createdOutcome` reads it, so a form says
      // "already existed" for a replay and "created" for a create.
      made.__contract.replayed = decoded.value.replayed === true;
      // THE FROZEN POLICY'S ANSWER TRAVELS WITH THE
      // OBJECT, exactly as `replayed` does. Only a Restore create carries one;
      // `ui/pages/restore-wizard.js`'s `frozenDecision` is its one reader.
      made.__contract.authorization = decoded.value.authorization || null;
      return made;
    });
  },
  async patchSuspend(ns, name, value) {
    requireGrant(ns, "scheduleSetSuspension");
    // THE PRECONDITION IS THE VALUE LAST READ, which is what this route takes
    // instead of an idempotency key. A controller status write between the
    // read and this call is a 412, and the contract's answer to a 412 is to
    // read again and resend -- so this reads again, here, rather than making
    // the page's toggle fail for a reason it did not cause.
    const current = decodeConsoleItem("schedules", await consoleGet(ns, "schedules", name));
    try {
      const answer = await consoleSetSuspension(ns, name, {
        suspended: value === true,
        expectedResourceVersion: current.value.item.resourceVersion,
      }, { token: decided === null ? null : decided.token });
      const decoded = decodeConsoleItem("schedules", answer);
      return note(projectSchedule(decoded.value.item), "backupschedules", decoded.unknown);
    } catch (refused) {
      throw withCauses(refused, "schedules");
    }
  },
  async previewCadence(query, options) {
    // A SAFE GET THAT READS NO OBJECT, so there is no namespace to scope it
    // to and no capability flag that covers it. The product API's own rule is
    // "may read schedules in at least one granted namespace", which is a
    // question this page cannot answer for itself -- the grants it holds are
    // the ones the session document published -- so it asks, and renders the
    // 403 if that is the answer.
    const decoded = decodeCadencePreview(await cadencePreview(query, options));
    return decoded.value;
  },
  async editSchedulePolicy(ns, name, body) {
    // THE SAME AUTHORITY AS CREATING ONE: editing a schedule's future policy
    // has the same role row as creating one and `scheduleCreate` covers both;
    // a route test pins the equality, so splitting the flag and splitting the
    // authority have to happen together.
    requireGrant(ns, "scheduleCreate");
    const checked = decodeRequest("schedules:policy", body);
    if (checked.unknown.length > 0) {
      throw contractFailure(
        "UpdateSchedulePolicyRequest",
        checked.unknown[0],
        "this page built a policy this API does not declare; it was not sent",
      );
    }
    try {
      const answer = await consoleSchedulePolicy(ns, name, body, {
        token: decided === null ? null : decided.token,
      });
      const decoded = decodeConsoleItem("schedules", answer);
      return note(projectSchedule(decoded.value.item), "backupschedules", decoded.unknown);
    } catch (refused) {
      throw withCauses(refused, "schedules");
    }
  },
  // The namespace's effective approval policy, or `null` when this
  // session may not read approvals there (the policy route is gated on the
  // same read).
  async approvalPolicy(ns, options) {
    if (!granted(ns, "approvalsRead")) {
      return null;
    }
    return decodeApprovalPolicy(await consoleApprovalPolicy(ns, options)).value.item;
  },
  // A governed approver's countersigned sidecar for one Restore.
  // The product API refuses the requester (403), a non-Governed namespace
  // (409 policy_mismatch) and a stale or expired confirmation; this page
  // renders each refusal as it arrives.
  async submitGovernedApproval(ns, restoreName, sidecarBytes, approvalBytes) {
    requireGrant(ns, "approvalSubmit");
    // `approvalBytes` only for an UNBOUND namespace's v1 files; a governed
    // countersignature sends the sidecar alone and the server refuses a
    // document beside it.
    const body = typeof approvalBytes === "string"
      ? { sidecarBytes: String(sidecarBytes), approvalBytes: approvalBytes }
      : { sidecarBytes: String(sidecarBytes) };
    const checked = decodeRequest("restores:approval", body);
    if (checked.unknown.length > 0) {
      throw contractFailure(
        "SubmitApprovalRequest",
        checked.unknown[0],
        "this page built a submission this API does not declare; it was not sent",
      );
    }
    let answer;
    try {
      answer = await consoleAction(ns, "restores:approval", restoreName, body, {
        token: decided === null ? null : decided.token,
      });
    } catch (refused) {
      throw withCauses(refused, "approvals");
    }
    const decoded = decodeConsoleItem("approvals", answer);
    const made = note(projectApproval(decoded.value.item), "approvals", decoded.unknown);
    made.__contract.replayed = decoded.value.replayed === true;
    return made;
  },
  // One Restore's two-person request, as the approver is shown
  // it -- or `null` when this session may not read approvals there (the route
  // is gated on the same read). Every field the page renders is the server's,
  // taken from bytes the console verified its own signature on.
  async approvalRequest(ns, restoreName, options) {
    if (!granted(ns, "approvalsRead")) {
      return null;
    }
    try {
      return decodeApprovalRequest(
        await consoleApprovalRequest(ns, restoreName, options),
      ).value.item;
    } catch (refused) {
      throw withCauses(refused, "approvals");
    }
  },
  // THE SECOND PERSON'S CLICK. The body is ONE field: the hash of
  // the request's signed bytes, exactly as the view showed it. It names what
  // was reviewed; it supplies nothing an authorization carries. The product
  // API refuses the requester and anyone who is not an Approver (403), a
  // request that changed or expired (409), and a cross-site or tokenless
  // request before any of that; this page renders each refusal as it arrives.
  async approveInConsole(ns, restoreName, confirmationSha256) {
    requireGrant(ns, "approvalSubmit");
    const body = { confirmationSha256: String(confirmationSha256) };
    const checked = decodeRequest("restores:console-approval", body);
    if (checked.unknown.length > 0) {
      throw contractFailure(
        "ConsoleApprovalRequest",
        checked.unknown[0],
        "this page built an approval this API does not declare; it was not sent",
      );
    }
    let answer;
    try {
      answer = await consoleAction(ns, "restores:console-approval", restoreName, body, {
        token: decided === null ? null : decided.token,
      });
    } catch (refused) {
      throw withCauses(refused, "approvals");
    }
    const decoded = decodeConsoleItem("approvals", answer);
    const made = note(projectApproval(decoded.value.item), "approvals", decoded.unknown);
    made.__contract.replayed = decoded.value.replayed === true;
    return made;
  },
  async runBackupNow(ns, body, key) {
    requireGrant(ns, "manualBackupCreate");
    const checked = decodeRequest("backups", body);
    if (checked.unknown.length > 0) {
      throw contractFailure(
        "CreateBackupRequest",
        checked.unknown[0],
        "this page built a manual run this API does not declare; it was not sent",
      );
    }
    let answer;
    try {
      // THE KEY IS THE CALLER'S, NOT THIS MODULE'S, AND THAT IS THE POINT.
      // Every other create in this file composes its key from the NAME being
      // created; a manual run has no name until the server derives one, so
      // the intent has to be minted by whoever holds the draft and kept for
      // as long as that draft lives. `ui/pages/schedules.js` mints one per
      // draft and hands the SAME string to every resend of it.
      answer = await consoleCreate(ns, "backups", body, {
        idempotencyKey: key,
        token: decided === null ? null : decided.token,
      });
    } catch (refused) {
      // A `409 policy_changed` CARRIES A FACT AND NOT ONLY A COMPLAINT: the
      // revision that is in force now. It is attached to the error so the page
      // can offer the new one rather than telling a person to reload and look.
      const changed = refused.problem === undefined || refused.problem === null
        ? null
        : decodePolicyChanged(refused.problem);
      if (changed !== null) {
        refused.policy = changed;
      }
      throw withCauses(refused, "backups");
    }
    const decoded = decodeManualBackup(answer);
    const made = note(projectBackup(decoded.value.item), "backups", decoded.unknown);
    made.__contract.replayed = decoded.value.replayed === true;
    // THE SCHEDULE'S OWN STATE TRAVELS WITH THE RUN, as notices. A suspended
    // schedule and an already-active run are both things a person should SEE
    // after clicking, and neither is a reason the click was refused: nothing
    // about a schedule blocks a manual run.
    return { run: made, schedule: decoded.value.schedule, replayed: made.__contract.replayed };
  },
  async listCluster(plural) {
    throw noRoute(
      "the product API does not serve " + String(plural) + ". The TrustRoster is cluster-scoped " +
        "and admin-only, and a shared console is not where a cluster-scoped read belongs; " +
        "read it with kubectl.",
    );
  },
});

// DESTINATIONS, TOPIC DISCOVERIES AND OPERATION READINESS exist on the
// product API and are not projected into custom resources.
//
// WHO MAY START ONE. The flags `destinations`, `topicDiscovery` and
// `preflight` are the domains' READ FLOORS ("this build serves the domain and
// you may read it"). Whether the actor may also START, CANCEL or MANAGE is
// the role table, which `/session` publishes per grant: `CAPABILITY_FLAGS`
// is frozen and pinned against the schema, so the read is gated by the FLAG
// and the write by [`mayOperate`], which reads the roles.

/** The capability flag that is each domain's READ FLOOR. */
const DOMAIN_CAPABILITY = Object.freeze({
  destinations: "destinations",
  "topic-discoveries": "topicDiscovery",
  preflights: "preflight",
});

/** The two product roles that may start, cancel and manage in a namespace.
 *  Taken from `authz.rs`'s decision table: Viewer reads, Approver reads what a
 *  governed approval needs, and Operator and Administrator are the two that
 *  mutate. */
export const OPERATING_ROLES = Object.freeze(["operator", "administrator"]);

/** Whether this actor may take an operator-level action in `ns`.
 *
 *  A ROLE, NOT A FLAG, AND THE DIFFERENCE MATTERS. `granted(ns, "destinations")`
 *  is `implemented && may read`; it is true for a Viewer, who may not create a
 *  destination, start a discovery or cancel a check. Branching a BUTTON on it
 *  would put a control on screen that the server answers 403 for every time.
 *
 *  AN EMPTY ROLE LIST IS A MODE, NOT A REFUSAL. localAdmin mode has no product
 *  roles: it is one administrator on a loopback listener, and `authz.rs`
 *  answers every action `true` for it. There the server is the gate and the
 *  control is offered. What this refuses
 *  is the case that is actually a refusal: an authenticated actor who HOLDS
 *  roles in this namespace and none of them is Operator or Administrator. */
export function mayOperate(ns) {
  if (decided === null || decided.mode !== CONSOLE) {
    return true;
  }
  const document = decided.session;
  if (document !== null && document.authenticationMode === "localAdmin") {
    return true;
  }
  const held = decided.roles[ns];
  if (held === undefined) {
    return false;
  }
  for (const role of held) {
    if (OPERATING_ROLES.indexOf(role) !== -1) {
      return true;
    }
  }
  return false;
}

/** An error for a flow this page will not attempt because the actor's own
 *  roles do not cover it. `status` is 403 and `reason` is the product API's
 *  own code, so it renders exactly as the server's refusal would. */
function notOperator(ns, what) {
  const held = decided === null ? [] : (decided.roles[ns] || []);
  const error = new Error(
    "this session may not " + what + " in namespace " + String(ns) + ". The roles it holds " +
      "there are " + (held.length === 0 ? "none" : held.join(", ")) + "; " +
      OPERATING_ROLES.join(" or ") + " is required. The roles come from the product API's " +
      "own session document; nothing on this page decides them.",
  );
  error.kind = "rejected";
  error.status = 403;
  error.reason = "namespace_forbidden";
  return error;
}

// THE READ GATE AND THE WRITE GATE, SEPARATELY. Every D2 call below opens with
// one of these two, so "which permission does this need" is one line at the
// top of each method and not a paragraph somewhere else.
function requireDomain(ns, domain) {
  requireGrant(ns, DOMAIN_CAPABILITY[domain]);
}

function requireOperator(ns, domain, what) {
  requireDomain(ns, domain);
  if (!mayOperate(ns)) {
    throw notOperator(ns, what);
  }
}

// THE BODY IS CHECKED AGAINST THE PUBLISHED REQUEST SHAPE BEFORE IT IS SENT.
// `ui/contract.js` declares every D2 request; a field that became required on
// the server, or one this page invented, is a named contract failure here
// rather than a 422 in front of an operator.
function sending(name, body) {
  const decodedBody = decodeRequest(name, body);
  if (decodedBody.unknown.length > 0) {
    throw contractFailure(
      name,
      decodedBody.unknown[0],
      "this page built a field the published request schema does not declare, and a mutation " +
        "input that carries one is a 422 from the product API",
    );
  }
  return body;
}

// Every D2 read returns the DECODED DTO, not a projection into a custom
// resource: the DTO IS the shape, and a projection would be a second
// vocabulary for one set of facts.
const consoleChecks = Object.freeze({
  async destinations(ns, options) {
    requireDomain(ns, "destinations");
    const decoded = decodeConsoleList(
      "destinations",
      await consoleList(ns, "destinations", Object.assign({ limit: LIST_PAGE_SIZE }, options || {})),
    );
    return { items: decoded.value.items, page: decoded.value.page, unknown: decoded.unknown };
  },
  async destination(ns, name, options) {
    requireDomain(ns, "destinations");
    const decoded = decodeConsoleItem("destinations", await consoleGet(ns, "destinations", name, options));
    return { item: decoded.value.item, unknown: decoded.unknown };
  },
  async createDestination(ns, request) {
    requireOperator(ns, "destinations", "create a destination");
    const answer = await consoleCreate(ns, "destinations", sending("destinations", request), {
      idempotencyKey: idempotencyKey("destinations", ns, request.name),
      token: tokenNow(),
    });
    const decoded = decodeConsoleItem("destinations", answer);
    return { item: decoded.value.item, replayed: decoded.value.replayed === true };
  },
  async updateDestinationAccess(ns, name, request) {
    requireOperator(ns, "destinations", "rotate destination access");
    // NO IDEMPOTENCY KEY, AND NOT BECAUSE ONE WAS FORGOTTEN. This route's
    // replay guard is `expectedGeneration`: a second send of the same body
    // after the first landed is a 412, which is the RIGHT answer -- the object
    // moved. The product API answers 400 for a key here, and `api.js`'s action
    // table refuses to send one at all.
    const answer = await consoleAction(
      ns, "destinations:update-access", name,
      sending("destinations:update-access", request),
      { token: tokenNow() },
    );
    const decoded = decodeConsoleItem("destinations", answer);
    return { item: decoded.value.item };
  },
  async testDestination(ns, name, request, options) {
    requireOperator(ns, "destinations", "test destination access");
    const body = sending("destinations:test", request || {});
    // THE ATTEMPT, WHEN THE PAGE MINTS ONE: a deliberate second test (an
    // operator fixed a Secret and presses Test access again) is a new check,
    // not a replay of the first Preflight's verdict.
    const attempt = (options || {}).attempt;
    const subject = name + "." + rolesTag(body.roles) +
      (typeof attempt === "string" && attempt.length > 0 ? "." + digest32(attempt) : "");
    const answer = await consoleAction(ns, "destinations:test", name, body, {
      // THE KEY IS A FUNCTION OF WHAT IS BEING TESTED AND OF WHICH ROLES, so a
      // double click is one Preflight and a test of a different role set is
      // another. A key that ignored the roles would answer the second request
      // with the first one's result.
      idempotencyKey: idempotencyKey("destinations:test", ns, subject),
      token: tokenNow(),
    });
    const decoded = decodeConsoleItem("preflights", answer);
    return { item: decoded.value.item, replayed: decoded.value.replayed === true };
  },
  async destinationUsage(ns, name, options) {
    requireDomain(ns, "destinations");
    const decoded = decodeDestinationUsage(await consoleSub(ns, "destinations", name, "usage", options));
    return decoded.value;
  },
  async latestDiscoveries(ns, connection, options) {
    requireDomain(ns, "topic-discoveries");
    const decoded = decodeDiscoveryLatest(
      await consoleSub(ns, "connections", connection, "topic-discoveries",
        Object.assign({}, options || {}, { latest: true })),
    );
    return {
      latestAttempt: decoded.value.latestAttempt,
      lastSuccessful: decoded.value.lastSuccessful,
      unknown: decoded.unknown,
    };
  },
  async startDiscovery(ns, connection, request, options) {
    requireOperator(ns, "topic-discoveries", "start a topic discovery");
    const body = sending("connections:topic-discoveries", request || {});
    // `attempt` IS THE PAGE'S INTENT TOKEN: the parameters alone
    // are the same key every time, and a discovery past its freshness would
    // be replayed for as long as the controller keeps the object.
    const attempt = (options || {}).attempt;
    const subject = connection + "." + discoveryTag(body) +
      (typeof attempt === "string" && attempt.length > 0 ? "." + digest32(attempt) : "");
    const answer = await consoleAction(ns, "connections:topic-discoveries", connection, body, {
      idempotencyKey: idempotencyKey("topic-discoveries", ns, subject),
      token: tokenNow(),
    });
    const decoded = decodeConsoleItem("topic-discoveries", answer);
    return {
      item: decoded.value.item,
      replayed: decoded.value.replayed === true,
      reused: decoded.value.reused === true,
    };
  },
  async discovery(ns, id, options) {
    requireDomain(ns, "topic-discoveries");
    const decoded = decodeConsoleItem("topic-discoveries", await consoleGet(ns, "topic-discoveries", id, options));
    return { item: decoded.value.item, unknown: decoded.unknown };
  },
  async discoveryTopics(ns, id, options) {
    requireDomain(ns, "topic-discoveries");
    const decoded = decodeTopicPage(
      await consoleSub(ns, "topic-discoveries", id, "topics",
        Object.assign({ limit: TOPIC_PAGE_SIZE }, options || {})),
    );
    return decoded.value;
  },
  async cancelDiscovery(ns, id) {
    requireOperator(ns, "topic-discoveries", "cancel a topic discovery");
    const decoded = decodeCancel(
      await consoleAction(ns, "topic-discoveries:cancel", id, {}, { token: tokenNow() }),
    );
    return decoded.value;
  },
  async startPreflight(ns, request, options) {
    requireOperator(ns, "preflights", "start a readiness check");
    const body = sending("preflights", request);
    const answer = await consoleCreate(ns, "preflights", body, {
      idempotencyKey: idempotencyKey(
        "preflights", ns, preflightTag(body, (options || {}).attempt),
      ),
      token: tokenNow(),
    });
    const decoded = decodeConsoleItem("preflights", answer);
    return { item: decoded.value.item, replayed: decoded.value.replayed === true };
  },
  async preflight(ns, id, options) {
    requireDomain(ns, "preflights");
    // `?planHash=` IS THE QUESTION, NOT A FILTER. It asks the product API to
    // recompute `applicable` and `staleReasons` against the plan the operator
    // is looking at NOW. Omitting it is a different question -- "is this
    // result still about its own inputs" -- and not a weaker one.
    const decoded = decodeConsoleItem("preflights", await consoleGet(ns, "preflights", id, options));
    return { item: decoded.value.item, unknown: decoded.unknown };
  },
  async preflightDetails(ns, id, options) {
    requireDomain(ns, "preflights");
    const decoded = decodeDetailPage(
      await consoleSub(ns, "preflights", id, "details",
        Object.assign({ limit: DETAIL_PAGE_SIZE }, options || {})),
    );
    return decoded.value;
  },
  async cancelPreflight(ns, id) {
    requireOperator(ns, "preflights", "cancel a readiness check");
    const decoded = decodeCancel(
      await consoleAction(ns, "preflights:cancel", id, {}, { token: tokenNow() }),
    );
    return decoded.value;
  },
  async checkOperation(ns, kind, name, options) {
    requireDomain(ns, kind === "discovery" ? "topic-discoveries" : "preflights");
    const decoded = decodeCheckOperation(await consoleOperation(ns, kind, name, options));
    return { item: decoded.value.item, unknown: decoded.unknown };
  },
});

/** The page size a topic listing asks for. The product API's own maximum for
 *  that route; a smaller one only means more round trips over the same chunks. */
export const TOPIC_PAGE_SIZE = 200;

/** The page size a preflight's detail document is read in. */
export const DETAIL_PAGE_SIZE = 100;

// The synchroniser token, or null. One expression, so no method below has to
// remember the `decided === null` arm.
function tokenNow() {
  return decided === null ? null : decided.token;
}

/** THE SESSION'S SYNCHRONISER TOKEN, for the one product-API write that is not
 *  a method of the client object: `ui/operation-watch.js`'s `connectArchive`.
 *  `null` without a session and before the probe has answered.
 *
 *  EVERY UNSAFE REQUEST CARRIES THIS VALUE AND NO OTHER. The product API
 *  refuses a write whose `X-CSRF-Token` does not match the session that sent
 *  it, so a write that takes its token from anywhere else (a caller's options
 *  bag, a route's `deps`) is refused. Like the token itself this lives in
 *  this module's memory and nowhere else. */
export function sessionToken() {
  return tokenNow();
}

// The roles a destination test exercises, as one stable tag for the
// idempotency key. Sorted, because `["archiveRead","archiveWrite"]` and
// `["archiveWrite","archiveRead"]` are the same test and must not be two.
function rolesTag(roles) {
  return Array.isArray(roles) && roles.length > 0 ? roles.slice().sort().join("-") : "configured";
}

// A discovery's parameters, as one tag. Two requests that differ only in
// `reuseFresh` are the same discovery to the server's own parameter hash, so
// they are the same key here too.
function discoveryTag(body) {
  return [
    body.includeInternal === true ? "i1" : "i0",
    "m" + String(typeof body.maxTopics === "number" ? body.maxTopics : 0),
    "t" + String(typeof body.timeoutSeconds === "number" ? body.timeoutSeconds : 0),
    "e" + digest32((Array.isArray(body.expectedTopics) ? body.expectedTopics : []).join(",")),
  ].join(".");
}

// A preflight's subject, as one tag: the operation plus a digest over the
// block that names what it is about. A second click on "Check readiness" for
// the SAME plan is the same check; an edited plan is a different one, because
// the plan hash is inside the digest.
//
// `attempt` IS THE CALLER'S "THIS IS A NEW ONE". A readiness check over an
// unchanged plan passes none, so a retry replays. A connectivity test's
// subject is a broker that may answer differently a minute later, so its
// page mints one token per accepted CLICK; without it "Test connection"
// would replay its first answer for ever.
function preflightTag(body, attempt) {
  const block = body[body.operation];
  const subject = body.operation + "." +
    digest32(JSON.stringify(block === undefined ? null : block));
  return typeof attempt === "string" && attempt.length > 0
    ? subject + "." + digest32(attempt)
    : subject;
}

function consolePlural(plural) {
  const route = CONSOLE_PLURAL[plural];
  if (route === undefined) {
    throw noRoute("the product API has no route for " + String(plural) + ".");
  }
  return route;
}

function requireGrant(ns, flag) {
  if (typeof flag !== "string" || !granted(ns, flag)) {
    throw notGranted(ns, flag);
  }
}

// A 409 from the product API means the name this operation would take is
// taken by something else. It is reported as a conflict so a form says so
// and keeps what was typed, instead of reading as a generic refusal.
function conflictKind(error) {
  const code = (error || {}).code;
  if (code === "idempotency_conflict" || code === "state_conflict") {
    error.kind = "conflict";
  }
  return error;
}

/** The product API's create body for `plural`, built from the custom resource
 *  the page composed. THE ONLY PLACE the two request shapes differ, and the
 *  differences are named: the product API mints the object's NAME from the
 *  idempotency scope, so the name the operator typed is what makes the
 *  submission repeatable rather than what the object is called; and it spells
 *  a Secret reference `credentialRef`. */
function requestBody(plural, object) {
  const spec = (object || {}).spec || {};
  if (plural === "kafkaclusters") {
    // REFUSED, NOT DROPPED. `CreateConnectionRequest` declares
    // `additionalProperties: false` and has no field for either of connection
    // contract v1's references, so there is no shape this module could put
    // them in. Sending the request without them would create a connection that
    // projects the legacy data key, or that dials without the private CA the
    // operator named -- a different connection from the one the form
    // described. The refusal is by name, like every other console-mode gap.
    if (((spec.auth.secretRef) || {}).passwordKey) {
      throw noRoute(
        "the product API's connection create has no field for spec.auth.secretRef.passwordKey " +
          "(saved-connection contract v1), so this connection cannot be created through it " +
          "without changing which entry of the Secret the controller projects. Leave the field " +
          "blank to use the Secret's default entry, or ask an administrator with access to the " +
          "cluster to create this connection.",
      );
    }
    // A CA from a ConfigMap travels; one held in a Secret does not: the product
    // API takes the CA from a ConfigMap only, so every Secret a console-made
    // connection uses is one the API created itself.
    const ca = spec.auth.tlsCa;
    if (ca !== undefined && ca !== null && ca.secretKeyRef) {
      throw noRoute(
        "the product API takes a connection's private CA from a ConfigMap only (a CA " +
          "certificate is public), so this connection cannot be created through it with a " +
          "Secret-held CA. Put the CA certificate in a ConfigMap, or ask an administrator with " +
          "access to the cluster to create this connection.",
      );
    }
    // THE CONSOLE NEVER NAMES AN EXISTING SECRET. A connection that could name
    // any Secret could make Logweir present another team's credential to
    // brokers of its author's choosing; the product API refuses
    // `credentialRef`, and this module never sends one.
    if (((spec.auth.secretRef) || {}).name || ((spec.auth.clientCertificate) || {}).name) {
      throw noRoute(
        "the product API never names an existing Secret for a connection's credential: enter " +
          "the password or the client certificate and key in the form, and the API stores it in " +
          "a Secret owned by and bound to this connection.",
      );
    }
    const auth = { mode: spec.auth.mode, tls: spec.auth.tls === true };
    if (typeof spec.auth.username === "string" && spec.auth.username.length > 0) {
      auth.username = spec.auth.username;
    }
    // THE WRITE-ONLY VALUE, off the object's own (non-enumerable) field: this
    // is the one place it is read, and it goes straight into the request.
    const entered = (object || {}).__credential;
    if (entered !== undefined && entered !== null) {
      auth.credential = Object.assign({}, entered);
    }
    if (ca !== undefined && ca !== null && ca.configMapKeyRef) {
      auth.tlsCa = {
        configMapKeyRef: { name: ca.configMapKeyRef.name, key: ca.configMapKeyRef.key },
      };
    }
    const body = { role: spec.role, bootstrapServers: spec.bootstrapServers.slice(), auth: auth };
    if (typeof spec.markerTopic === "string" && spec.markerTopic.length > 0) {
      body.markerTopic = spec.markerTopic;
    }
    return body;
  }
  if (plural === "backupschedules") {
    // THE WHOLE POLICY, BECAUSE THE ROUTE TAKES THE WHOLE POLICY. A field ABSENT
    // from the custom resource stays absent from the body: an absent policy
    // field is the documented default rather than a value this module invented.
    //
    // ONE LOCATION AND ONE SELECTION, decided by the object the page composed:
    // `destinationRef` replaces `archive` and `allUserTopics` replaces a named
    // `topics`, and the API refuses a body carrying both halves of either pair.
    const body = {
      schedule: spec.schedule,
      sourceRef: { name: spec.sourceRef.name },
      topics: Array.isArray(spec.topics) ? spec.topics.slice() : [],
      suspended: spec.suspend === true,
    };
    if (spec.destinationRef !== undefined && spec.destinationRef !== null) {
      body.destinationRef = { name: spec.destinationRef.name };
    } else {
      body.archive = requestArchive(spec.archive);
    }
    if (spec.allUserTopics !== undefined && spec.allUserTopics !== null) {
      body.allUserTopics = spec.allUserTopics;
    }
    for (const field of [
      "timeZone", "concurrencyPolicy", "startingDeadlineSeconds", "catchUpPolicy",
      "retry", "activeDeadlineSeconds",
    ]) {
      if (spec[field] !== undefined && spec[field] !== null) {
        body[field] = spec[field];
      }
    }
    return body;
  }
  if (plural === "restores") {
    // THE HASH IS THE ONE THAT WAS ON SCREEN, OR THERE IS NO REQUEST. The
    // product API takes `planHash` beside `planBytes` and the controller
    // recomputes it, so a hash this module computed for itself would be a
    // second opinion about bytes an approver signs. `preparedFor` answers only
    // for a document `ui/plan.js` prepared -- which is the document the review
    // step rendered -- and bytes that reached here another way have no answer
    // and are refused before anything is sent.
    const document = preparedFor(spec.planBytes);
    if (document === null) {
      throw noRoute(
        "these plan bytes were not prepared by this page's plan module, so the hash the " +
          "product API requires beside them is not one this page reviewed. Reopen the wizard " +
          "and submit the plan it shows.",
      );
    }
    const body = {
      planBytes: document.bytes,
      planHash: document.hash,
      approvalRef: { name: spec.approvalRef.name },
      sourceArchive: requestArchive(spec.sourceArchive),
      backupSetRef: spec.backupSetRef,
      pointInTime: spec.pointInTime,
      target: {
        clusterRef: { name: spec.target.clusterRef.name },
        mode: spec.target.mode,
        topicNaming: { prefix: spec.target.topicNaming.prefix },
      },
      deadlineSeconds: spec.deadlineSeconds,
    };
    // THE ORIGINAL-NAME DECLARATION TRAVELS WITH THE EMPTY PREFIX:
    // `TopicNamingRequest.originalName` tells the product API that an empty
    // prefix is a choice and not a missing field. Sent only when stated.
    if (spec.target.topicNaming.originalName === true) {
      body.target.topicNaming.originalName = true;
    }
    if (spec.sourceDestinationRef !== undefined) {
      body.sourceDestinationRef = { name: spec.sourceDestinationRef.name };
    }
    if (spec.evidenceDestinationRef !== undefined) {
      body.evidenceDestinationRef = { name: spec.evidenceDestinationRef.name };
    }
    // The coverage the plan asks for, declared beside it and stored on
    // `Restore.spec`, where the controller holds it to the plan. Sent only when
    // stated, so a sampled restore sends what it always did.
    if (spec.coverage === "complete") {
      body.coverage = "complete";
      if (typeof spec.completeMaxRecords === "number") {
        body.completeMaxRecords = spec.completeMaxRecords;
      }
    }
    // THE DECLARED MAPPING RIDES ON THE REQUEST AND NOT ON THE OBJECT:
    // `Restore.spec` has no topic list, so this travels beside the create body
    // the wizard built, and the product API recomputes every row from
    // `target.topicNaming.prefix` and answers 422 when the preview and the
    // submission disagree. Carried only when it is there. The change ticket
    // rides the same way: a field of the REQUEST, signed into the authorization
    // document, never of `Restore.spec`.
    const ticket = (object || {}).ticket;
    if (typeof ticket === "string" && ticket.length > 0) {
      body.ticket = ticket;
    }
    const mapping = (object || {}).topicMapping;
    if (Array.isArray(mapping) && mapping.length > 0) {
      body.topicMapping = mapping.map((row) => ({
        source: (row || {}).source,
        target: (row || {}).target,
      }));
    }
    // THE ORIGINAL TOPIC NAMES THE REQUESTER RE-TYPED ride beside the body the
    // same way: a field of the REQUEST, which the console signs into the
    // authorization document, never of `Restore.spec`.
    const typed = ((object || {}).originalNameConfirmation || {}).typedTopics;
    if (Array.isArray(typed)) {
      body.originalNameConfirmation = { typedTopics: typed.map((name) => String(name)) };
    }
    // AND THE BODY IS HELD TO THE PUBLISHED REQUEST SHAPE BEFORE IT IS SENT,
    // like every other create's (`sending`).
    return sending("restores", body);
  }
  throw noRoute("the product API has no create route for " + String(plural) + ".");
}

function requestArchive(ref) {
  const out = { url: ref.url };
  if (((ref.secretRef) || {}).name) {
    out.credentialRef = { name: ref.secretRef.name };
  }
  return out;
}

/** True for a refusal that means "this view does not get that extra", and
 *  false for one that means "the answer was not what the contract says".
 *
 *  THE DISTINCTION IS THE WHOLE POINT OF [`enrich`]. A 403 or a 404 from the
 *  operation route or the packet route is a fact about this actor or this
 *  object -- the detail view stands without them and always did. A CONTRACT
 *  FAILURE is a fact about the SERVER: the evidence block would render empty,
 *  and an empty evidence block is exactly what "the controller recorded
 *  nothing" looks like. `ui/contract.js`'s own header names that cell as the
 *  thing this module exists to stop, and `ui/README.md` promises it is never
 *  absorbed. So it is not absorbed. Nor is a 5xx or a timeout, which say the
 *  server could not answer rather than that it would not. */
function isMissingExtra(error) {
  if (isContractFailure(error)) {
    return false;
  }
  const status = (error || {}).status;
  return status === 403 || status === 404;
}

/** A detail read's second half: the fields the product API keeps on their own
 *  routes. The operation carries the evidence and the exit; the approval
 *  packet carries the document bytes, and is read ONLY through the route that
 *  is named for it and ONLY when the session grants it.
 *
 *  A refusal that means the extra is not for this view is not a failure of the
 *  detail view and the object is returned without it. Anything else -- a
 *  contract failure, a 5xx, a transport failure -- is raised, so the page shows
 *  what went wrong instead of a cell that cannot be told from an empty one. */
async function enrich(ns, plural, name, object, options) {
  if (plural === "backups" || plural === "restores") {
    if (!granted(ns, "operationsRead")) {
      return object;
    }
    try {
      const body = await consoleOperation(
        ns, plural === "backups" ? "backup" : "restore", name, options,
      );
      // ONE DECODE OF THE DOCUMENT THE ROUTE PUBLISHES: the frozen sixteen and
      // every member D3 added, by the view's own shape.
      return mergeOperation(object, decodeOperation(body).value.item);
    } catch (unread) {
      if (isMissingExtra(unread)) {
        return object;
      }
      throw unread;
    }
  }
  if (plural === "approvals" && granted(ns, "approvalPacketRead")) {
    try {
      const decoded = decodeApprovalPacket(
        await consoleSub(ns, "approvals", name, "packet", options),
      );
      object.spec.approvalBytes = decoded.value.item.approvalBytes;
      object.spec.sidecarBytes = decoded.value.item.sidecarBytes;
    } catch (unread) {
      // The metadata view stands on its own; the bytes are an extra -- but
      // only when the answer was a refusal and not a broken promise.
      if (!isMissingExtra(unread)) {
        throw unread;
      }
    }
  }
  return object;
}

/** A namespace with more objects than one console list will read. `status` is
 *  0 because the server answered every page it was asked for; what could not
 *  be done was done here. */
function tooMany(ns, plural, read) {
  const error = new Error(
    "namespace " + String(ns) + " holds more than " + String(read) + " " + String(plural) +
      ", which is more than this page reads in one list. It is not showing you the first " +
      String(read) + " as if they were all of them. An administrator can read the whole " +
      "namespace from the cluster, or move some of them to another namespace.",
  );
  error.kind = "refused";
  error.status = 0;
  error.reason = "ListTooLarge";
  return error;
}

/** THE ONE OBJECT EVERY PAGE READS THROUGH. Frozen, with an identity that
 *  never changes, so a page holds it at module scope exactly as it held
 *  `{list, get, create}` before.
 *
 *  ONCE THE MODE IS DECIDED, A CALL DISPATCHES SYNCHRONOUSLY. That is not a
 *  micro-optimisation: `createRouteLifecycle` aborts the PREVIOUS route's
 *  reads when the next route begins, and a read that had not reached the
 *  network yet would be started after its own route had already gone. The
 *  mode is decided at boot, before the first mount, so this is the path every
 *  real render takes; the awaiting branch below exists for a caller that ran
 *  before the boot probe answered, and it waits for the record rather than
 *  guessing a mode. */
export function apiClient() {
  return Object.freeze({
    list(ns, plural, options) {
      return dispatch((api) => api.list(ns, plural, options));
    },
    get(ns, plural, name, options) {
      return dispatch((api) => api.get(ns, plural, name, options));
    },
    create(ns, plural, object) {
      return dispatch((api) => api.create(ns, plural, object));
    },
    patchSuspend(ns, name, value) {
      return dispatch((api) => api.patchSuspend(ns, name, value));
    },
    listCluster(plural, options) {
      return dispatch((api) => api.listCluster(plural, options));
    },

    // The draft cadence preview, the future-policy replace and "Back up now".
    // They go through `dispatch` and not `dispatchChecks` because they are the
    // SAME kinds this facade has always served (a schedule and a Backup), not a
    // domain of their own.
    previewCadence(query, options) {
      return dispatch((api) => api.previewCadence(query, options));
    },
    editSchedulePolicy(ns, name, body) {
      return dispatch((api) => api.editSchedulePolicy(ns, name, body));
    },
    runBackupNow(ns, body, key) {
      return dispatch((api) => api.runBackupNow(ns, body, key));
    },
    approvalPolicy(ns, options) {
      return dispatch((api) => api.approvalPolicy(ns, options));
    },
    submitGovernedApproval(ns, restoreName, sidecarBytes, approvalBytes) {
      return dispatch((api) => api.submitGovernedApproval(ns, restoreName, sidecarBytes,
        approvalBytes));
    },
    approvalRequest(ns, restoreName, options) {
      return dispatch((api) => api.approvalRequest(ns, restoreName, options));
    },
    approveInConsole(ns, restoreName, confirmationSha256) {
      return dispatch((api) => api.approveInConsole(ns, restoreName, confirmationSha256));
    },

    // Saved destinations, bounded topic discovery and operation readiness.
    destinations(ns, options) {
      return dispatchChecks((api) => api.destinations(ns, options));
    },
    destination(ns, name, options) {
      return dispatchChecks((api) => api.destination(ns, name, options));
    },
    createDestination(ns, request) {
      return dispatchChecks((api) => api.createDestination(ns, request));
    },
    updateDestinationAccess(ns, name, request) {
      return dispatchChecks((api) => api.updateDestinationAccess(ns, name, request));
    },
    testDestination(ns, name, request, options) {
      return dispatchChecks((api) => api.testDestination(ns, name, request, options));
    },
    destinationUsage(ns, name, options) {
      return dispatchChecks((api) => api.destinationUsage(ns, name, options));
    },
    latestDiscoveries(ns, connection, options) {
      return dispatchChecks((api) => api.latestDiscoveries(ns, connection, options));
    },
    startDiscovery(ns, connection, request, options) {
      return dispatchChecks((api) => api.startDiscovery(ns, connection, request, options));
    },
    discovery(ns, id, options) {
      return dispatchChecks((api) => api.discovery(ns, id, options));
    },
    discoveryTopics(ns, id, options) {
      return dispatchChecks((api) => api.discoveryTopics(ns, id, options));
    },
    cancelDiscovery(ns, id) {
      return dispatchChecks((api) => api.cancelDiscovery(ns, id));
    },
    startPreflight(ns, request, options) {
      return dispatchChecks((api) => api.startPreflight(ns, request, options));
    },
    preflight(ns, id, options) {
      return dispatchChecks((api) => api.preflight(ns, id, options));
    },
    preflightDetails(ns, id, options) {
      return dispatchChecks((api) => api.preflightDetails(ns, id, options));
    },
    cancelPreflight(ns, id) {
      return dispatchChecks((api) => api.cancelPreflight(ns, id));
    },
    checkOperation(ns, kind, name, options) {
      return dispatchChecks((api) => api.checkOperation(ns, kind, name, options));
    },
  });
}

function dispatch(call) {
  if (decided !== null) {
    try {
      return Promise.resolve(call(implementation(decided, consoleApi, signedOutApi)));
    } catch (refused) {
      return Promise.reject(refused);
    }
  }
  return ensure().then((record) =>
    call(implementation(record, consoleApi, signedOutApi)));
}

// WHICH IMPLEMENTATION A DECIDED RECORD USES: the console's, or the refusals
// a signed-out or unreachable page answers.
function implementation(record, consoleImpl, signedOutImpl) {
  return record.mode === CONSOLE ? consoleImpl : signedOutImpl;
}

// Every method of `impl`, answering the refusal for the decided state --
// signed out, or the service not reached -- and sending nothing.
function refusingAll(impl) {
  const out = {};
  for (const key of Object.keys(impl)) {
    out[key] = () => {
      throw decided !== null && decided.mode === UNAVAILABLE
        ? unavailableError()
        : signedOutError();
    };
  }
  return Object.freeze(out);
}

// The refusal while the console could not be reached: nothing was sent.
function unavailableError() {
  const error = new Error(
    "The Logweir service could not be reached when this page loaded, so nothing was read or " +
      "sent. Retry once it answers.",
  );
  error.kind = "refused";
  error.status = 503;
  error.reason = "service_unavailable";
  return error;
}

// The D2 half dispatches exactly as the five older kinds do, over its own pair
// of implementations: the older kinds are PROJECTED into custom resources, and
// these three are not.
function dispatchChecks(call) {
  if (decided !== null) {
    try {
      return Promise.resolve(call(implementation(decided, consoleChecks, signedOutChecks)));
    } catch (refused) {
      return Promise.reject(refused);
    }
  }
  return ensure().then((record) =>
    call(implementation(record, consoleChecks, signedOutChecks)));
}

// The signed-out halves: the same method names, each refusing by name.
const signedOutApi = refusingAll(consoleApi);
const signedOutChecks = refusingAll(consoleChecks);

/** The console implementation, by name, for the suite. A page never picks:
 *  `apiClient` does. */
export const MODES = Object.freeze({ console: consoleApi });

/** The D2 half of the same. */
export const CHECK_MODES = Object.freeze({ console: consoleChecks });
