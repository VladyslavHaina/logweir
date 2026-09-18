// client.js -- which API is in front of this page, decided ONCE, and the one
// object every page reads through.
//
// TWO DEPLOYMENTS, ONE PAGE (decision D0, "Static migration" 1-2). The same
// files are served two ways. Behind `kubectl proxy` the page talks to
// kube-apiserver at `/apis/logweir.dev/v1alpha1/...` with the viewer's own
// kubeconfig credential attached by the proxy -- LEGACY MODE, which is what
// ships today and what `ui.enabled` keeps rendering for a compatibility
// release. Behind `logweir-api` the same files talk to a bounded product API
// at `/api/v1/...` on the same origin -- CONSOLE MODE. Nothing else about the
// page changes: same directory, same hash routes, same deep links.
//
// THE CHOICE IS MADE ONCE, BY ONE REQUEST. At boot the page asks
// `GET /api/v1/session`. An answer that decodes as a session document is
// console mode, and that document is also where the namespace grants come
// from. Anything else -- a refusal from the proxy's path filter, a body that
// is not JSON, no answer inside the bound below -- is legacy mode. The result
// is recorded in this module and never asked again: a mode decided per request
// is a page that can change APIs between a read and the write that follows it.
//
// NO TOKEN IS EVER STORED IN THE BROWSER. Console mode authenticates with a
// session cookie the browser attaches by itself to a same-origin request; this
// page neither reads nor writes it. The synchroniser token the session
// document carries is held in THIS MODULE'S MEMORY for the life of the loaded
// page -- the same place a draft lives -- and is gone on reload. There is no
// browser storage anywhere in this tree, and `scripts/check-ui-offline.sh`
// fails the build over the byte sequences that would introduce one.
//
// WHAT A PAGE SEES. Exactly the object it saw before: `{list, get, create,
// patchSuspend, listCluster}` over Kubernetes-shaped custom resources. In
// legacy mode that is what the API server sent, validated and passed through
// unchanged. In console mode the product DTO is decoded strictly and then
// PROJECTED BACK onto the custom resource's shape, because the product API's
// objects ARE projections of those resources and the pages are views of them.
// Every field the projection cannot supply is named in `ui/README.md` and
// recorded on the object under `__contract`, never quietly defaulted.
//
// THIS MODULE ISSUES NO REQUEST OF ITS OWN. Every identifier is built and
// every response is read by `api.js`, which holds the one `fetch` in the tree.

import {
  cadencePreview,
  consoleAction,
  consoleCreate,
  consoleGet,
  consoleList,
  consoleOperation,
  consoleSchedulePolicy,
  consoleSetSuspension,
  consoleSub,
  create,
  get,
  list,
  listCluster,
  patchSuspend,
  session,
} from "./api.js";
import {
  contractFailure,
  decodeApprovalPacket,
  decodeCadencePreview,
  decodeCancel,
  decodeCheckOperation,
  decodeConsoleItem,
  decodeConsoleList,
  decodeDestinationUsage,
  decodeDetailPage,
  decodeDiscoveryLatest,
  decodeLegacyList,
  decodeLegacyObject,
  decodeManualBackup,
  decodeOperation,
  decodePolicyChanged,
  decodeRequest,
  decodeSession,
  decodeTopicPage,
  isContractFailure,
} from "./contract.js";
import { preparedFor } from "./plan.js";
import { causesFrom, validateRequest } from "./validate.js";

/** The two modes, by name. */
export const LEGACY = "legacy";
/** @see LEGACY */
export const CONSOLE = "console";

/** How long the boot probe waits before deciding this is legacy mode. A page
 *  behind a proxy that never answers must still render: the refusal IS the
 *  legacy answer, and a missing refusal cannot be allowed to hang the shell. */
export const PROBE_TIMEOUT_MS = 5000;

/** The Kubernetes plural each product plural stands for, and back. ONE table,
 *  so a page names `kafkaclusters` in both modes and nothing else translates. */
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

// --------------------------------------------------------------- the record

let decided = null;
let probing = null;

/** The mode, or `null` before the boot probe has answered. */
export function mode() {
  return decided === null ? null : decided.mode;
}

/** The decoded session document in console mode, `null` in legacy mode and
 *  before the probe. Never carries a credential: the product API's session
 *  document carries an actor, grants and flags. */
export function sessionDocument() {
  return decided === null ? null : decided.session;
}

/** The namespaces this actor is granted, from `/session` in console mode. An
 *  empty list in legacy mode, where the namespace is the explicit selection
 *  PLAT-13.1 introduced and this page does not list namespaces. */
export function grantedNamespaces() {
  return decided === null ? [] : decided.namespaces.slice();
}

/** The PRODUCT roles the actor holds in `ns`, as the session reported them.
 *
 *  Empty in legacy mode and in localAdmin mode, and empty is not "none of the
 *  above": it is a mode with no roles at all. Nothing on this page branches on
 *  a role -- `granted` below is what decides whether a call is made, because a
 *  capability flag is `implemented && allowed` and a role is neither. This is
 *  here so a view can SAY who the actor is without guessing it from the flags. */
export function rolesFor(ns) {
  const held = decided === null ? undefined : decided.roles[ns];
  return held === undefined ? [] : held.slice();
}

/** The revision of the administrator's binding table that produced the current
 *  grants, or the empty string when there is none (legacy and localAdmin). The
 *  product API records it in every audit line; carrying it here is what lets a
 *  reader tie what this page shows to the configuration that allowed it. */
export function bindingRevision() {
  return decided === null ? "" : decided.bindingRevision;
}

/** Whether `flag` is granted for `ns`. In legacy mode every capability is
 *  "granted" here and the API server's RBAC is the gate, which is the whole
 *  authorisation story of that mode. */
export function granted(ns, flag) {
  if (decided === null || decided.mode !== CONSOLE) {
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
    // RESOLVES the decision -- legacy, which is what a page that cannot reach
    // the product API is looking at -- and aborts the request on its way out.
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
      }, PROBE_TIMEOUT_MS);
    });
    const answer = await Promise.race([asked, bounded]);
    if (answer === null || answer === undefined || answer.ok !== true) {
      return legacyRecord();
    }
    const decoded = decodeSession(answer.body);
    return consoleRecord(decoded.value, decoded.unknown);
  } catch (refused) {
    // EVERY REFUSAL IS THE SAME ANSWER: this is not the product API. A path
    // filter's 403, a body that is not a session document, a decode that found
    // a required field missing, an abort at the bound above -- each of them
    // says the page is not behind `logweir-api`, and legacy mode is what it is
    // behind instead.
    return legacyRecord();
  } finally {
    if (timer !== null) {
      globalThis.clearTimeout(timer);
    }
  }
}

function legacyRecord() {
  return Object.freeze({
    mode: LEGACY,
    session: null,
    namespaces: Object.freeze([]),
    grants: Object.freeze(Object.create(null)),
    roles: Object.freeze(Object.create(null)),
    // Legacy mode has no product roles and no binding table: the API server's
    // own RBAC is the whole authorisation story there.
    bindingRevision: "",
    token: null,
    unknown: Object.freeze([]),
  });
}

function consoleRecord(document, unknown) {
  const grants = Object.create(null);
  const roles = Object.create(null);
  const names = [];
  for (const grant of document.namespaces) {
    grants[grant.name] = grant.capabilities;
    // THE ROLES COME FROM THE SESSION AND ARE NEVER DERIVED HERE (PLAT-17.2).
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

// ------------------------------------------------------------- the refusals

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

// ---------------------------------------------------------- the idempotency

// THE KEY IS A FUNCTION OF WHAT IS BEING CREATED, NOT OF WHEN. A retry after a
// lost response, a double click and a reload all compose the same key, so the
// product API answers the second request with the object the first one made --
// which is the same property the legacy mode gets from naming the object. A
// DELIBERATELY NEW operation is a new name, and a new name is a new key.
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

// ---------------------------------------------------------- the projections

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
// wrote. `ui/README.md` lists all ten.
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

function operationStatus(summary) {
  const status = { phase: PHASE_OF[summary.state] };
  if (summary.stateReason !== null) {
    status.reason = summary.stateReason;
  }
  return status;
}

/** The fields a console projection of `plural` cannot supply, named. Recorded
 *  on every projected object under `__contract.absent` so a page, a test or a
 *  reader can see what is missing instead of reading an empty cell and
 *  guessing why. */
const ABSENT_IN_CONSOLE = Object.freeze({
  // CONNECTION CONTRACT v1's TWO REFERENCES ARE NOT IN THE PRODUCT API YET.
  // `schemas/logweir-api-v1.openapi.json`'s `ConnectionAuthView` carries the
  // mode, the username, `credentialRef` and `tls` and nothing else, so a
  // console-mode projection cannot say which data key of the Secret the
  // controller projects, nor which private CA this connection trusts. They are
  // NAMED here rather than left as empty cells, and `requestBody` refuses a
  // create that carries either instead of dropping it: a form that silently
  // sent a connection without its CA would produce an object that dials
  // without one.
  kafkaclusters: Object.freeze([
    "status.conditions",
    "spec.auth.secretRef.passwordKey",
    "spec.auth.tlsCa",
  ]),
  // D1 W7. THE CONTROLLER RECORDS THESE AND THE PRODUCT API DOES NOT PUBLISH
  // THEM. `ScheduleStatusView` carries `policy`, `nextRuns` and `activeRuns`
  // and stops there, so a console-mode schedule card can show the revision and
  // the previews and cannot show what happened to the last slot or how many
  // came due and were skipped. Naming them here is the difference between a
  // gap and an omission: the page prints the sentence that says which task
  // owes the projection, and legacy mode -- which reads the custom resource
  // itself -- renders all four.
  backupschedules: Object.freeze([
    "status.retentionReport.skipped[].key",
    "status.retentionReport.skipped[].reason",
    "status.lastSlot",
    "status.missedSlots",
    "status.pendingRun",
    "status.history",
  ]),
  // AND THE SAME FOR A RUN'S COVERAGE. `Backup` publishes `trigger` and
  // `scheduleRef` (D1 W6) and no `status.selection` at all, so the coverage
  // label `weirkeeper` writes on every dynamic run -- the one fact that says
  // whether "all topics" was attested or only visible -- is not reachable in
  // console mode. The page will not infer one from the mode, the topic list or
  // a successful phase; it says so and names the route that owes it.
  backups: Object.freeze([
    "status.manifestSha256",
    "status.jobRef",
    "status.selection",
    "status.conditions",
  ]),
  restores: Object.freeze(["status.integrity", "status.jobRef"]),
  approvals: Object.freeze([]),
});

function note(object, plural, unknown) {
  object.__contract = {
    mode: CONSOLE,
    absent: ABSENT_IN_CONSOLE[plural] || [],
    unknown: unknown,
  };
  return object;
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
  // THE TWO FIELDS D1 W6 ADDED, PROJECTED UNDER THE NAMES THE CRD USES.
  // `spec.destinationRef` is the saved destination this schedule writes to,
  // and `spec.allUserTopics` is its dynamic-selection policy. Both are absent
  // on every schedule written before PLAT-06.2, and absent keeps meaning what
  // it meant: an inline archive, and a named allowlist.
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
  // THE EDITABLE FUTURE POLICY (D1 W7, PLAT-04.2/05.1), UNDER THE CRD's OWN
  // NAMES. Every one of these is optional on the wire and ABSENT on every
  // schedule written before PLAT-04.2, and absent keeps meaning what it meant:
  // UTC evaluation, a one-hour starting deadline, no catch-up, no retry and a
  // 3600 s run deadline. The projection invents none of them -- the form
  // renders "the default, N" for an absent field and sends nothing for it.
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
  if (view.activeBackup !== null) {
    status.activeBackupRef = { name: view.activeBackup };
  }
  if (view.pendingBackup !== null) {
    status.pendingBackupRef = { name: view.pendingBackup };
  }
  if (view.ready !== null) {
    status.conditions = [condition(view.ready)];
  }
  if (view.retentionReport !== null) {
    const r = view.retentionReport;
    const report = {
      setsKept: r.setsKept.slice(),
      setsThatWouldBeRemoved: r.setsThatWouldBeRemoved.map(removable),
      awsCli: r.removalCommands.slice(),
      mcCli: r.mcRemovalCommands.slice(),
    };
    for (const field of ["evaluatedAt", "keepLast", "keepDays"]) {
      if (r[field] !== null) {
        report[field] = r[field];
      }
    }
    status.retentionReport = report;
  }
  object.status = status;
  return object;
}

function activeRun(run) {
  return { name: run.name, kind: run.kind, attempt: run.attempt };
}

function removable(set) {
  const out = { backupId: set.backupId, newestRecordAt: set.newestRecordAt, reason: set.reason };
  if (set.days !== null) {
    out.days = set.days;
  }
  if (set.rank !== null) {
    out.rank = set.rank;
  }
  return out;
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
  // `schedule` IS THE OLD, COARSER FACT AND `scheduleRef` IS THE NEW ONE, and
  // the projection keeps them in one field the way the CRD does: the name
  // alone for a run created before PLAT-05.1, and the name with the uid,
  // generation and run-policy digest for one created after it. A run that
  // carries neither has no schedule, which is what a manual ad-hoc run is.
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
  // WHICH KIND OF RUN THIS IS (D1 W7, PLAT-05.1/06.2). ABSENT on every run
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
  object.status = status;
  return object;
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
  if (item.verifiedSubject !== null) {
    status.verifiedSubject = {
      kind: item.verifiedSubject.kind,
      name: item.verifiedSubject.name,
      namespace: item.verifiedSubject.namespace,
      uid: item.verifiedSubject.uid,
    };
  }
  object.status = status;
  object.__lengths = {
    approvalBytes: item.approvalBytesLength,
    sidecarBytes: item.sidecarBytesLength,
  };
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
 *  a list does not. */
function mergeOperation(object, operation) {
  const status = object.status;
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
  const recorded = VERIFICATION_OF[operation.verification.state];
  if (recorded !== undefined) {
    verification.result = recorded;
  }
  for (const field of ["payloadType", "matchedKeyId", "verifiedAt", "detail"]) {
    if (operation.verification[field] !== null) {
      verification[field] = operation.verification[field];
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
  return object;
}

// ------------------------------------------------------------- the two APIs

const legacyApi = Object.freeze({
  async list(ns, plural, options) {
    return decodeLegacyList(plural, await list(ns, plural, options)).value;
  },
  async get(ns, plural, name, options) {
    return decodeLegacyObject(plural, await get(ns, plural, name, options)).value;
  },
  async create(ns, plural, object) {
    return checked(plural, object, () => create(ns, plural, object));
  },
  async patchSuspend(ns, name, value) {
    return patchSuspend(ns, name, value);
  },
  async listCluster(plural, options) {
    return decodeLegacyList(plural, await listCluster(plural, options)).value;
  },
  // ------------------------------------------- D1 W7 (PLAT-04.2/05.1/06.2)
  //
  // TWO OF THE THREE ARE CONSOLE-ONLY, AND SAID SO BY NAME. There is no
  // cadence-preview route in front of `kubectl proxy` -- it is a computation
  // `logweir-api` performs with the controller's own tz database -- and the
  // browser will not evaluate cron to fill the gap, because a second
  // implementation is a second opinion about when a backup runs. The saved
  // schedule's `status.nextRuns` IS rendered in legacy mode: the controller
  // computed it, and the page reads the object.
  //
  // The policy replace is console-only for a different reason: `ui/api.js`
  // holds exactly one replace, it is built on the product API's
  // `expectedGeneration` precondition, and a JSON-merge patch against
  // kube-apiserver would have neither that precondition nor the API's
  // pre-write refusals. The CRD's own rules accept the edit either way, so
  // this is the page's boundary and not the cluster's, and the sentence says
  // which command does it instead.
  previewCadence() {
    return Promise.reject(consoleOnly("Previewing a cadence before saving it"));
  },
  editSchedulePolicy() {
    return Promise.reject(consoleOnly("Editing a schedule's future policy"));
  },
  runBackupNow() {
    // AND SO IS THE THIRD, FOR TWO REASONS THAT ARE BOTH ABOUT THIS MODE AND
    // NOT ABOUT THE FEATURE.
    //
    // THE NAME IS DERIVED FROM FACTS THE BROWSER DOES NOT HOLD. D1 section 8.2 makes
    // a manual run's name `logweir-manual-` plus base32 of a sha256 over
    // `(issuer, sub, namespace, route, key)` -- the AUTHENTICATED subject, as
    // the product API knows it. A browser has no issuer and no subject in
    // either mode, so a name minted here would be a DIFFERENT name from the
    // one the canonical path produces, and "kubectl, the API and the console
    // create the same object" would stop being true of the one field every
    // replay guard in this design rests on.
    //
    // AND THE IN-CLUSTER UI SERVICEACCOUNT HAS NO `create backups`.
    // `charts/logweir/templates/ui/ui.yaml` grants `create` on approvals,
    // kafkaclusters, backupschedules and restores and stops there; D1 section 8.5
    // adds this one, and that grant is a chart change with its own review.
    // Sending the create anyway would be a 403 this page would then have to
    // narrate, which is the thing refusing here by name exists to avoid.
    return Promise.reject(consoleOnly("Back up now"));
  },
});

// THE CLIENT'S OWN CHECKS RUN IN BOTH MODES, over the body about to be sent,
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
    // THE CURSOR IS FOLLOWED, AND A LIST IS NEVER SILENTLY SHORTENED.
    //
    // The product API pages; `kubectl proxy` does not, and every page in this
    // tree was written against a list that holds the namespace. A console mode
    // that stopped at the first fifty rows would show fewer Backups than the
    // legacy mode shows, in the same table, with nothing on screen saying so --
    // and `#/history` is the inventory somebody reads during an incident. So
    // the pages are followed to the end.
    //
    // AND THE END IS BOUNDED. A namespace larger than the budget below is not
    // quietly cut off either: it raises an error the page RENDERS, naming how
    // many rows were read and what to run instead. A prefix presented as the
    // whole is the failure this arm exists to prevent; a refusal that says so
    // is not that failure. Rendering "showing the first N, more exist" beside
    // the table would be better still, and it is PLAT-18.2's to add -- the
    // table, its footer and its copy belong to that task.
    const items = [];
    const unknown = [];
    let page = null;
    let cursor = null;
    for (let read = 0; read < LIST_PAGE_BUDGET; read += 1) {
      const query = Object.assign({}, options || {}, { limit: LIST_PAGE_SIZE });
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
    if (!Object.prototype.hasOwnProperty.call(CREATE_CAPABILITY, route)) {
      throw noRoute(
        "the product API has no create route for " + KIND_OF[plural] + ". Create it with " +
          "kubectl or the logweir CLI; this page will read it once it exists.",
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
      // "already existed" for a replay and "created" for a create, in this
      // mode exactly as the 409 read-back says it in the other one.
      made.__contract.replayed = decoded.value.replayed === true;
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
  // ------------------------------------------- D1 W7 (PLAT-04.2/05.1/06.2)
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
    // THE SAME AUTHORITY AS CREATING ONE. D1 W6 made editing a schedule's
    // future policy a new action with the SAME role row as creating one, and
    // left the capability flag `scheduleCreate` covering both; a route test
    // there pins the equality, so splitting the flag and splitting the
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
    // after clicking, and neither is a reason the click was refused -- D1 section 8.3
    // is explicit that nothing about a schedule blocks a manual run.
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

// ===========================================================================
// D2: destinations, topic discoveries and operation readiness
// ===========================================================================
//
// CONSOLE ONLY, AND SAID SO BY NAME. These three domains exist on the product
// API and nowhere else this page can reach. `kubectl proxy` would serve the
// custom resources -- they are in the same API group -- but the legacy UI
// ServiceAccount has no binding for them (D2 section 7.4 adds `get`/`list` for
// read-only summaries and NO create), and `ui/api.js`'s `WRITABLE_PLURALS` is
// deliberately unchanged. So in legacy mode every call below is refused HERE,
// with a sentence saying which API serves the flow, rather than sent and
// answered with a 403 the page would then have to narrate.
//
// WHO MAY START ONE. The three capability flags `destinations`,
// `topicDiscovery` and `preflight` are the domains' READ FLOORS: "this build
// serves the domain and you may read it". Whether the actor may also START,
// CANCEL or MANAGE is the role table, which `/session` publishes per grant in
// `namespaces[].roles` -- and D2 W12 deliberately did not split the three
// flags into read/start pairs, because `CAPABILITY_FLAGS` is frozen at
// nineteen entries and pinned against the schema's own `required` set. So the
// read is gated by the FLAG and the write by [`mayOperate`], which reads the
// roles.

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
 *  AN EMPTY ROLE LIST IS A MODE, NOT A REFUSAL. Legacy mode has no product
 *  roles at all -- the API server's own RBAC is the whole authorisation story
 *  there -- and localAdmin mode has none either: it is one administrator on a
 *  loopback listener, and `authz.rs` answers every action `true` for it. In
 *  both the server is the gate and the control is offered. What this refuses
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
// rather than a 422 in front of an operator. It is the same rule the read side
// has had since PLAT-18.1, applied to the half that was never checked.
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

// The refusal every D2 method answers in legacy mode.
function consoleOnly(what) {
  return noRoute(
    what + " is served by the Logweir product API and not by the Kubernetes API this page is " +
      "talking to. Open the console (`logweir-api`) to use it, or drive the custom resources " +
      "with kubectl; this page will not pretend to a route it cannot reach.",
  );
}

const legacyChecks = Object.freeze({
  destinations() { return Promise.reject(consoleOnly("Saved destinations")); },
  destination() { return Promise.reject(consoleOnly("Saved destinations")); },
  createDestination() { return Promise.reject(consoleOnly("Creating a destination")); },
  updateDestinationAccess() { return Promise.reject(consoleOnly("Rotating destination access")); },
  testDestination() { return Promise.reject(consoleOnly("Testing destination access")); },
  destinationUsage() { return Promise.reject(consoleOnly("What uses a destination")); },
  destinationFromLegacy() { return Promise.reject(consoleOnly("Adopting a legacy archive")); },
  latestDiscoveries() { return Promise.reject(consoleOnly("Topic discovery")); },
  startDiscovery() { return Promise.reject(consoleOnly("Topic discovery")); },
  discovery() { return Promise.reject(consoleOnly("Topic discovery")); },
  discoveryTopics() { return Promise.reject(consoleOnly("Topic discovery")); },
  cancelDiscovery() { return Promise.reject(consoleOnly("Cancelling a discovery")); },
  startPreflight() { return Promise.reject(consoleOnly("Operation readiness")); },
  preflight() { return Promise.reject(consoleOnly("Operation readiness")); },
  preflightDetails() { return Promise.reject(consoleOnly("Operation readiness")); },
  cancelPreflight() { return Promise.reject(consoleOnly("Cancelling a preflight")); },
  checkOperation() { return Promise.reject(consoleOnly("A check's status")); },
});

// Every D2 read returns the DECODED DTO, not a projection into a custom
// resource. The five older kinds are projected because their pages were
// written against `kubectl proxy`'s shape and both modes must render the same
// string; these three have no legacy page to agree with, so the DTO IS the
// shape -- and a projection would be a second vocabulary for one set of facts.
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
  async testDestination(ns, name, request) {
    requireOperator(ns, "destinations", "test destination access");
    const body = sending("destinations:test", request || {});
    const answer = await consoleAction(ns, "destinations:test", name, body, {
      // THE KEY IS A FUNCTION OF WHAT IS BEING TESTED AND OF WHICH ROLES, so a
      // double click is one Preflight and a test of a different role set is
      // another. A key that ignored the roles would answer the second request
      // with the first one's result.
      idempotencyKey: idempotencyKey("destinations:test", ns, name + "." + rolesTag(body.roles)),
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
  async destinationFromLegacy(ns, request) {
    requireOperator(ns, "destinations", "adopt a legacy archive as a destination");
    const answer = await consoleAction(
      ns, "destinations:from-legacy", null,
      sending("destinations:from-legacy", request),
      { idempotencyKey: idempotencyKey("destinations:from-legacy", ns, request.name), token: tokenNow() },
    );
    const decoded = decodeConsoleItem("destinations", answer);
    return { item: decoded.value.item, replayed: decoded.value.replayed === true };
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
  async startDiscovery(ns, connection, request) {
    requireOperator(ns, "topic-discoveries", "start a topic discovery");
    const body = sending("connections:topic-discoveries", request || {});
    const answer = await consoleAction(ns, "connections:topic-discoveries", connection, body, {
      idempotencyKey: idempotencyKey("topic-discoveries", ns, connection + "." + discoveryTag(body)),
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
// `attempt` IS THE CALLER'S "THIS IS A NEW ONE", and it is what the header of
// this section already anticipates: "A DELIBERATELY NEW operation is a new
// name, and a new name is a new key." A readiness check over an unchanged plan
// is the same question and passes none, so its key is unchanged and a retry
// replays. A connectivity test is the opposite: its whole subject is a broker
// that may answer differently a minute later, so the page that owns the
// control mints one token per accepted CLICK and passes it here. Without it
// "Test connection" would replay its first answer for ever -- which is the
// re-read this control exists to stop being.
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

// A 409 from the product API is the same event the legacy mode reports as
// `AlreadyExists` with a different spec: the name this operation would take is
// taken by something else. It is reported as a conflict so a form says so and
// keeps what was typed, instead of reading as a generic refusal.
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
    // REFUSED, NOT DROPPED (PLAT-07.2). `CreateConnectionRequest` declares
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
          "without changing which entry of the Secret the controller projects. Create it with " +
          "kubectl, or use the legacy direct mode.",
      );
    }
    if (spec.auth.tlsCa !== undefined && spec.auth.tlsCa !== null) {
      throw noRoute(
        "the product API's connection create has no field for spec.auth.tlsCa " +
          "(saved-connection contract v1), so this connection cannot be created through it " +
          "without dropping the private CA it named. Create it with kubectl, or use the legacy " +
          "direct mode.",
      );
    }
    const auth = { mode: spec.auth.mode, tls: spec.auth.tls === true };
    if (typeof spec.auth.username === "string" && spec.auth.username.length > 0) {
      auth.username = spec.auth.username;
    }
    if (((spec.auth.secretRef) || {}).name) {
      auth.credentialRef = { name: spec.auth.secretRef.name };
    }
    const body = { role: spec.role, bootstrapServers: spec.bootstrapServers.slice(), auth: auth };
    if (typeof spec.markerTopic === "string" && spec.markerTopic.length > 0) {
      body.markerTopic = spec.markerTopic;
    }
    return body;
  }
  if (plural === "backupschedules") {
    const body = {
      schedule: spec.schedule,
      sourceRef: { name: spec.sourceRef.name },
      topics: spec.topics.slice(),
      archive: requestArchive(spec.archive),
      suspended: spec.suspend === true,
    };
    if (spec.concurrencyPolicy !== undefined) {
      body.concurrencyPolicy = spec.concurrencyPolicy;
    }
    if (spec.retention !== undefined && spec.retention !== null) {
      body.retention = spec.retention;
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
    return {
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
      const decoded = decodeOperation(
        await consoleOperation(ns, plural === "backups" ? "backup" : "restore", name, options),
      );
      return mergeOperation(object, decoded.value.item);
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
      String(read) + " as if they were all of them. Read them with kubectl, or narrow the " +
      "namespace.",
  );
  error.kind = "refused";
  error.status = 0;
  error.reason = "ListTooLarge";
  return error;
}

// --------------------------------------------------------------- the facade

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

    // D1 W7 (PLAT-04.2, PLAT-05.1, PLAT-06.2): the draft cadence preview, the
    // future-policy replace and "Back up now". They go through `dispatch` and
    // not `dispatchChecks` because they are the SAME five kinds this facade
    // has always served -- a schedule and a Backup -- rather than a domain of
    // their own; the legacy half answers the two console-only ones by name.
    previewCadence(query, options) {
      return dispatch((api) => api.previewCadence(query, options));
    },
    editSchedulePolicy(ns, name, body) {
      return dispatch((api) => api.editSchedulePolicy(ns, name, body));
    },
    runBackupNow(ns, body, key) {
      return dispatch((api) => api.runBackupNow(ns, body, key));
    },

    // D2 (PLAT-08, PLAT-09.1, PLAT-03): saved destinations, bounded topic
    // discovery and operation readiness. Console mode only; the legacy half
    // answers each of these with a named refusal rather than a 403 from a
    // route the page cannot reach.
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
    testDestination(ns, name, request) {
      return dispatchChecks((api) => api.testDestination(ns, name, request));
    },
    destinationUsage(ns, name, options) {
      return dispatchChecks((api) => api.destinationUsage(ns, name, options));
    },
    destinationFromLegacy(ns, request) {
      return dispatchChecks((api) => api.destinationFromLegacy(ns, request));
    },
    latestDiscoveries(ns, connection, options) {
      return dispatchChecks((api) => api.latestDiscoveries(ns, connection, options));
    },
    startDiscovery(ns, connection, request) {
      return dispatchChecks((api) => api.startDiscovery(ns, connection, request));
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
      return Promise.resolve(call(decided.mode === CONSOLE ? consoleApi : legacyApi));
    } catch (refused) {
      return Promise.reject(refused);
    }
  }
  return ensure().then((record) => call(record.mode === CONSOLE ? consoleApi : legacyApi));
}

// The D2 half dispatches exactly as the five older kinds do, over its own pair
// of implementations. A separate pair and not five more methods on the
// existing ones: the older kinds are PROJECTED into custom resources so both
// modes render one string, and these three have no legacy page to agree with.
function dispatchChecks(call) {
  if (decided !== null) {
    try {
      return Promise.resolve(call(decided.mode === CONSOLE ? consoleChecks : legacyChecks));
    } catch (refused) {
      return Promise.reject(refused);
    }
  }
  return ensure().then((record) => call(record.mode === CONSOLE ? consoleChecks : legacyChecks));
}

/** The two implementations, by name, for the suite and for a reader who wants
 *  to see one without the other. A page never picks: `apiClient` does. */
export const MODES = Object.freeze({ legacy: legacyApi, console: consoleApi });

/** The D2 half of the same pair. */
export const CHECK_MODES = Object.freeze({ legacy: legacyChecks, console: consoleChecks });
