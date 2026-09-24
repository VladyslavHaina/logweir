// contract.js -- the typed contract: what a response is allowed to be, in both
// modes, and what happens when it is not.
//
// WHY A DECODER AND NOT A CAST. Before PLAT-18.1 every page read a response by
// reaching into it -- `object.status.evidence.verification.result` -- and a
// backend that renamed, moved or dropped that field produced an empty cell.
// An empty cell is what an absent OPTIONAL field also looks like, so the page
// could not tell "the controller did not record this" from "the contract
// changed under us". That is the silent field loss PLAT-18.1's acceptance
// names, and the only way to stop it is to state, once, which fields are
// REQUIRED, and to fail loudly when one of them is not there.
//
// THE THREE RULES EVERY DECODER HERE OBEYS.
//
//   1. A REQUIRED FIELD THAT IS ABSENT OR OF THE WRONG TYPE IS A CONTRACT
//      FAILURE. [`contractFailure`] builds an Error the pages already know how
//      to render (`status`, `reason`, `message`, exactly as `render.js`'s
//      `errorBox` reads them), naming the DTO and the JSON path. It is never
//      swallowed and never turned into a default value.
//   2. AN UNKNOWN FIELD IS TOLERATED AND RECORDED. D0's compatibility rule is
//      "unknown newer fields are tolerated on responses by older clients", so
//      an unknown key never fails a read -- but it is collected in `unknown`,
//      so a test and a reviewer can see exactly which fields this client is
//      ignoring rather than discovering them from a screenshot.
//   3. A DECODER RETURNS A VALUE, NOT A PROMISE, AND TOUCHES NO PLATFORM API.
//      No fetch, no clock, no storage, no DOM. Every decoder in this file is a
//      pure function, which is what lets `ui/tests/contract.spec.js` drive all
//      of them from checked-in fixtures under `node --test`.
//
// TWO MODES, ONE FILE. The legacy decoders (`decodeKafkaCluster` and its five
// siblings) validate a Kubernetes custom resource AND RETURN IT UNCHANGED:
// nothing is copied out, so nothing can be dropped on the way through. The
// console decoders (`decodeConnection` and its siblings) read the product API's
// DTOs, whose field names and shapes are the ones
// `schemas/logweir-api-v1.openapi.json` publishes. `ui/tests/contract.spec.js`
// reads that schema file and asserts that every required field named here is
// required there, so the two cannot drift.
//
// THIS MODULE ISSUES NO REQUEST. `api.js` remains the only module in the tree
// that does.

/** @typedef {{value: *, unknown: string[]}} Decoded
 *  A decoded value and the JSON paths of every field the contract does not
 *  name. `unknown` is information, never an error. */

/** @typedef {{field: string, code: string, message: string}} FieldError
 *  One field-level failure, in the product API's own shape. `field` is a JSON
 *  path; `ui/validate.js` maps it onto a form input. */

/** @typedef {{type: string, title: string, status: number, code: string,
 *             detail: string, requestId: string, retryable: boolean,
 *             errors: FieldError[]}} Problem
 *  An `application/problem+json` document. `code` is the stable machine
 *  identifier clients branch on; `detail` is prose and is never branched on. */

/** @typedef {{approvalsRead: boolean, approvalPacketRead: boolean,
 *             approvalSubmit: boolean, backupsRead: boolean,
 *             connectionCreate: boolean, connectionTest: boolean,
 *             connectionsRead: boolean, credentialWrite: boolean,
 *             destinations: boolean, manualBackupCreate: boolean,
 *             operationEvents: boolean, operationsRead: boolean,
 *             preflight: boolean, restoreCreate: boolean,
 *             restoresRead: boolean, scheduleCreate: boolean,
 *             scheduleSetSuspension: boolean, schedulesRead: boolean,
 *             topicDiscovery: boolean}} Capabilities */

/** @typedef {{name: string, capabilities: Capabilities, roles: string[]}} NamespaceGrant */

/** @typedef {{actor: {id: string, issuer: string, subject: string,
 *                     displayName: string},
 *             authenticationMode: string, bindingRevision: string,
 *             capabilities: Capabilities,
 *             namespaces: NamespaceGrant[], csrfToken: (string|null),
 *             expiresAt: (string|null), requestId: string}} Session */

/** @typedef {{limit: number, nextCursor: (string|null),
 *             snapshot: (string|null)}} Page */

// ===========================================================================
// the failure
// ===========================================================================

/** The reason every contract failure carries, so a page can tell one from an
 *  API refusal without reading prose. */
export const CONTRACT_REASON = "ContractViolation";

/** An Error for a response that is not what the contract says it is.
 *
 *  It carries the three fields `render.js`'s `errorBox` reads -- `status`,
 *  `reason`, `message` -- so every existing page renders it with no change,
 *  and `kind: "contract"` so `lifecycle.js`'s `failureKind` reports it as
 *  itself rather than guessing from a status code. `status` is 0 because no
 *  HTTP status describes "the body was not the shape it promised": the request
 *  succeeded and the answer was wrong.
 *
 *  `detail` names the DTO and the JSON path, in that order, because those two
 *  are what a reader needs to go and look at the schema. */
export function contractFailure(dto, path, said) {
  const where = path.length > 0 ? dto + "." + path : dto;
  const error = new Error(
    "the response does not match the contract at " + where + ": " + said +
      ". This page refuses to render a partial object rather than show an empty " +
      "field that cannot be told from an absent one.",
  );
  error.kind = "contract";
  error.status = 0;
  error.reason = CONTRACT_REASON;
  error.contract = { dto: dto, path: path, detail: said };
  return error;
}

/** True for an Error [`contractFailure`] made. */
export function isContractFailure(error) {
  return error !== null && typeof error === "object" && error.kind === "contract";
}

// ===========================================================================
// the kit
// ===========================================================================

function typeName(value) {
  if (value === null) {
    return "null";
  }
  if (Array.isArray(value)) {
    return "an array";
  }
  return "a " + typeof value;
}

function join(path, key) {
  return path.length === 0 ? key : path + "." + key;
}

/** A scalar decoder: `{read(value, ctx, dto, path)}`. Each returns the value
 *  or raises a contract failure naming the path. */
function scalar(what, test) {
  return {
    what: what,
    read(value, ctx, dto, path) {
      if (!test(value)) {
        throw contractFailure(dto, path, "expected " + what + ", got " + typeName(value));
      }
      return value;
    },
  };
}

/** A string. */
export const str = scalar("a string", (v) => typeof v === "string");
/** A boolean. */
export const bool = scalar("a boolean", (v) => typeof v === "boolean");
/** A whole number. */
export const int = scalar(
  "a whole number",
  (v) => typeof v === "number" && isFinite(v) && Math.floor(v) === v,
);
/** Any JSON value, taken as it arrived. For a field the contract declares
 *  OPAQUE -- the plan bytes, an approval document -- where reading INTO it
 *  would be exactly the round trip `plan.js` exists to prevent. */
export const opaque = { what: "any value", read: (value) => value };

/** One of a closed set of strings. An unrecognised member is a contract
 *  failure and never a silently passed-through string: the set is the whole
 *  point of an enum. */
export function oneOf(members) {
  const frozen = Object.freeze(members.slice());
  return {
    what: "one of " + frozen.join(", "),
    members: frozen,
    read(value, ctx, dto, path) {
      if (typeof value !== "string" || frozen.indexOf(value) === -1) {
        throw contractFailure(
          dto,
          path,
          "expected one of " + frozen.join(", ") + ", got " + JSON.stringify(value),
        );
      }
      return value;
    },
  };
}

/** An array whose every member is decoded by `member`. */
export function listOf(member) {
  return {
    what: "an array of " + member.what,
    read(value, ctx, dto, path) {
      if (!Array.isArray(value)) {
        throw contractFailure(dto, path, "expected an array, got " + typeName(value));
      }
      return value.map((item, i) => member.read(item, ctx, dto, path + "[" + String(i) + "]"));
    },
  };
}

/** A nested object decoded by `shape` (see [`shapeOf`]).
 *
 *  THE NESTED SHAPE'S OWN NAME BECOMES THE DTO in any failure it raises, while
 *  the PATH stays rooted at the response: a reader gets "Connection at
 *  item.reachability", which names both the schema to look at and where in the
 *  body to find it. */
export function objectOf(shape) {
  return {
    what: "an object",
    read(value, ctx, dto, path) {
      return readShape(shape, value, ctx, shape.name, path);
    },
  };
}

/** The same, for a shape declared LATER in this file.
 *
 *  DECLARATION ORDER IS A REAL CONSTRAINT HERE and this is the one escape
 *  from it. `objectOf(SHAPE)` reads `SHAPE` while the module is evaluating, so
 *  a DTO can only carry a shape defined above it; `Connection.lastTest` is a
 *  preflight view, and the preflight vocabulary is 600 lines below the
 *  connection's. The alternatives were to hoist an exported vocabulary out of
 *  the section it documents, or to spell the state list twice -- and a
 *  vocabulary spelled twice is the defect every closed list in this file
 *  exists to prevent. The thunk is resolved when a DOCUMENT is read, which is
 *  long after every declaration has run.
 *
 *  @param {() => object} later a function returning the shape */
export function objectOfLater(later) {
  return {
    what: "an object",
    read(value, ctx, dto, path) {
      const shape = later();
      return readShape(shape, value, ctx, shape.name, path);
    },
  };
}

/** Declares a DTO: `required` fields whose absence is a contract failure, and
 *  `optional` fields whose absence is simply absence. A `null` in an optional
 *  field is absence too -- the product API spells "no value" that way, and a
 *  page reading `undefined` and a page reading `null` must not differ.
 *
 *  Every field NOT named in either bag is tolerated and recorded. */
export function shapeOf(name, required, optional) {
  return Object.freeze({
    name: name,
    required: Object.freeze(Object.assign({}, required)),
    optional: Object.freeze(Object.assign({}, optional || {})),
  });
}

function readShape(shape, value, ctx, dto, path) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw contractFailure(dto, path, "expected an object, got " + typeName(value));
  }
  // NO PROTOTYPE: the result is read by field name and a field name is data.
  const out = Object.create(null);
  for (const key of Object.keys(shape.required)) {
    if (!Object.prototype.hasOwnProperty.call(value, key) || value[key] === undefined) {
      throw contractFailure(
        dto,
        join(path, key),
        "the field is required by the contract and is absent",
      );
    }
    out[key] = shape.required[key].read(value[key], ctx, dto, join(path, key));
  }
  for (const key of Object.keys(shape.optional)) {
    const present = Object.prototype.hasOwnProperty.call(value, key) &&
      value[key] !== undefined && value[key] !== null;
    out[key] = present
      ? shape.optional[key].read(value[key], ctx, dto, join(path, key))
      : null;
  }
  for (const key of Object.keys(value)) {
    if (
      !Object.prototype.hasOwnProperty.call(shape.required, key) &&
      !Object.prototype.hasOwnProperty.call(shape.optional, key)
    ) {
      ctx.unknown.push(join(path, key));
    }
  }
  return out;
}

/** Decodes `value` against `shape`, returning [`Decoded`]. The only entry
 *  point: every decoder below is `decodeWith(SHAPE, value)`. */
export function decodeWith(shape, value) {
  const ctx = { unknown: [] };
  const decoded = readShape(shape, value, ctx, shape.name, "");
  return { value: decoded, unknown: ctx.unknown };
}

/** Decodes an envelope `{items: [...], page, requestId}`. The page and the
 *  request id are part of the contract; the items are decoded one by one so a
 *  single malformed member names its own index. */
export function decodeListWith(shape, itemShape, value) {
  const ctx = { unknown: [] };
  const envelope = readShape(shape, value, ctx, shape.name, "");
  const items = [];
  const raw = (value || {}).items;
  for (let i = 0; i < raw.length; i += 1) {
    items.push(readShape(itemShape, raw[i], ctx, itemShape.name, "items[" + String(i) + "]"));
  }
  envelope.items = items;
  return { value: envelope, unknown: ctx.unknown };
}

// ===========================================================================
// the console DTOs -- schemas/logweir-api-v1.openapi.json
// ===========================================================================

const NAME_REF = shapeOf("NameRef", { name: str });

export const CAPABILITY_FLAGS = Object.freeze([
  "approvalPacketRead", "approvalSubmit", "approvalsRead", "backupsRead",
  "catalogConnect", "catalogWindowQuery", "catalogs", "connectionCreate",
  "connectionTest", "connectionsRead", "credentialWrite", "destinations",
  "manualBackupCreate", "operationEvents", "operationsRead", "preflight",
  "protection", "rehearsals", "restoreCreate", "restoresRead", "retention",
  "scheduleCreate", "scheduleSetSuspension", "schedulesRead", "topicDiscovery",
  "trustAdministration", "trustPoliciesRead",
]);

const CAPABILITIES = shapeOf(
  "Capabilities",
  CAPABILITY_FLAGS.reduce((bag, flag) => {
    bag[flag] = bool;
    return bag;
  }, {}),
);

const ACTOR = shapeOf("ActorView", {
  id: str, issuer: str, subject: str, displayName: str,
});

/** The four PRODUCT roles (PLAT-17.2). They are Logweir's own: they are not
 *  Kubernetes roles, they are not granted by cluster RBAC, and holding one
 *  says nothing about what the console ServiceAccount may do. The set is
 *  empty in localAdmin mode, which has no roles -- so an empty list is a
 *  mode, not a missing field, and the field itself is required. */
export const ROLES = Object.freeze(["viewer", "operator", "approver", "administrator"]);

const GRANT = shapeOf("NamespaceGrant", {
  name: str,
  capabilities: objectOf(CAPABILITIES),
  roles: listOf(oneOf(ROLES)),
});

const SESSION = shapeOf(
  "SessionResponse",
  {
    actor: objectOf(ACTOR),
    authenticationMode: str,
    // THE REVISION OF THE BINDING TABLE THAT PRODUCED THESE GRANTS. It is in
    // every audit line the product API writes, so a decision can be tied to
    // the configuration that made it; this page carries it so a reader can
    // name that configuration too. Empty in localAdmin mode.
    bindingRevision: str,
    capabilities: objectOf(CAPABILITIES),
    namespaces: listOf(objectOf(GRANT)),
    requestId: str,
  },
  { csrfToken: str, expiresAt: str },
);

const FIELD_ERROR = shapeOf("FieldError", { field: str, code: str, message: str });

const PROBLEM = shapeOf(
  "Problem",
  {
    type: str, title: str, status: int, code: str, detail: str,
    requestId: str, retryable: bool,
  },
  { errors: listOf(objectOf(FIELD_ERROR)) },
);

const PAGE = shapeOf("Page", { limit: int }, { nextCursor: str, snapshot: str });

const CONDITION = shapeOf(
  "ConditionView",
  { type: str, status: str },
  { reason: str, message: str, lastTransitionTime: str },
);

const ARCHIVE = shapeOf("ArchiveView", { url: str }, { credentialRef: objectOf(NAME_REF) });

/** The two authentication modes a connection may declare. */
export const CONNECTION_AUTH_MODES = Object.freeze(["plaintext", "scramSha512"]);

/** The two roles a connection may carry. */
export const CONNECTION_ROLES = Object.freeze(["source", "target"]);

/** The two concurrency policies a schedule may carry. */
export const CONCURRENCY_POLICIES = Object.freeze(["Forbid", "Allow"]);

/** The two target modes a restore may ask for. Byte for byte the runner's
 *  `TargetMode` and the CRD's own enum -- see `ui/plan.js`. */
export const RESTORE_MODES = Object.freeze(["scratch", "newTopic"]);

const CONNECTION_AUTH = shapeOf(
  "ConnectionAuthView",
  { mode: str, tls: bool },
  { username: str, credentialRef: objectOf(NAME_REF) },
);

/** The reachability verdicts `crates/logweir-api/src/projection.rs` emits. */
export const REACHABILITY_STATES = Object.freeze(["reachable", "unreachable", "unknown"]);

const REACHABILITY = shapeOf(
  "ReachabilityView",
  { state: oneOf(REACHABILITY_STATES) },
  { clusterId: str, observedAt: str, reason: str },
);

const CONNECTION = shapeOf(
  "Connection",
  {
    name: str, namespace: str, uid: str, resourceVersion: str,
    role: str, bootstrapServers: listOf(str),
    auth: objectOf(CONNECTION_AUTH), reachability: objectOf(REACHABILITY),
  },
  // `lastTest` IS A DIFFERENT FACT FROM `reachability` and the page renders
  // them apart: the reachability block is the controller's own probe on its
  // own cadence, and this is a connectivity check somebody asked for, with its
  // own instant and its own staleness. It is absent on a list and on a create
  // -- the product API computes it only on the detail read -- so it is
  // optional here, and `objectOfLater` is why a connection may carry a
  // preflight view at all (see its own note).
  { createdAt: str, markerTopic: str, lastTest: objectOfLater(() => LAST_TEST) },
);

const RETENTION = shapeOf("RetentionView", {}, { keepLast: int, keepDays: int });

const REMOVABLE_SET = shapeOf(
  "RemovableSetView",
  { backupId: str, newestRecordAt: str, reason: str },
  { days: int, rank: int },
);

const RETENTION_REPORT = shapeOf(
  "RetentionReportView",
  {
    setsKept: listOf(str),
    setsThatWouldBeRemoved: listOf(objectOf(REMOVABLE_SET)),
    removalCommands: listOf(str),
    mcRemovalCommands: listOf(str),
    skippedManifests: int,
    truncated: bool,
  },
  { evaluatedAt: str, keepLast: int, keepDays: int, note: str },
);

// ---------------------------------------------- D1 W7: the cadence policy
//
// THE FOUR CLOSED SETS BELOW ARE THE CONTROLLER'S OWN SPELLINGS. `nextRuns[]`
// on a saved schedule and `runs[]` on a draft preview are ONE shape
// (`NextRunView`), so a form renders a policy that is saved and a policy that
// is only typed through one branch; and the three `adjustment` words are the
// PascalCase the CRD writes, so a preview and a status cannot disagree about
// which occurrence of a repeated local hour a row is.

/** The DST marker on one firing. A word this build does not know is a contract
 *  failure and never a blank cell: the whole value of the marker is that the
 *  reader learns WHICH instant a row is. */
export const CADENCE_ADJUSTMENTS = Object.freeze([
  "NonexistentLocalTimeShifted", "RepeatedLocalTimeFirst", "RepeatedLocalTimeSecond",
]);

/** Whether a slot past its starting deadline may still run. ABSENT means
 *  `None`, which is what every schedule written before PLAT-04.2 carries. */
export const CATCH_UP_POLICIES = Object.freeze(["None", "Latest"]);

/** Which kind of run a `Backup` is. PascalCase, the CRD's own. */
export const TRIGGER_KINDS = Object.freeze(["Scheduled", "CatchUp", "Retry", "Manual"]);

/** The readiness verdict a person clicked past, as the API records it. */
export const ACKNOWLEDGED_READINESS = Object.freeze(["notReady", "unknown"]);

const NEXT_RUN = shapeOf(
  "NextRunView",
  { at: str, localTime: str },
  { adjustment: oneOf(CADENCE_ADJUSTMENTS) },
);

const ACTIVE_RUN = shapeOf("ActiveRunView", { name: str, kind: str, attempt: int });

// `evaluatedAt` IS WHEN THE STATUS LAST MOVED, NOT A LIVENESS PROBE. The
// controller writes nothing when nothing changed, so this instant standing
// still means "nothing has changed" and a console must never compare it with
// the requeue interval. The staleness signal is `nextRuns[0].at` in the past,
// and `ui/render.js`'s `nextRunsPanel` is the one place that reads it.
const SCHEDULE_POLICY = shapeOf(
  "SchedulePolicyView",
  {
    generation: int, runPolicySha256: str, timeZone: str, tzdb: str,
    effectiveSince: str, evaluatedAt: str,
  },
);

const SCHEDULE_STATUS = shapeOf(
  "ScheduleStatusView",
  {},
  {
    lastFireTime: str, nextFireTime: str, lastMissedSlot: str,
    activeBackup: str, pendingBackup: str,
    ready: objectOf(CONDITION), retentionReport: objectOf(RETENTION_REPORT),
    // D1 W7. `activeRuns` ABSENT means "not yet computed", never "none are
    // running", and `ui/pages/schedules.js` renders those two differently.
    observedGeneration: int, policy: objectOf(SCHEDULE_POLICY),
    nextRuns: listOf(objectOf(NEXT_RUN)), activeRuns: listOf(objectOf(ACTIVE_RUN)),
  },
);

/** The retry policy a schedule carries. ABSENT is `maxRetries: 0`, which is
 *  "no retry" and is what every schedule written before PLAT-04.2 means. */
const RETRY_POLICY = shapeOf("RetryPolicy", { maxRetries: int }, { delaySeconds: int });

// A PRESET IS NEVER STORED. `spec.schedule` is the single source of truth and
// this is the catalogue entry an expression IS, so a form can round-trip one.
// It is declared as one shape rather than as five, because the published
// document spells it as a `oneOf` of five objects sharing a `kind` tag: the
// drift arm skips the required-set comparison for a `oneOf`, and what this
// client needs from it is the tag and the parameters of whichever branch
// arrived. The PARAMETERS are checked against the catalogue by
// `ui/pages/schedules.js`'s own table, which `ui/tests/d1.spec.js` compares
// with `ui/tests/fixtures/cadence-presets.json` -- the file Rust generates.
const CADENCE_PRESET = shapeOf(
  "CadencePreset",
  { kind: str },
  { minute: int, hour: int, dayOfWeek: int, dayOfMonth: int, n: int },
);

/** What a dynamic run does when discovery cannot prove it saw everything.
 *  Required on the block, with NO default, for the reason D1 gives: both
 *  possible defaults are wrong in a way the operator would not notice. */
export const INCOMPLETE_DISCOVERY_POLICIES = Object.freeze(["Refuse", "BackUpVisibleTopics"]);

const TOPIC_EXCLUSIONS = shapeOf(
  "TopicExclusions",
  {},
  { topics: listOf(str), prefixes: listOf(str) },
);

const ALL_USER_TOPICS = shapeOf(
  "AllUserTopics",
  { incompleteDiscovery: oneOf(INCOMPLETE_DISCOVERY_POLICIES) },
  { exclude: objectOf(TOPIC_EXCLUSIONS) },
);

// `destinationRef` AND `allUserTopics` LANDED WITH D1 W6 (PLAT-06.2, PLAT-09.2)
// AND ARE DECODED HERE, which is the whole of what this client does with them:
// a schedule may now NAME a saved destination, and it may now say that its
// selection is dynamic. Both are OPTIONAL and both are absent on every schedule
// written before that change, so absent keeps meaning what it meant.
//
// AND D1 W7 DECLARES THE REST, BECAUSE D1 W7 IS THE FORM THAT OWNS THEM.
// `preset`, `timeZone`, `retry`, `catchUpPolicy`, `startingDeadlineSeconds`,
// `activeDeadlineSeconds` and `generation` were left in the decoder's
// `unknown` list by D1 W6 -- tolerated, recorded and visibly unrendered --
// with the constraint that they are declared by the task that RENDERS them,
// never by one that would leave a declared field with nothing on screen. That
// task is this one: `ui/pages/schedules.js`'s policy form reads every one of
// them, and `renderScheduleRevision` reads `generation`.
//
// `generation` STAYS OPTIONAL, AND THAT IS NOT THIS SIDE'S CHOICE. D1 W6
// records that the API always emits it and left it out of the schema's
// `required` set only so that tightening it would be one cross-side commit.
// The drift arm compares this client's required set with the SCHEMA's, so
// moving it here alone turns `contract.spec.js` red: the tightening needs
// `crates/logweir-api/src/contract.rs` in the same commit, which is Rust
// product code this task does not own. The page therefore treats an absent
// generation as "revision not recorded" -- the same words a pre-PLAT-05.1 run
// gets -- and never as revision 0.
const SCHEDULE = shapeOf(
  "Schedule",
  {
    name: str, namespace: str, uid: str, resourceVersion: str,
    schedule: str, sourceRef: objectOf(NAME_REF), topics: listOf(str),
    archive: objectOf(ARCHIVE), suspended: bool,
    concurrencyPolicy: str, status: objectOf(SCHEDULE_STATUS),
  },
  {
    createdAt: str, retention: objectOf(RETENTION),
    destinationRef: objectOf(NAME_REF), allUserTopics: objectOf(ALL_USER_TOPICS),
    generation: int, preset: objectOf(CADENCE_PRESET), timeZone: str,
    startingDeadlineSeconds: int, catchUpPolicy: oneOf(CATCH_UP_POLICIES),
    retry: objectOf(RETRY_POLICY), activeDeadlineSeconds: int,
  },
);

/** The normalized operation states `crates/logweir-api/src/status.rs` emits. */
export const OPERATION_STATES = Object.freeze([
  "pending", "queued", "preparing", "running", "verifying",
  "succeeded", "failed", "refused", "cancelled", "unknown",
]);

/** The verification verdicts the same module emits. */
export const VERIFICATION_STATES = Object.freeze([
  "pending", "valid", "invalid", "notAttempted", "noEvidence", "unknown",
]);

const OPERATION_SUMMARY = shapeOf(
  "OperationSummary",
  {
    state: oneOf(OPERATION_STATES),
    terminal: bool,
    verificationState: oneOf(VERIFICATION_STATES),
    verifiedSuccess: bool,
  },
  { stateReason: str },
);

const WINDOW_COVERED = shapeOf("WindowCoveredView", { fromMs: int, toMs: int });

const OBSERVED_AUTH = shapeOf("ObservedAuthView", {}, { mode: str, username: str });

// WHAT KIND OF RUN THIS IS, AND WHICH REVISION IT FROZE (D1 W7).
//
// BOTH ARE OPTIONAL AND BOTH ARE ABSENT ON A PRE-PLAT-05.1 RUN, and the page
// renders that absence as "revision not recorded" rather than as `Manual`,
// generation 0 or an empty digest. `triggeredBy` -- the CRD's older, coarser
// `manual | schedule` string -- stays required and stays rendered: it is what
// a run created before `trigger` existed carries, and the two are shown as the
// two facts they are rather than one reconciled guess.
const TRIGGER = shapeOf(
  "TriggerView",
  { kind: oneOf(TRIGGER_KINDS), attempt: int },
  { retryOf: objectOf(NAME_REF), timeZone: str },
);

const SCHEDULE_REF = shapeOf(
  "ScheduleRefView",
  { name: str },
  { uid: str, generation: int, runPolicySha256: str },
);

const BACKUP_DESTINATION_REF = shapeOf(
  "BackupDestinationRefView",
  { name: str },
  { uid: str },
);

// P10: the ceiling a queued manual run waits behind, published only while the
// run is queued.
const RUN_QUEUE = shapeOf("RunQueueView", { limit: int });

const BACKUP = shapeOf(
  "Backup",
  {
    name: str, namespace: str, uid: str, resourceVersion: str,
    sourceRef: objectOf(NAME_REF), topics: listOf(str), archive: objectOf(ARCHIVE),
    triggeredBy: str, deadlineSeconds: int, operation: objectOf(OPERATION_SUMMARY),
  },
  {
    createdAt: str, schedule: str, slot: str, backupId: str, manifestKey: str,
    records: int, windowCovered: objectOf(WINDOW_COVERED),
    observedAuth: objectOf(OBSERVED_AUTH),
    trigger: objectOf(TRIGGER), scheduleRef: objectOf(SCHEDULE_REF),
    destinationRef: objectOf(BACKUP_DESTINATION_REF), locationDigest: str,
    queue: objectOf(RUN_QUEUE),
  },
);

const RESTORE_TARGET = shapeOf(
  "RestoreTargetView",
  { clusterRef: objectOf(NAME_REF), mode: str, topicPrefix: str },
);

const RESTORE = shapeOf(
  "Restore",
  {
    name: str, namespace: str, uid: str, resourceVersion: str,
    planHash: str, planBytesLength: int, approvalRef: objectOf(NAME_REF),
    sourceArchive: objectOf(ARCHIVE), backupSetRef: str, pointInTime: str,
    target: objectOf(RESTORE_TARGET), deadlineSeconds: int,
    newTopics: listOf(str), operation: objectOf(OPERATION_SUMMARY),
  },
  {
    createdAt: str, planBytes: opaque,
    sourceDestinationRef: objectOf(NAME_REF), evidenceDestinationRef: objectOf(NAME_REF),
    queue: objectOf(RUN_QUEUE),
  },
);

const SUBJECT_REF = shapeOf("SubjectRefView", { kind: str, name: str });

const VERIFIED_SUBJECT = shapeOf(
  "VerifiedSubjectView",
  { kind: str, name: str, namespace: str, uid: str },
);

// PLAT-19.2: the policy, mode and console-attested requester a v2 verdict
// was made under. Present only while the Approval is verified.
const APPROVAL_AUTHORIZATION = shapeOf(
  "ApprovalProvenanceView",
  { mode: str, policyName: str, policyDigest: str, requester: str, confirmationKeyId: str },
);

const APPROVAL = shapeOf(
  "Approval",
  {
    name: str, namespace: str, uid: str, resourceVersion: str,
    subjectRef: objectOf(SUBJECT_REF), planHash: str,
    approvalBytesLength: int, sidecarBytesLength: int,
    conditions: listOf(objectOf(CONDITION)),
  },
  {
    createdAt: str, verified: bool, matchedKeyId: str, approver: str,
    ticket: str, selfAttestedRisk: bool, verifiedSubject: objectOf(VERIFIED_SUBJECT),
    authorization: objectOf(APPROVAL_AUTHORIZATION),
  },
);

// PLAT-19.2 / PLAT-12.1: where a submitted Restore goes next, decided by the
// namespace's frozen approval policy. `state` is `confirmed` (route to the
// operation) or `awaitingApproval` (route to its approval page); `mode` is
// `governed` or `ordinary`. Read as strings, and compared by the one function
// that routes on them, `ui/pages/restore-wizard.js`'s `frozenDecision`.
const RESTORE_AUTHORIZATION = shapeOf(
  "RestoreRoutingView",
  { mode: str, policy: str, legacy: bool, state: str, approvalName: str },
  { policyDigest: str, confirmationName: str, requester: str, expiresAt: str },
);

// PLAT-19.2: a namespace's effective approval policy.
const APPROVAL_POLICY = shapeOf(
  "ApprovalPolicyView",
  {
    namespace: str, name: str, mode: str, legacy: bool,
    requireDistinctPrincipal: bool, installationDigest: str,
    ordinaryConfirmationAvailable: bool, ticketRequired: bool,
  },
  { maxAgeSeconds: int, digest: str, confirmationKeyId: str },
);

// PLAT-19.2: a governed approver's countersigned sidecar.
const SUBMIT_APPROVAL_REQUEST = shapeOf(
  "SubmitApprovalRequest", { sidecarBytes: str }, { approvalBytes: str },
);

const APPROVAL_PACKET = shapeOf(
  "ApprovalPacket",
  {
    name: str, namespace: str, uid: str,
    subjectRef: objectOf(SUBJECT_REF), planHash: str,
    approvalBytes: opaque, sidecarBytes: opaque,
  },
);

const OPERATION_EVIDENCE = shapeOf(
  "OperationEvidence",
  {},
  {
    payloadKey: str, payloadSha256: str, sidecarKey: str,
    offsetReportKey: str, offsetReportSha256: str,
  },
);

/** The result verdicts the same module emits. */
export const RESULT_STATUSES = Object.freeze([
  "pending", "pass", "notPass", "refused", "error", "unknown",
]);

const OPERATION_RESULT = shapeOf(
  "OperationResult",
  { status: oneOf(RESULT_STATUSES) },
  { exitCode: int, exitReason: str, outcome: str, lastPhaseCompleted: int },
);

const OPERATION_VERIFICATION = shapeOf(
  "OperationVerification",
  { state: oneOf(VERIFICATION_STATES) },
  { payloadType: str, matchedKeyId: str, verifiedAt: str, detail: str },
);

/** The two operation kinds the product API's route accepts. */
export const OPERATION_KINDS = Object.freeze(["backup", "restore"]);

const OPERATION = shapeOf(
  "Operation",
  {
    kind: oneOf(OPERATION_KINDS),
    name: str, namespace: str, uid: str, resourceVersion: str,
    state: oneOf(OPERATION_STATES), terminal: bool,
    result: objectOf(OPERATION_RESULT),
    evidence: objectOf(OPERATION_EVIDENCE),
    verification: objectOf(OPERATION_VERIFICATION),
    verifiedSuccess: bool,
    conditions: listOf(objectOf(CONDITION)),
  },
  { createdAt: str, lastUpdatedAt: str, stateReason: str, message: str },
);

// A list envelope. `items` is declared opaque here and decoded member by
// member in `decodeListWith`, so a malformed member names its own index
// instead of failing the whole envelope at `items`.
function envelope(name) {
  return shapeOf(name, { items: listOf(opaque), page: objectOf(PAGE), requestId: str });
}

const CONNECTION_LIST = envelope("ConnectionList");
const SCHEDULE_LIST = envelope("ScheduleList");
const BACKUP_LIST = envelope("BackupList");
const RESTORE_LIST = envelope("RestoreList");
const APPROVAL_LIST = envelope("ApprovalList");

// A single-item response for a route that CREATES: `replayed` is how the
// product API says "this idempotency key had already made this object".
function item(name, itemShape) {
  return shapeOf(name, { item: objectOf(itemShape), requestId: str }, { replayed: bool });
}

// A single-item response for a route that only READS. It has no `replayed`,
// and declaring one here would have been this client tolerating a field the
// schema does not publish -- which the drift arm now refuses.
function readOnlyItem(name, itemShape) {
  return shapeOf(name, { item: objectOf(itemShape), requestId: str });
}

const CONNECTION_RESPONSE = item("ConnectionResponse", CONNECTION);
const SCHEDULE_RESPONSE = item("ScheduleResponse", SCHEDULE);
const BACKUP_RESPONSE = item("BackupResponse", BACKUP);
// Not `item(...)`: a create answers PLAT-19.2's `authorization` beside the item.
const RESTORE_RESPONSE = shapeOf(
  "RestoreResponse",
  { item: objectOf(RESTORE), requestId: str },
  { replayed: bool, authorization: objectOf(RESTORE_AUTHORIZATION) },
);
const APPROVAL_RESPONSE = item("ApprovalResponse", APPROVAL);
const APPROVAL_PACKET_RESPONSE = readOnlyItem("ApprovalPacketResponse", APPROVAL_PACKET);
const OPERATION_RESPONSE = readOnlyItem("OperationResponse", OPERATION);
const APPROVAL_POLICY_RESPONSE = readOnlyItem("ApprovalPolicyResponse", APPROVAL_POLICY);

// ------------------------------------------- D1 W7: the three W6 answers

/** A draft cadence, previewed. `schedule` is the CANONICAL expression -- what
 *  a preset compiled to, and therefore the string the form saves -- and
 *  `timeZone` is the EFFECTIVE zone, `UTC` when the request named none. A
 *  shorter list than `count` is a real answer and an empty one means "it does
 *  not fire again"; neither is an error, and `ui/render.js` says so. */
const CADENCE_PREVIEW_RESPONSE = shapeOf(
  "CadencePreviewResponse",
  {
    requestId: str, schedule: str, timeZone: str, tzdb: str, after: str,
    runs: listOf(objectOf(NEXT_RUN)),
  },
  { preset: objectOf(CADENCE_PRESET) },
);

/** The schedule a manual run was taken from, as the create route saw it.
 *  `suspended` and `activeRuns` are NOTICES here and not blocks: D1 section 8.3 is
 *  explicit that nothing about a schedule prevents a manual run. */
const SCHEDULE_CONTEXT = shapeOf(
  "ScheduleContextView",
  {
    name: str, uid: str, generation: int, suspended: bool,
    activeRuns: listOf(objectOf(ACTIVE_RUN)),
  },
  { runPolicySha256: str },
);

/** `POST .../backups`. `replayed` is REQUIRED here -- unlike the generic
 *  create envelope, where it is optional -- because this route's whole
 *  contract is that the second click is answered `200` with the first click's
 *  run, and a console that could not tell the two apart would report a second
 *  run that does not exist. */
const MANUAL_BACKUP_RESPONSE = shapeOf(
  "ManualBackupResponse",
  { requestId: str, replayed: bool, item: objectOf(BACKUP) },
  { schedule: objectOf(SCHEDULE_CONTEXT) },
);

/** The extension member `409 policy_changed` carries. The ONE extension
 *  member this API defines: `expectedGeneration` was not malformed, it named a
 *  superseded revision, and the current revision is what a console needs. */
const POLICY_CHANGED_DETAIL = shapeOf(
  "PolicyChangedDetail",
  { currentGeneration: int },
  { currentRunPolicySha256: str },
);


// ===========================================================================
// the console REQUEST DTOs -- the other half of the contract
// ===========================================================================
//
// WHY THESE ARE DECLARED AND NOT ONLY BUILT. `ui/client.js` hand-writes the
// body of every product-API create. Nothing compared those bodies with the
// published request schemas, so a field that became REQUIRED on a create route
// would have been a 422 in front of an operator rather than a red test -- the
// mirror image of the read-side failure this module exists to stop. They are
// declared here, `ui/tests/contract.spec.js` holds them to
// `schemas/logweir-api-v1.openapi.json` exactly as it holds the response
// shapes, and `ui/tests/client.spec.js` validates the body `requestBody`
// actually builds against them.
//
// THEY ARE NOT USED TO BUILD A REQUEST. A decoder is a reader; making it also
// a writer would put a second opinion about the plan bytes in this tree.

const ARCHIVE_REQUEST = shapeOf("ArchiveRequest", { url: str }, { credentialRef: objectOf(NAME_REF) });

const CONNECTION_AUTH_REQUEST = shapeOf(
  "ConnectionAuthRequest",
  { mode: oneOf(CONNECTION_AUTH_MODES), tls: bool },
  { username: str, credentialRef: objectOf(NAME_REF) },
);

const CREATE_CONNECTION_REQUEST = shapeOf(
  "CreateConnectionRequest",
  {
    role: oneOf(CONNECTION_ROLES),
    bootstrapServers: listOf(str),
    auth: objectOf(CONNECTION_AUTH_REQUEST),
  },
  { markerTopic: str },
);

const RETENTION_REQUEST = shapeOf("RetentionRequest", {}, { keepLast: int, keepDays: int });

/** `POST .../schedules`: THE WHOLE POLICY, as of PLAT-10.1.
 *
 *  IT CREATES WHAT THE EDIT ROUTE CAN EDIT, and until PLAT-10.1 it did not:
 *  five required fields against the edit route's thirteen, so a form could
 *  edit a schedule into a shape it had no way to create and every guided
 *  creation would have been a create followed by a repair. `archive` and
 *  `topics` left the required set to make room for the two spellings that
 *  replace them -- `destinationRef` and `allUserTopics` -- and the xor of each
 *  pair is the SERVER's rule, not a copy kept here: a body with both, or with
 *  neither, is `422 validation_failed` naming the field. This shape's job is
 *  only to notice a field that became required on the server and stayed
 *  optional here, which is the silent field loss PLAT-18.1 exists to stop. */
const CREATE_SCHEDULE_REQUEST = shapeOf(
  "CreateScheduleRequest",
  {
    schedule: str,
    sourceRef: objectOf(NAME_REF),
    suspended: bool,
  },
  {
    timeZone: str, topics: listOf(str), allUserTopics: objectOf(ALL_USER_TOPICS),
    archive: objectOf(ARCHIVE_REQUEST), destinationRef: objectOf(NAME_REF),
    concurrencyPolicy: oneOf(CONCURRENCY_POLICIES), startingDeadlineSeconds: int,
    catchUpPolicy: oneOf(CATCH_UP_POLICIES), retry: objectOf(RETRY_POLICY),
    activeDeadlineSeconds: int, retention: objectOf(RETENTION_REQUEST),
  },
);

const TOPIC_NAMING_REQUEST = shapeOf("TopicNamingRequest", { prefix: str });

const RESTORE_TARGET_REQUEST = shapeOf(
  "RestoreTargetRequest",
  {
    clusterRef: objectOf(NAME_REF),
    mode: oneOf(RESTORE_MODES),
    topicNaming: objectOf(TOPIC_NAMING_REQUEST),
  },
);

const CREATE_RESTORE_REQUEST = shapeOf(
  "CreateRestoreRequest",
  {
    planBytes: opaque,
    planHash: str,
    approvalRef: objectOf(NAME_REF),
    sourceArchive: objectOf(ARCHIVE_REQUEST),
    backupSetRef: str,
    pointInTime: str,
    target: objectOf(RESTORE_TARGET_REQUEST),
    deadlineSeconds: int,
  },
  {
    sourceDestinationRef: objectOf(NAME_REF),
    evidenceDestinationRef: objectOf(NAME_REF),
    // PLAT-19.2: the change ticket the console signs; required under a
    // Governed policy (D0).
    ticket: str,
  },
);

const SET_SUSPENSION_REQUEST = shapeOf(
  "SetSuspensionRequest",
  { suspended: bool, expectedResourceVersion: str },
);

// ------------------------------------------- D1 W7: the two write bodies

const TOPIC_SELECTION_REQUEST = shapeOf(
  "TopicSelectionRequest",
  {},
  { topics: listOf(str), allUserTopics: objectOf(ALL_USER_TOPICS) },
);

/** `PUT .../schedules/{name}`: THE WHOLE FUTURE POLICY, NOT A DIFF.
 *
 *  A FIELD OMITTED IS REMOVED, and that is the property the form is built
 *  around: every field of the policy is on screen, the body carries every one
 *  the form holds, and the page says so above the button. `expectedGeneration`
 *  is the precondition; `sourceRef` is on the DTO only to be refused, so this
 *  client never sends it. */
const UPDATE_SCHEDULE_POLICY_REQUEST = shapeOf(
  "UpdateSchedulePolicyRequest",
  {
    expectedGeneration: int, schedule: str, suspended: bool,
    topicSelection: objectOf(TOPIC_SELECTION_REQUEST),
  },
  {
    timeZone: str, archive: objectOf(ARCHIVE_REQUEST), destinationRef: objectOf(NAME_REF),
    concurrencyPolicy: str, startingDeadlineSeconds: int,
    catchUpPolicy: oneOf(CATCH_UP_POLICIES), retry: objectOf(RETRY_POLICY),
    activeDeadlineSeconds: int, retention: objectOf(RETENTION_REQUEST),
    sourceRef: objectOf(NAME_REF),
  },
);

const BACKUP_SCHEDULE_REF_REQUEST = shapeOf(
  "BackupScheduleRefRequest",
  { name: str },
  { expectedGeneration: int },
);

/** An acknowledgement is RECORDED and is never authoritative. The API reads no
 *  `Preflight` and gates on nothing; this is the annotation a person's second
 *  click leaves behind, so that a run taken past a red verdict says so. */
const READINESS_ACKNOWLEDGEMENT_REQUEST = shapeOf(
  "ReadinessAcknowledgementRequest",
  { preflight: str, state: oneOf(ACKNOWLEDGED_READINESS) },
);

/** `POST .../backups`. TWO BODIES IN ONE SHAPE, as the schema spells it:
 *  `scheduleRef` alone takes the schedule's own current revision, and
 *  `sourceRef` + `topicSelection` + a location is the ad-hoc run. Mixing them
 *  is a 422, and `ui/client.js` builds exactly one of the two. */
const CREATE_BACKUP_REQUEST = shapeOf(
  "CreateBackupRequest",
  {},
  {
    scheduleRef: objectOf(BACKUP_SCHEDULE_REF_REQUEST),
    sourceRef: objectOf(NAME_REF), topicSelection: objectOf(TOPIC_SELECTION_REQUEST),
    legacyArchive: objectOf(ARCHIVE_REQUEST), destinationRef: objectOf(NAME_REF),
    deadlineSeconds: int,
    readinessAcknowledgement: objectOf(READINESS_ACKNOWLEDGEMENT_REQUEST),
  },
);

// ===========================================================================
// D2: destinations, topic discoveries and operation readiness
// ===========================================================================
//
// THREE DOMAINS, ONE RULE: NOTHING HERE READS AS HEALTH BY DEFAULT.
//
//   * A destination's `status.valid` is OPTIONAL, and absent means the
//     controller has not reached a verdict. Absent is not `false` and is not
//     `true`; the page renders it as "not judged".
//   * A discovery's `visibility.state` is `unknown` for a listing that
//     succeeded. `unknown` is the HEALTHY default of that field, not a
//     failure, and never the word "complete".
//   * A check's lifecycle carries `unknown` as a member: a controller phase
//     this build does not recognise reads `unknown` and never `succeeded`.
//
// A decoder cannot enforce those readings on its own -- it can only make the
// fields impossible to MISS, which is what the required/optional split below
// does. `ui/render.js` carries the words and `ui/tests/pages.spec.js` holds
// them.

/** How a request names the bucket. NEVER a transport choice (defect G5). */
export const ADDRESSING_MODES = Object.freeze(["pathStyle", "virtualHosted"]);

/** Transport security. Immutable once the destination exists. */
export const TRANSPORT_SECURITY = Object.freeze(["tls", "insecureHttp"]);

/** The one object-store provider a destination may name today. */
export const STORAGE_PROVIDERS = Object.freeze(["s3"]);

/** How one role's credential is obtained. The last two are RESPONSE-ONLY
 *  spellings of an absent grant: sending either is a 422. */
export const ACCESS_MODES = Object.freeze([
  "secretKeys", "workloadIdentity", "controllerIdentity", "archiveReadGrant",
  "inheritsArchiveWrite", "notConfigured",
]);

/** Whether an explicit readiness test may write a marker object. */
export const WRITE_PROBES = Object.freeze(["disabled", "createOnlyMarker"]);

/** The four credentials a destination test can exercise. */
export const DESTINATION_ROLES = Object.freeze([
  "archiveWrite", "archiveRead", "evidenceWrite", "evidenceRead",
]);

/** The lifecycle of a transient check. `unknown` is a MEMBER, not an error:
 *  a controller phase this build does not recognise reads `unknown` and never
 *  `succeeded`. */
export const CHECK_LIFECYCLE = Object.freeze([
  "pending", "queued", "running", "succeeded", "failed", "cancelled", "unknown",
]);

/** The aggregate a preflight reports. `unknown` sits between `notReady` and
 *  `failed` on purpose: a blocking check that could not be decided, or was
 *  skipped, is neither a pass nor a refusal. */
export const PREFLIGHT_STATES = Object.freeze([
  "pending", "queued", "running", "ready", "notReady", "unknown", "failed", "cancelled",
]);

/** What a preflight is about.
 *
 *  `sourceConnection` IS THE ONE THAT NAMES NOTHING ELSE (D2-SOURCECHECK). The
 *  other three each need something the connection would be used FOR -- a
 *  destination, a plan, a topic list -- and a console control that asked for
 *  one of those in order to find out whether a broker answers would be a
 *  different question wearing the same label. */
export const PREFLIGHT_OPERATIONS = Object.freeze([
  "backup", "restore", "destinationAccess", "sourceConnection",
]);

/** One check's verdict. `skipped` never counts as a pass. */
export const CHECK_VERDICTS = Object.freeze(["ready", "notReady", "unknown", "skipped"]);

/** Whether a check's verdict gates the operation. */
export const CHECK_GATING = Object.freeze(["blocking", "advisory", "executionOnly"]);

/** How complete a topic inventory's own author believes it is. A successful
 *  Kafka list ALONE is `unknown`; this page must never render it "complete". */
export const VISIBILITY_STATES = Object.freeze(["unknown", "limited", "attestedComplete"]);

/** The two transient check kinds `GET .../operations/{kind}/{name}` serves. */
export const CHECK_OPERATION_KINDS = Object.freeze(["discovery", "preflight"]);

/** Why a stored readiness verdict no longer describes the caller's inputs.
 *
 *  SEVEN, AND THE SEVENTH IS THE IMPORTANT ONE. Six are the spellings
 *  `logweir_core::check_contract::StaleReason` renders; `unverifiable` is the
 *  product API's own, and it means "this service could not COMPARE something,
 *  so it will not call the verdict applicable". A page that treated it as a
 *  kind of freshness would be reporting a verdict nobody checked as current. */
export const STALE_REASONS = Object.freeze([
  "expired", "planHashChanged", "referentChanged", "caBundleChanged",
  "policyChanged", "inputsDigestChanged", "unverifiable",
]);

const CA_BUNDLE = shapeOf("CaBundleView", { configMapName: str, key: str }, { sha256: str });

const STORAGE = shapeOf(
  "StorageView",
  {
    provider: oneOf(STORAGE_PROVIDERS), bucket: str, prefix: str,
    addressing: oneOf(ADDRESSING_MODES),
  },
  { endpoint: str, region: str },
);

const TRANSPORT = shapeOf(
  "TransportView",
  { security: oneOf(TRANSPORT_SECURITY) },
  { caBundle: objectOf(CA_BUNDLE) },
);

const ACCESS_GRANT = shapeOf(
  "AccessGrantView",
  { mode: oneOf(ACCESS_MODES), keys: listOf(str) },
  { secretName: str, serviceAccountName: str },
);

const ACCESS = shapeOf("AccessView", {
  archiveWrite: objectOf(ACCESS_GRANT), archiveRead: objectOf(ACCESS_GRANT),
  evidenceWrite: objectOf(ACCESS_GRANT), evidenceRead: objectOf(ACCESS_GRANT),
});

// EVERY FIELD OPTIONAL, AND THAT IS THE CONTRACT. `valid` absent means the
// controller has not judged this destination; a decoder that required it would
// refuse every object one second old, and one that defaulted it to `false`
// would render "invalid" for "not looked at yet".
const DESTINATION_STATUS = shapeOf(
  "DestinationStatusView",
  {},
  { valid: bool, reason: str, message: str, observedGeneration: int, observedAt: str },
);

const LAST_TEST = shapeOf(
  "LastTestView",
  {
    preflightId: str, state: oneOf(PREFLIGHT_STATES), stale: bool, truncated: bool,
  },
  { observedAt: str },
);

const DESTINATION = shapeOf(
  "Destination",
  {
    name: str, namespace: str, uid: str, resourceVersion: str, generation: int,
    storage: objectOf(STORAGE), transport: objectOf(TRANSPORT),
    access: objectOf(ACCESS), writeProbe: oneOf(WRITE_PROBES),
    canonicalUrl: str, status: objectOf(DESTINATION_STATUS),
    default: bool,
  },
  { createdAt: str, description: str, locationDigest: str, lastTest: objectOf(LAST_TEST) },
);

const DESTINATION_SUMMARY = shapeOf(
  "DestinationSummary",
  {
    name: str, uid: str, generation: int, canonicalUrl: str,
    addressing: oneOf(ADDRESSING_MODES), transport: oneOf(TRANSPORT_SECURITY),
    status: objectOf(DESTINATION_STATUS), default: bool,
  },
  { description: str, endpoint: str },
);

const DESTINATION_USE = shapeOf("DestinationUseView", { kind: str, name: str }, { createdAt: str });

const DESTINATION_USAGE_RESPONSE = shapeOf("DestinationUsageResponse", {
  requestId: str, name: str, truncated: bool, basis: str,
  schedules: listOf(objectOf(DESTINATION_USE)),
  backups: listOf(objectOf(DESTINATION_USE)),
});

const DISCOVERY_CONNECTION = shapeOf(
  "DiscoveryConnectionView",
  { name: str },
  { uid: str, generation: int, principal: str, authMode: str },
);

const DISCOVERY_COUNTS = shapeOf("DiscoveryCountsView", {
  listed: int, returned: int, internalExcluded: int, errored: int,
});

const VISIBILITY = shapeOf(
  "VisibilityView",
  { state: oneOf(VISIBILITY_STATES), basis: listOf(str) },
  { attestation: str },
);

const EXPECTED_TOPICS = shapeOf("ExpectedTopicsView", {
  requested: int, visible: int, notAuthorized: int, notFound: int, unknown: int,
});

const CHECK_ERROR = shapeOf("CheckErrorView", { code: str }, { message: str });

const TOPIC_DISCOVERY = shapeOf(
  "TopicDiscovery",
  {
    id: str, namespace: str, uid: str, resourceVersion: str,
    connection: objectOf(DISCOVERY_CONNECTION),
    state: oneOf(CHECK_LIFECYCLE), terminal: bool,
    stale: bool, staleReasons: listOf(str), truncated: bool, chunkCount: int,
    conditions: listOf(objectOf(CONDITION)),
  },
  {
    createdAt: str, reason: str, observedAt: str, freshUntil: str, clusterId: str,
    counts: objectOf(DISCOVERY_COUNTS), truncationReason: str,
    visibility: objectOf(VISIBILITY), expected: objectOf(EXPECTED_TOPICS),
    topicsSha256: str, error: objectOf(CHECK_ERROR),
  },
);

const TOPIC_ENTRY = shapeOf(
  "TopicEntryView",
  { name: str, partitions: int, internal: bool, expected: bool },
  { errorCode: str },
);

const SCAN = shapeOf("ScanView", { complete: bool, chunksScanned: int });

const TOPIC_PAGE_RESPONSE = shapeOf("TopicPageResponse", {
  requestId: str, items: listOf(objectOf(TOPIC_ENTRY)), page: objectOf(PAGE),
  scan: objectOf(SCAN),
});

// TWO SLOTS, NOT ONE. A failed attempt never hides the last successful
// inventory, and a successful inventory never hides that the newest attempt
// failed -- so both are optional and a page renders whichever it was given.
const DISCOVERY_LATEST_RESPONSE = shapeOf(
  "DiscoveryLatestResponse",
  { requestId: str },
  { latestAttempt: objectOf(TOPIC_DISCOVERY), lastSuccessful: objectOf(TOPIC_DISCOVERY) },
);

const REFERENT = shapeOf("ReferentView", { kind: str, name: str }, { uid: str, generation: int });

const PREFLIGHT_BINDING = shapeOf(
  "PreflightBindingView",
  { referents: listOf(objectOf(REFERENT)) },
  { planHash: str, inputsDigest: str },
);

const CHECK_SCOPE = shapeOf("CheckScopeView", {}, { kind: str, name: str, uid: str });

const CHECK_ENTRY = shapeOf(
  "CheckEntryView",
  { id: str, state: oneOf(CHECK_VERDICTS) },
  {
    category: str, gating: oneOf(CHECK_GATING), code: str, message: str,
    remedy: str, authority: str, scope: objectOf(CHECK_SCOPE),
    observedAt: str, expiresAt: str,
  },
);

const EXECUTION_ONLY = shapeOf("ExecutionOnlyView", { id: str, note: str });

const STALE_REASON = shapeOf(
  "StaleReasonView",
  { reason: oneOf(STALE_REASONS) },
  { kind: str, name: str, basis: str },
);

const PREFLIGHT = shapeOf(
  "Preflight",
  {
    id: str, namespace: str, uid: str, resourceVersion: str,
    operation: oneOf(PREFLIGHT_OPERATIONS), state: oneOf(PREFLIGHT_STATES),
    terminal: bool, binding: objectOf(PREFLIGHT_BINDING),
    applicable: bool, stale: bool,
    staleReasons: listOf(objectOf(STALE_REASON)), staleBasis: listOf(str),
    checks: listOf(objectOf(CHECK_ENTRY)), warnings: listOf(objectOf(CHECK_ENTRY)),
    executionOnly: listOf(objectOf(EXECUTION_ONLY)),
    detailsAvailable: bool, conditions: listOf(objectOf(CONDITION)),
  },
  { createdAt: str, reason: str, observedAt: str, expiresAt: str },
);

const DETAIL_ENTRY = shapeOf("DetailEntryView", { entry: opaque }, { check: str });

const DETAIL_PAGE_RESPONSE = shapeOf("DetailPageResponse", {
  requestId: str, items: listOf(objectOf(DETAIL_ENTRY)), page: objectOf(PAGE),
});

// NO `result`, NO `evidence`, NO `verification`, AND THAT IS THE POINT. A
// transient check has none of those facts, so the product API publishes a
// DIFFERENT document for it rather than three empty fields a console would be
// invited to render as "verification: pending" for a topic list.
const CHECK_OPERATION = shapeOf(
  "CheckOperation",
  {
    kind: oneOf(CHECK_OPERATION_KINDS), name: str, namespace: str, uid: str,
    resourceVersion: str, state: oneOf(CHECK_LIFECYCLE), terminal: bool,
    cancellable: bool, conditions: listOf(objectOf(CONDITION)),
  },
  { createdAt: str, stateReason: str, message: str, observedAt: str },
);

const CANCEL_RESPONSE = shapeOf("CancelResponse", {
  requestId: str, id: str, state: str, alreadyTerminal: bool,
});

const DESTINATION_LIST = envelope("DestinationList");
const TOPIC_DISCOVERY_LIST = envelope("TopicDiscoveryList");

const DESTINATION_RESPONSE = item("DestinationResponse", DESTINATION);
const CHECK_OPERATION_RESPONSE = readOnlyItem("CheckOperationResponse", CHECK_OPERATION);
const PREFLIGHT_RESPONSE = item("PreflightResponse", PREFLIGHT);

// `reused` is this route's own third answer: not "made" and not "replayed"
// but "a fresh identical result already existed and you are getting it".
const TOPIC_DISCOVERY_RESPONSE = shapeOf(
  "TopicDiscoveryResponse",
  { item: objectOf(TOPIC_DISCOVERY), requestId: str },
  { replayed: bool, reused: bool },
);

// ------------------------------------------------- the D2 request shapes

const CA_BUNDLE_REQUEST = shapeOf("CaBundleRequest", { configMapName: str }, { key: str });

const STORAGE_REQUEST = shapeOf(
  "StorageRequest",
  { provider: oneOf(STORAGE_PROVIDERS), bucket: str, addressing: oneOf(ADDRESSING_MODES) },
  { prefix: str, region: str, endpoint: str },
);

const TRANSPORT_REQUEST = shapeOf(
  "TransportRequest",
  { security: oneOf(TRANSPORT_SECURITY) },
  { caBundle: objectOf(CA_BUNDLE_REQUEST) },
);

const UPDATE_TRANSPORT_REQUEST = shapeOf(
  "UpdateTransportRequest",
  {},
  { caBundle: objectOf(CA_BUNDLE_REQUEST) },
);

const EXISTING_SECRET_REQUEST = shapeOf(
  "ExistingSecretRequest",
  { name: str },
  { accessKeyIdKey: str, secretAccessKeyKey: str, sessionTokenKey: str },
);

// THE WRITE-ONLY HALF. These three field names appear in this module and in
// the form that collects them, and nowhere else: no draft keeps them, no log
// line carries them, and the response to a create that used them carries a
// Secret NAME and no value at all.
const NEW_CREDENTIAL_REQUEST = shapeOf(
  "NewCredentialRequest",
  { accessKeyId: str, secretAccessKey: str },
  { sessionToken: str },
);

const SECRET_SOURCE_REQUEST = shapeOf(
  "SecretSourceRequest",
  {},
  { existing: objectOf(EXISTING_SECRET_REQUEST), new: objectOf(NEW_CREDENTIAL_REQUEST) },
);

const WORKLOAD_IDENTITY_REQUEST = shapeOf("WorkloadIdentityRequest", {}, { serviceAccountName: str });

const ACCESS_GRANT_REQUEST = shapeOf(
  "AccessGrantRequest",
  { mode: oneOf(ACCESS_MODES) },
  { secret: objectOf(SECRET_SOURCE_REQUEST), workloadIdentity: objectOf(WORKLOAD_IDENTITY_REQUEST) },
);

const ACCESS_REQUEST = shapeOf(
  "AccessRequest",
  { archiveWrite: objectOf(ACCESS_GRANT_REQUEST) },
  {
    archiveRead: objectOf(ACCESS_GRANT_REQUEST),
    evidenceWrite: objectOf(ACCESS_GRANT_REQUEST),
    evidenceRead: objectOf(ACCESS_GRANT_REQUEST),
  },
);

const READINESS_REQUEST = shapeOf("ReadinessRequest", {}, { writeProbe: oneOf(WRITE_PROBES) });

const CREATE_DESTINATION_REQUEST = shapeOf(
  "CreateDestinationRequest",
  {
    name: str, storage: objectOf(STORAGE_REQUEST), transport: objectOf(TRANSPORT_REQUEST),
    access: objectOf(ACCESS_REQUEST),
  },
  { description: str, readiness: objectOf(READINESS_REQUEST), default: bool },
);

const UPDATE_DESTINATION_ACCESS_REQUEST = shapeOf(
  "UpdateDestinationAccessRequest",
  { expectedGeneration: int, access: objectOf(ACCESS_REQUEST) },
  { transport: objectOf(UPDATE_TRANSPORT_REQUEST) },
);

const TEST_DESTINATION_REQUEST = shapeOf(
  "TestDestinationRequest",
  {},
  { roles: listOf(oneOf(DESTINATION_ROLES)) },
);

const DESTINATION_FROM_LEGACY_REQUEST = shapeOf(
  "DestinationFromLegacyRequest",
  { name: str, access: objectOf(ACCESS_REQUEST) },
  { description: str, sourceSchedule: str, sourceBackup: str },
);

const CREATE_TOPIC_DISCOVERY_REQUEST = shapeOf(
  "CreateTopicDiscoveryRequest",
  {},
  {
    includeInternal: bool, expectedTopics: listOf(str), maxTopics: int,
    timeoutSeconds: int, reuseFresh: bool,
  },
);

const RECOVERY_POINT_REQUEST = shapeOf("RecoveryPointRequest", { backupName: str }, { backupUid: str });

/** PLAT-15.2: a catalog point a restore readiness check is about -- the
 *  catalog that lists it and its content-derived id. */
const CATALOG_POINT_REQUEST = shapeOf("CatalogPointRequest", { catalog: str, pointId: str });

const BACKUP_PREFLIGHT_REQUEST = shapeOf(
  "BackupPreflightRequest",
  { sourceConnection: str, topics: listOf(str) },
  { destination: str, legacyArchive: objectOf(ARCHIVE_REQUEST), schedule: str },
);

const RESTORE_PREFLIGHT_REQUEST = shapeOf(
  "RestorePreflightRequest",
  {},
  {
    planBytes: str, planHash: str, target: str, restoreName: str,
    sourceDestination: str, evidenceDestination: str,
    legacySourceArchive: objectOf(ARCHIVE_REQUEST),
    recoveryPoint: objectOf(RECOVERY_POINT_REQUEST),
    catalogPoint: objectOf(CATALOG_POINT_REQUEST),
  },
);

const DESTINATION_ACCESS_PREFLIGHT_REQUEST = shapeOf("DestinationAccessPreflightRequest", {
  destination: str, roles: listOf(oneOf(DESTINATION_ROLES)),
});

// ONE REQUIRED FIELD AND NO OPTIONAL ONE. The absences are the contract: a
// source-connectivity check names no destination, no plan and no topic, and a
// body that carried one would be a `422 unknown_field` from the product API.
const SOURCE_CONNECTION_PREFLIGHT_REQUEST = shapeOf("SourceConnectionPreflightRequest", {
  connectionRef: str,
});

const CREATE_PREFLIGHT_REQUEST = shapeOf(
  "CreatePreflightRequest",
  { operation: oneOf(PREFLIGHT_OPERATIONS) },
  {
    backup: objectOf(BACKUP_PREFLIGHT_REQUEST),
    restore: objectOf(RESTORE_PREFLIGHT_REQUEST),
    destinationAccess: objectOf(DESTINATION_ACCESS_PREFLIGHT_REQUEST),
    sourceConnection: objectOf(SOURCE_CONNECTION_PREFLIGHT_REQUEST),
    skipChecks: listOf(str), timeoutSeconds: int,
  },
);

/** The request shapes, by the plural whose create route takes them, plus the
 *  one update. `ui/client.js` builds a body for each; the suite checks the
 *  body it built against the shape, and the shape against the schema. */
export const CONSOLE_REQUESTS = Object.freeze({
  connections: CREATE_CONNECTION_REQUEST,
  schedules: CREATE_SCHEDULE_REQUEST,
  restores: CREATE_RESTORE_REQUEST,
  "schedules:set-suspension": SET_SUSPENSION_REQUEST,
  // D2 W13. Keyed by the ACTION the client names, which is the same key
  // `api.js`'s frozen action table uses, so the body a page builds and the
  // route it is sent to cannot come apart.
  destinations: CREATE_DESTINATION_REQUEST,
  preflights: CREATE_PREFLIGHT_REQUEST,
  "destinations:update-access": UPDATE_DESTINATION_ACCESS_REQUEST,
  "destinations:test": TEST_DESTINATION_REQUEST,
  "destinations:from-legacy": DESTINATION_FROM_LEGACY_REQUEST,
  "connections:topic-discoveries": CREATE_TOPIC_DISCOVERY_REQUEST,
  // D1 W7. The policy replace is keyed by the route it is sent to; the manual
  // run is keyed by the plural, because it goes through `consoleCreate`.
  "schedules:policy": UPDATE_SCHEDULE_POLICY_REQUEST,
  backups: CREATE_BACKUP_REQUEST,
  // PLAT-19.2: a governed approver's countersignature, keyed by the action.
  "restores:approval": SUBMIT_APPROVAL_REQUEST,
});

/** Checks a body this client BUILT against the shape the server publishes.
 *  Returns [`Decoded`]; an unknown field is recorded here as it is on a
 *  response, but a mutation input that carries one is a `422` from the product
 *  API, so `ui/tests/client.spec.js` asserts the list is empty. */
export function decodeRequest(name, value) {
  const shape = CONSOLE_REQUESTS[name];
  if (shape === undefined) {
    throw contractFailure("ConsoleRequest", name, "no request shape is declared for this route");
  }
  return decodeWith(shape, value);
}

/** Every console DTO this client decodes, by the name the OpenAPI document
 *  gives it. `ui/tests/contract.spec.js` walks this map against
 *  `schemas/logweir-api-v1.openapi.json` and fails when a field is required
 *  there and optional here, or named here and absent there. */
export const CONSOLE_SHAPES = Object.freeze({
  SessionResponse: SESSION,
  Capabilities: CAPABILITIES,
  NamespaceGrant: GRANT,
  ActorView: ACTOR,
  Problem: PROBLEM,
  FieldError: FIELD_ERROR,
  Page: PAGE,
  ConditionView: CONDITION,
  NameRef: NAME_REF,
  ArchiveView: ARCHIVE,
  ConnectionAuthView: CONNECTION_AUTH,
  ReachabilityView: REACHABILITY,
  Connection: CONNECTION,
  RetentionView: RETENTION,
  RemovableSetView: REMOVABLE_SET,
  RetentionReportView: RETENTION_REPORT,
  ScheduleStatusView: SCHEDULE_STATUS,
  TopicExclusions: TOPIC_EXCLUSIONS,
  AllUserTopics: ALL_USER_TOPICS,
  Schedule: SCHEDULE,
  OperationSummary: OPERATION_SUMMARY,
  WindowCoveredView: WINDOW_COVERED,
  ObservedAuthView: OBSERVED_AUTH,
  Backup: BACKUP,
  RestoreTargetView: RESTORE_TARGET,
  Restore: RESTORE,
  SubjectRefView: SUBJECT_REF,
  VerifiedSubjectView: VERIFIED_SUBJECT,
  Approval: APPROVAL,
  ApprovalPacket: APPROVAL_PACKET,
  OperationEvidence: OPERATION_EVIDENCE,
  OperationResult: OPERATION_RESULT,
  OperationVerification: OPERATION_VERIFICATION,
  Operation: OPERATION,
  ConnectionList: CONNECTION_LIST,
  ScheduleList: SCHEDULE_LIST,
  BackupList: BACKUP_LIST,
  RestoreList: RESTORE_LIST,
  ApprovalList: APPROVAL_LIST,
  ConnectionResponse: CONNECTION_RESPONSE,
  ScheduleResponse: SCHEDULE_RESPONSE,
  BackupResponse: BACKUP_RESPONSE,
  RestoreResponse: RESTORE_RESPONSE,
  RestoreRoutingView: RESTORE_AUTHORIZATION,
  ApprovalProvenanceView: APPROVAL_AUTHORIZATION,
  ApprovalPolicyView: APPROVAL_POLICY,
  ApprovalPolicyResponse: APPROVAL_POLICY_RESPONSE,
  SubmitApprovalRequest: SUBMIT_APPROVAL_REQUEST,
  ApprovalResponse: APPROVAL_RESPONSE,
  ApprovalPacketResponse: APPROVAL_PACKET_RESPONSE,
  OperationResponse: OPERATION_RESPONSE,
  ArchiveRequest: ARCHIVE_REQUEST,
  ConnectionAuthRequest: CONNECTION_AUTH_REQUEST,
  CreateConnectionRequest: CREATE_CONNECTION_REQUEST,
  RetentionRequest: RETENTION_REQUEST,
  CreateScheduleRequest: CREATE_SCHEDULE_REQUEST,
  TopicNamingRequest: TOPIC_NAMING_REQUEST,
  RestoreTargetRequest: RESTORE_TARGET_REQUEST,
  CreateRestoreRequest: CREATE_RESTORE_REQUEST,
  SetSuspensionRequest: SET_SUSPENSION_REQUEST,

  // D1 W7: the cadence policy, the revision and the manual run.
  NextRunView: NEXT_RUN,
  ActiveRunView: ACTIVE_RUN,
  SchedulePolicyView: SCHEDULE_POLICY,
  RetryPolicy: RETRY_POLICY,
  CadencePreset: CADENCE_PRESET,
  TriggerView: TRIGGER,
  ScheduleRefView: SCHEDULE_REF,
  CadencePreviewResponse: CADENCE_PREVIEW_RESPONSE,
  ScheduleContextView: SCHEDULE_CONTEXT,
  ManualBackupResponse: MANUAL_BACKUP_RESPONSE,
  PolicyChangedDetail: POLICY_CHANGED_DETAIL,
  TopicSelectionRequest: TOPIC_SELECTION_REQUEST,
  UpdateSchedulePolicyRequest: UPDATE_SCHEDULE_POLICY_REQUEST,
  BackupScheduleRefRequest: BACKUP_SCHEDULE_REF_REQUEST,
  ReadinessAcknowledgementRequest: READINESS_ACKNOWLEDGEMENT_REQUEST,
  CreateBackupRequest: CREATE_BACKUP_REQUEST,

  // D2: destinations, topic discoveries and operation readiness.
  CaBundleView: CA_BUNDLE,
  StorageView: STORAGE,
  TransportView: TRANSPORT,
  AccessGrantView: ACCESS_GRANT,
  AccessView: ACCESS,
  DestinationStatusView: DESTINATION_STATUS,
  LastTestView: LAST_TEST,
  Destination: DESTINATION,
  DestinationSummary: DESTINATION_SUMMARY,
  DestinationUseView: DESTINATION_USE,
  DestinationUsageResponse: DESTINATION_USAGE_RESPONSE,
  DestinationList: DESTINATION_LIST,
  DestinationResponse: DESTINATION_RESPONSE,
  DiscoveryConnectionView: DISCOVERY_CONNECTION,
  DiscoveryCountsView: DISCOVERY_COUNTS,
  VisibilityView: VISIBILITY,
  ExpectedTopicsView: EXPECTED_TOPICS,
  CheckErrorView: CHECK_ERROR,
  TopicDiscovery: TOPIC_DISCOVERY,
  TopicDiscoveryList: TOPIC_DISCOVERY_LIST,
  TopicDiscoveryResponse: TOPIC_DISCOVERY_RESPONSE,
  DiscoveryLatestResponse: DISCOVERY_LATEST_RESPONSE,
  TopicEntryView: TOPIC_ENTRY,
  ScanView: SCAN,
  TopicPageResponse: TOPIC_PAGE_RESPONSE,
  ReferentView: REFERENT,
  PreflightBindingView: PREFLIGHT_BINDING,
  CheckScopeView: CHECK_SCOPE,
  CheckEntryView: CHECK_ENTRY,
  ExecutionOnlyView: EXECUTION_ONLY,
  StaleReasonView: STALE_REASON,
  Preflight: PREFLIGHT,
  PreflightResponse: PREFLIGHT_RESPONSE,
  DetailEntryView: DETAIL_ENTRY,
  DetailPageResponse: DETAIL_PAGE_RESPONSE,
  CheckOperation: CHECK_OPERATION,
  CheckOperationResponse: CHECK_OPERATION_RESPONSE,
  CancelResponse: CANCEL_RESPONSE,
  CaBundleRequest: CA_BUNDLE_REQUEST,
  StorageRequest: STORAGE_REQUEST,
  TransportRequest: TRANSPORT_REQUEST,
  UpdateTransportRequest: UPDATE_TRANSPORT_REQUEST,
  ExistingSecretRequest: EXISTING_SECRET_REQUEST,
  NewCredentialRequest: NEW_CREDENTIAL_REQUEST,
  SecretSourceRequest: SECRET_SOURCE_REQUEST,
  WorkloadIdentityRequest: WORKLOAD_IDENTITY_REQUEST,
  AccessGrantRequest: ACCESS_GRANT_REQUEST,
  AccessRequest: ACCESS_REQUEST,
  ReadinessRequest: READINESS_REQUEST,
  CreateDestinationRequest: CREATE_DESTINATION_REQUEST,
  UpdateDestinationAccessRequest: UPDATE_DESTINATION_ACCESS_REQUEST,
  TestDestinationRequest: TEST_DESTINATION_REQUEST,
  DestinationFromLegacyRequest: DESTINATION_FROM_LEGACY_REQUEST,
  CreateTopicDiscoveryRequest: CREATE_TOPIC_DISCOVERY_REQUEST,
  RecoveryPointRequest: RECOVERY_POINT_REQUEST,
  CatalogPointRequest: CATALOG_POINT_REQUEST,
  BackupPreflightRequest: BACKUP_PREFLIGHT_REQUEST,
  RestorePreflightRequest: RESTORE_PREFLIGHT_REQUEST,
  DestinationAccessPreflightRequest: DESTINATION_ACCESS_PREFLIGHT_REQUEST,
  SourceConnectionPreflightRequest: SOURCE_CONNECTION_PREFLIGHT_REQUEST,
  CreatePreflightRequest: CREATE_PREFLIGHT_REQUEST,
});

/** EVERY CLOSED SET THIS CLIENT HOLDS, by the name the OpenAPI document gives
 *  it. Each one was hand-copied out of that document, and a hand-copied list
 *  is a list that drifts: a server that adds an eleventh `OperationState`
 *  would turn every console list of that kind into a whole-page contract
 *  failure, and no test would have gone red first. `ui/tests/contract.spec.js`
 *  compares each of these with the union of the schema's own `oneOf[].enum`.
 *
 *  THE COPIES ELSEWHERE IN THE TREE ARE JOINED TO THESE. `ui/validate.js`
 *  imports the two it enforces rather than keeping its own. `ui/plan.js`'s
 *  `TARGET_MODES` stays its own -- it is the RUNNER's grammar, not the product
 *  API's, and the two are equal by agreement rather than by construction -- so
 *  the suite asserts that agreement instead of assuming it. */
export const CONSOLE_ENUMS = Object.freeze({
  ReachabilityState: REACHABILITY_STATES,
  ResultStatus: RESULT_STATUSES,
  OperationKind: OPERATION_KINDS,
  OperationState: OPERATION_STATES,
  VerificationState: VERIFICATION_STATES,
  ConnectionAuthMode: CONNECTION_AUTH_MODES,
  ConnectionRole: CONNECTION_ROLES,
  ConcurrencyPolicy: CONCURRENCY_POLICIES,
  RestoreMode: RESTORE_MODES,
  Role: ROLES,
  IncompleteDiscoveryPolicy: INCOMPLETE_DISCOVERY_POLICIES,
  CadenceAdjustment: CADENCE_ADJUSTMENTS,
  CatchUpPolicy: CATCH_UP_POLICIES,
  TriggerKind: TRIGGER_KINDS,
  AcknowledgedReadiness: ACKNOWLEDGED_READINESS,
  AddressingDto: ADDRESSING_MODES,
  TransportSecurityDto: TRANSPORT_SECURITY,
  StorageProviderDto: STORAGE_PROVIDERS,
  AccessModeDto: ACCESS_MODES,
  WriteProbeDto: WRITE_PROBES,
  DestinationRoleDto: DESTINATION_ROLES,
  CheckLifecycle: CHECK_LIFECYCLE,
  PreflightState: PREFLIGHT_STATES,
  PreflightOperationDto: PREFLIGHT_OPERATIONS,
  CheckVerdict: CHECK_VERDICTS,
  CheckGating: CHECK_GATING,
  VisibilityState: VISIBILITY_STATES,
  CheckOperationKind: CHECK_OPERATION_KINDS,
  StaleReasonKind: STALE_REASONS,
});

/** @returns {Decoded} */
export function decodeSession(value) {
  return decodeWith(SESSION, value);
}

/** @returns {Decoded} */
export function decodeProblem(value) {
  return decodeWith(PROBLEM, value);
}

/** The five list envelopes and the seven single-item responses, by the plural
 *  the client addresses. One table, so a caller names a kind and never a
 *  shape. */
export const CONSOLE_ROUTES = Object.freeze({
  connections: Object.freeze({
    list: CONNECTION_LIST, item: CONNECTION, response: CONNECTION_RESPONSE,
  }),
  schedules: Object.freeze({
    list: SCHEDULE_LIST, item: SCHEDULE, response: SCHEDULE_RESPONSE,
  }),
  backups: Object.freeze({
    list: BACKUP_LIST, item: BACKUP, response: BACKUP_RESPONSE,
  }),
  restores: Object.freeze({
    list: RESTORE_LIST, item: RESTORE, response: RESTORE_RESPONSE,
  }),
  approvals: Object.freeze({
    list: APPROVAL_LIST, item: APPROVAL, response: APPROVAL_RESPONSE,
  }),
  destinations: Object.freeze({
    list: DESTINATION_LIST, item: DESTINATION_SUMMARY, response: DESTINATION_RESPONSE,
  }),
  // A DISCOVERY'S LIST ROW IS THE WHOLE OBJECT, unlike a destination's, whose
  // list row is a summary. That is the product API's shape and not a
  // simplification here: a caller choosing between the latest attempt and the
  // last successful inventory needs the counts and the visibility state on
  // both, so a summary would have had to carry them anyway.
  "topic-discoveries": Object.freeze({
    list: TOPIC_DISCOVERY_LIST, item: TOPIC_DISCOVERY, response: TOPIC_DISCOVERY_RESPONSE,
  }),
  preflights: Object.freeze({
    list: null, item: PREFLIGHT, response: PREFLIGHT_RESPONSE,
  }),
});

/** @returns {Decoded} */
export function decodeConsoleList(plural, value) {
  const route = CONSOLE_ROUTES[plural];
  if (route === undefined || route.list === null) {
    throw contractFailure("ConsoleRoute", plural, "no list route is defined for this kind");
  }
  return decodeListWith(route.list, route.item, value);
}

/** @returns {Decoded} */
export function decodeConsoleItem(plural, value) {
  const route = CONSOLE_ROUTES[plural];
  if (route === undefined) {
    throw contractFailure("ConsoleRoute", plural, "no item route is defined for this kind");
  }
  return decodeWith(route.response, value);
}

/** PLAT-19.2: a namespace's effective approval policy. @returns {Decoded} */
export function decodeApprovalPolicy(value) {
  return decodeWith(APPROVAL_POLICY_RESPONSE, value);
}

/** @returns {Decoded} */
export function decodeApprovalPacket(value) {
  return decodeWith(APPROVAL_PACKET_RESPONSE, value);
}

/** A durable run's normalized status: a `Backup` or a `Restore`.
 *  @returns {Decoded} */
export function decodeOperation(value) {
  return decodeWith(OPERATION_RESPONSE, value);
}

/** A TRANSIENT CHECK's normalized status: a `TopicDiscovery` or a `Preflight`.
 *
 *  A SEPARATE DECODER BECAUSE IT IS A SEPARATE DOCUMENT. `GET
 *  .../operations/{kind}/{name}` answers `OperationResponse` for `backup` and
 *  `restore` and `CheckOperationResponse` for `discovery` and `preflight`, and
 *  the second carries no `result`, no `evidence`, no `verification` and no
 *  `verifiedSuccess`. Reading one as the other would either fail on four
 *  required fields or -- with a tolerant reader -- put "verification: pending"
 *  on screen for a topic list. Two decoders is the shape of that fact.
 *  @returns {Decoded} */
export function decodeCheckOperation(value) {
  return decodeWith(CHECK_OPERATION_RESPONSE, value);
}

/** One page of a discovery's stored topics. @returns {Decoded} */
export function decodeTopicPage(value) {
  return decodeWith(TOPIC_PAGE_RESPONSE, value);
}

/** The newest attempt and the last successful inventory for one connection.
 *  Both slots are optional and neither hides the other. @returns {Decoded} */
export function decodeDiscoveryLatest(value) {
  return decodeWith(DISCOVERY_LATEST_RESPONSE, value);
}

/** What names a destination, with the BASIS on which the lists were built --
 *  so an empty answer is never read as "nothing uses this". @returns {Decoded} */
export function decodeDestinationUsage(value) {
  return decodeWith(DESTINATION_USAGE_RESPONSE, value);
}

/** One page of a preflight's detail document. @returns {Decoded} */
export function decodeDetailPage(value) {
  return decodeWith(DETAIL_PAGE_RESPONSE, value);
}

/** The answer to a cancel: the state after the request, and whether the check
 *  had already finished. @returns {Decoded} */
export function decodeCancel(value) {
  return decodeWith(CANCEL_RESPONSE, value);
}

/** A draft cadence's next firings, as `GET /api/v1/cadence-previews` computed
 *  them. THE BROWSER NEVER EVALUATES CRON: this is the whole of what the
 *  schedule form knows about when a policy will fire, and the saved object's
 *  `status.nextRuns` is the same shape from the controller.
 *  @returns {Decoded} */
export function decodeCadencePreview(value) {
  return decodeWith(CADENCE_PREVIEW_RESPONSE, value);
}

/** A manual run, and the schedule it was taken from.
 *  @returns {Decoded} */
export function decodeManualBackup(value) {
  return decodeWith(MANUAL_BACKUP_RESPONSE, value);
}

/** The `policy` extension member of a `409 policy_changed`, or `null` when the
 *  problem carries none.
 *
 *  A PROBLEM THAT IS NOT THAT ONE IS NOT AN ERROR HERE. Every other refusal a
 *  policy edit or a manual run can get -- `412`, `422`, `409
 *  idempotency_conflict` -- reaches the page as its own message; this reads
 *  the one member that carries a FACT the page needs (the revision that is in
 *  force now) and answers `null` for everything else rather than throwing a
 *  contract failure over a problem document that was perfectly well formed. */
export function decodePolicyChanged(problem) {
  const detail = (problem || {}).policy;
  if (detail === null || detail === undefined) {
    return null;
  }
  return decodeWith(POLICY_CHANGED_DETAIL, detail).value;
}

// ===========================================================================
// the legacy custom resources -- config/crd, as kube-apiserver returns them
// ===========================================================================
//
// THESE DECODERS VALIDATE AND RETURN THE OBJECT UNCHANGED. The pages read a
// custom resource by reaching into it, and copying it field by field here
// would be the very field loss this module exists to prevent: a key nobody
// thought to list would vanish on the way through. So the legacy half checks
// the fields WITHOUT WHICH A PAGE CANNOT RENDER, records every other key it
// does not know about, and hands back the object the API server sent, byte for
// byte the same reference.

const LEGACY_META = shapeOf(
  "ObjectMeta",
  { name: str },
  {
    namespace: str, uid: str, resourceVersion: str, generation: int,
    creationTimestamp: str, labels: opaque, annotations: opaque,
    ownerReferences: opaque, finalizers: opaque, managedFields: opaque,
    deletionTimestamp: str, deletionGracePeriodSeconds: int,
    selfLink: str, generateName: str,
  },
);

/** What each legacy kind must carry for its page to mean anything. The `spec`
 *  entries are the CRD's own required fields; `status` is never required --
 *  the controller has not necessarily written one yet, and a page that refused
 *  an unreconciled object would refuse every object one second old. */
const LEGACY_SPECS = Object.freeze({
  kafkaclusters: shapeOf("KafkaCluster.spec", { bootstrapServers: listOf(str) }),
  backupschedules: shapeOf("BackupSchedule.spec", {
    schedule: str, sourceRef: objectOf(NAME_REF), topics: listOf(str),
  }),
  backups: shapeOf("Backup.spec", { sourceRef: objectOf(NAME_REF), topics: listOf(str) }),
  restores: shapeOf("Restore.spec", {
    planBytes: opaque, approvalRef: objectOf(NAME_REF), backupSetRef: str, pointInTime: str,
  }),
  approvals: shapeOf("Approval.spec", {
    subjectRef: objectOf(SUBJECT_REF), planHash: str,
    approvalBytes: opaque, sidecarBytes: opaque,
  }),
  trustrosters: shapeOf("TrustRoster.spec", {}),
  // D3 (PLAT-14.2, PLAT-15.1, PLAT-16.2, PLAT-19.1). Four kinds this page
  // READS and never writes. Each required field below is the one the CRD
  // itself requires and the one the page's renderer cannot do without:
  // a ProtectionPolicy with no `protects` names nothing to protect, a
  // RecoveryCatalog with no destination names no archive, a RetentionPolicy
  // with no `scope` has no prefix its plan could be bounded by, and a
  // TrustPolicy with no `keys` array is not a trust policy. `status` is never
  // required, here as everywhere: an object one second old has none.
  protectionpolicies: shapeOf("ProtectionPolicy.spec", { protects: opaque, objectives: opaque }),
  recoverycatalogs: shapeOf("RecoveryCatalog.spec", {}),
  retentionpolicies: shapeOf("RetentionPolicy.spec", { scope: opaque }),
  trustpolicies: shapeOf("TrustPolicy.spec", { keys: listOf(opaque) }),
});

/** The `kind` a legacy object of each plural declares. Checked, because a
 *  proxy misconfigured onto another group would otherwise render silently.
 *
 *  ON A LIST IT IS THE LIST'S OWN `kind` THAT CARRIES THIS. Kubernetes omits
 *  `apiVersion` and `kind` on the items inside a `List`, so the per-item check
 *  below never fires for a list member and could not be made to; what stands
 *  there instead is the required `spec` fields, and what stands for the list is
 *  `<Kind>List` on the envelope, checked in `decodeLegacyList`. */
const LEGACY_KINDS = Object.freeze({
  kafkaclusters: "KafkaCluster",
  backupschedules: "BackupSchedule",
  backups: "Backup",
  restores: "Restore",
  approvals: "Approval",
  trustrosters: "TrustRoster",
  protectionpolicies: "ProtectionPolicy",
  recoverycatalogs: "RecoveryCatalog",
  retentionpolicies: "RetentionPolicy",
  trustpolicies: "TrustPolicy",
});

/** Validates one custom resource of `plural` and RETURNS IT UNCHANGED.
 *  @returns {Decoded} */
export function decodeLegacyObject(plural, value) {
  const spec = LEGACY_SPECS[plural];
  const kind = LEGACY_KINDS[plural];
  if (spec === undefined) {
    throw contractFailure("CustomResource", plural, "no legacy shape is defined for this kind");
  }
  const dto = kind;
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw contractFailure(dto, "", "expected an object, got " + typeName(value));
  }
  const ctx = { unknown: [] };
  if (typeof value.kind === "string" && value.kind !== kind) {
    throw contractFailure(dto, "kind", "expected " + kind + ", got " + JSON.stringify(value.kind));
  }
  if (value.metadata === undefined || value.metadata === null) {
    throw contractFailure(dto, "metadata", "the field is required by the contract and is absent");
  }
  readShape(LEGACY_META, value.metadata, ctx, dto, "metadata");
  if (value.spec === undefined || value.spec === null) {
    throw contractFailure(dto, "spec", "the field is required by the contract and is absent");
  }
  readShape(
    shapeOf(spec.name, spec.required, everyOtherKey(value.spec, spec.required)),
    value.spec,
    ctx,
    dto,
    "spec",
  );
  return { value: value, unknown: ctx.unknown };
}

/** Validates a legacy list and RETURNS IT UNCHANGED, with every item checked.
 *  @returns {Decoded} */
export function decodeLegacyList(plural, value) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw contractFailure(LEGACY_KINDS[plural] + "List", "", "expected an object, got " + typeName(value));
  }
  const listKind = LEGACY_KINDS[plural] + "List";
  if (typeof value.kind === "string" && value.kind !== listKind) {
    throw contractFailure(
      listKind,
      "kind",
      "expected " + listKind + ", got " + JSON.stringify(value.kind),
    );
  }
  if (!Array.isArray(value.items)) {
    throw contractFailure(
      listKind,
      "items",
      "the field is required by the contract and is " + typeName(value.items),
    );
  }
  const unknown = [];
  for (let i = 0; i < value.items.length; i += 1) {
    const decoded = decodeLegacyObject(plural, value.items[i]);
    for (const path of decoded.unknown) {
      unknown.push("items[" + String(i) + "]." + path);
    }
  }
  return { value: value, unknown: unknown };
}

// A legacy `spec` is validated for its REQUIRED fields only: every other key
// it carries is declared optional-and-opaque here so the walk records nothing
// as unknown for a resource whose CRD simply has more fields than a page
// reads. The unknown list on the legacy side is therefore about `metadata`,
// which is Kubernetes' own and stable, and about the kind check above.
function everyOtherKey(spec, required) {
  const optional = {};
  for (const key of Object.keys(spec)) {
    if (!Object.prototype.hasOwnProperty.call(required, key)) {
      optional[key] = opaque;
    }
  }
  return optional;
}

// ===========================================================================
// D3 -- the shapes the console's D3 surfaces consume
// ===========================================================================
//
// RECONCILED AGAINST THE PUBLISHED DOCUMENT. These were declared before the API
// half existed, from D3's own status contracts, and every name and every
// `required` set below now comes from `schemas/logweir-api-v1.openapi.json` --
// which is the source of truth for names, and against which
// `contract.spec.js`'s drift arm pins each of them field for field. Nothing
// here is assumed any more; the shapes that used to be
// (`D3Operation`, `RecoveryCatalog`, `CatalogPointView`, `CreateCatalogRequest`
// and the four `*StatusView`s) are gone, and this comment is what is left of
// them.
//
// THREE THINGS THE RECONCILIATION SETTLED, EACH ONE A DECISION AND NOT A
// RENAME.
//
//   1. **THE VIEWS ARE FLAT.** "DTOs are separate from CRDs" is PLAT-17.1's
//      rule and every projection this console already reads (`Connection`,
//      `Schedule`, `Backup`, `Destination`) is flat. So a D3 view carries its
//      identity and its facts at the top level, and there is no `spec`/`status`
//      pair to read. The renderers in `ui/pages/*` still speak the CUSTOM
//      RESOURCE's vocabulary, because legacy mode hands them exactly that, so
//      `ui/operation-watch.js` projects the flat view into it -- the same
//      decision `ui/client.js` made for the five older kinds, for the same
//      reason: one renderer, one vocabulary, and no page that reads one way in
//      one mode and another way in the other.
//
//   2. **THE OPEN VOCABULARIES ARE STRINGS, AND THAT IS DELIBERATE.** Eight D3
//      words are published as typed enums; the rest -- a health, an
//      availability, a diagnosis code, an effective key state -- are published
//      as `string`, because each is a word a CONTROLLER writes into a status
//      field the API passes through, and a closed enum there would turn a
//      forward-compatible status into a 500. So this client declares them
//      `str` too, and keeps the vocabularies below as RENDERING lists: a word
//      inside the set gets its colour, a word outside it is rendered VERBATIM
//      and never dropped, never rounded to a neighbour and never treated as a
//      pass. That is also what D3 section 7.7's "unknown is not valid"
//      requires, one level up.
//
//   3. **THE EVIDENCE VERDICT ARRIVES ALREADY COMBINED.** `OperationTrust.state`
//      is D3 section 2.5's own word -- `verified`, `verifiedHistorical`,
//      `untrusted`, `invalid`, `notAttempted`, `notApplicable`, `pending` --
//      computed by `logweir-api` from the controller's `result` and
//      `trust.basis`. In console mode the page reads that word instead of
//      re-deriving it, which is the whole point of a normalizing API; in legacy
//      mode there is no such word and the page keeps its own rule over the
//      custom resource's `result` + PascalCase `basis`. Two documents, two
//      rules, each reading what its own document carries.

// --------------------------------------------------------- typed vocabularies

/** `OperationStage` -- D3 section 2.5's six stages, camelCase on the wire.
 *  ABSENT when nothing was observed, which is why `queued` and `preparing` are
 *  never inferred. */
export const PROGRESS_STAGES = Object.freeze([
  "admission", "queued", "preparing", "running", "verifying", "finished",
]);

/** `TrustState` -- the evidence verdict, the RESULT and the TRUST BASIS
 *  together. This is the word the console badge rule reads in console mode. */
export const TRUST_STATES = Object.freeze([
  "pending", "verified", "verifiedHistorical", "untrusted", "invalid",
  "notAttempted", "notApplicable",
]);

/** `VerificationScopeLevel`. `complete` does not exist in v1 and is absent
 *  here on purpose. */
export const SCOPE_LEVELS = Object.freeze(["sampled", "degraded", "none"]);

/** `ApprovedPlanState` -- where the retention two-step approval stands. */
export const APPROVED_PLAN_STATES = Object.freeze([
  "notApplicable", "notRequired", "noPlan", "awaitingApproval", "approved",
  "expired", "unknown",
]);

/** `EvaluationState` -- whether a trust evaluation is believed. The API
 *  decides this, against ITS OWN clock, and publishes the instant it decided
 *  against; the page renders the answer. */
export const EVALUATION_STATES = Object.freeze(["fresh", "unknown"]);

/** `EvaluationReason` -- why it is not fresh, when it is not. */
export const EVALUATION_REASONS = Object.freeze([
  "evaluated", "notEvaluated", "generationBehind", "noEvaluationTime", "stale",
]);

/** `ConnectSyncMode` / `ConnectDeepCheck` -- the REQUEST spellings, lowercase,
 *  which are not the CRD's PascalCase (`Index`/`Full`, `None`/`ManifestDigest`
 *  /`SegmentSample`). The form sends these. */
export const CONNECT_SYNC_MODES = Object.freeze(["index", "full"]);
export const CONNECT_DEEP_CHECKS = Object.freeze(["none", "manifestDigest", "segmentSample"]);

// ------------------------------------------------------- rendering vocabularies
//
// PUBLISHED AS `string`, KEPT AS LISTS. See point 2 of this section's header:
// membership decides a badge's colour and nothing else, and a word outside a
// list is rendered as itself.

/** `ProtectionPolicyView.health`. */
export const HEALTH_STATES = Object.freeze([
  "Healthy", "AtRisk", "Stale", "Unprotected", "Unknown",
]);

/** How availability was established. `CatalogStale` is NOT a health: it is why
 *  a health could not be computed. */
export const AVAILABILITY_BASES = Object.freeze([
  "KubernetesStatus", "Catalog", "CatalogStale",
]);

/** The evidence word `lastAvailablePoint` carries. */
export const POINT_EVIDENCE = Object.freeze([
  "Valid", "ValidHistorical", "Untrusted", "NotAttempted",
]);

/** The five alert kinds (D3 section 3.3). */
export const ALERT_KINDS = Object.freeze([
  "BackupFailure", "Staleness", "ArchiveUnavailable", "RehearsalFailure", "RecoveryCompleted",
]);

/** An alert ledger entry's own state. */
export const ALERT_STATES = Object.freeze(["Open", "Resolved"]);

/** What happened to the delivery of one transition. `Suppressed` means no sink
 *  was configured for the kind, which is a configuration choice and not a
 *  failure (D3 section 3.4). */
export const DELIVERY_STATES = Object.freeze([
  "Pending", "Delivered", "Failed", "Suppressed",
]);

/** A catalog entry's availability axis (D3 section 5.4) -- the BEST of its
 *  locations. */
export const AVAILABILITY_STATES = Object.freeze([
  "Available", "Missing", "Unreadable", "Deleted", "Conflict", "UnsupportedFormat", "Partial",
]);

/** A catalog entry's verification axis -- the WORST of its locations. SEPARATE
 *  from availability, and the two are never collapsed into one column. */
export const CATALOG_VERIFICATIONS = Object.freeze([
  "Verified", "VerifiedHistorical", "UntrustedSigner", "Revoked", "Invalid",
  "NoEvidence", "NotAttempted",
]);

/** What is actually happening under a RetentionPolicy, as opposed to what
 *  `mode` asked for. */
export const ENFORCEMENT_STATES = Object.freeze([
  "RecommendationOnly", "LogweirWorker", "ExternalLifecycleDeclared",
]);

/** Each guarantee's level. `ProviderEnforcedUnverified` is never enforcement
 *  BY LOGWEIR and the page's words say so. */
export const GUARANTEE_LEVELS = Object.freeze([
  "LogweirEnforced", "ProviderEnforcedUnverified", "NotEnforced",
]);

/** `mode` -- what was asked for. */
export const RETENTION_MODES = Object.freeze(["Report", "Enforce", "ExternalLifecycle"]);

/** A key's resolved state, plus the `unknown` the API writes when the
 *  evaluation is not fresh (D3 section 7.7). */
export const EFFECTIVE_KEY_STATES = Object.freeze([
  "Active", "NotYetValid", "Expired", "Retired", "Revoked", "Unparseable", "unknown",
]);

/** Whether evidence this key signed still verifies, and on what basis. */
export const VERIFICATION_USES = Object.freeze(["Full", "Historical", "None"]);

/** A key's DECLARED lifecycle state. MONOTONIC. */
export const KEY_STATES = Object.freeze(["Active", "Retired", "Revoked"]);

/** The three usages a key may carry (D3 section 7.3). */
export const KEY_USAGES = Object.freeze([
  "EvidenceSigning", "GovernedApproval", "ConsoleConfirmation",
]);

/** Why a key was revoked. `KeyCompromise` is the one that refuses a claimed
 *  signing time and needs an independent observation. */
export const REVOCATION_REASONS = Object.freeze([
  "KeyCompromise", "Superseded", "Unspecified",
]);

/** `trust.basis`, in the CRD's OWN spelling (D3 section 12: an absent block
 *  projects to `None`). An unrecognised value is passed through by the API and
 *  is rendered as itself here. */
export const TRUST_BASES = Object.freeze([
  "Current", "Historical", "RecordedBeforeRevocation", "Unverified", "None",
]);

/** `status.evidence.verification.result` on a CUSTOM RESOURCE -- the CRD's own
 *  PascalCase vocabulary, which gained `Untrusted` with D3 section 7.4 and
 *  `Pending` with D2 section 3.9's evidence-fetch Job. It is NOT the
 *  console's lowercase `verification.state` and not `trust.state` either;
 *  three fields of two documents, and this file keeps them apart.
 *
 *  `Pending` IS NOT A VERDICT. It is written while the evidence-fetch check
 *  Job reads a run's evidence with the destination's `evidenceRead` grant,
 *  and `logweir-api` projects it as the operation state `verifying` with the
 *  trust state `pending` -- never green, never `unknown`
 *  (`ui/tests/evidence-results.spec.js`). */
export const EVIDENCE_RESULTS = Object.freeze([
  "Valid", "Invalid", "NotAttempted", "Untrusted", "Pending",
]);

/** The closed diagnosis vocabulary (D3 section 2.3). Thirteen codes; a
 *  fourteenth from a newer controller renders as itself rather than refusing
 *  the whole document. */
export const DIAGNOSIS_CODES = Object.freeze([
  "CredentialSecretNotFound", "CredentialSecretKeyMissing", "TrustBundleNotFound",
  "RunnerImagePullFailed", "RunnerImageNotPresent", "RunnerImageInvalid",
  "PodUnschedulable", "SigningKeyMissing", "VolumeMountFailed",
  "RunnerServiceAccountMissing", "PodCreateRejected", "DisruptedMidRun", "WaitingForPod",
]);

/** A diagnosis' severity. */
export const DIAGNOSIS_SEVERITIES = Object.freeze(["Warning", "Error"]);

// --------------------------------------------------------- the operation view

const D3_DIAGNOSTIC_OBJECT = shapeOf("DiagnosticObjectView", { kind: str, name: str });

const D3_DIAGNOSTIC = shapeOf(
  "DiagnosticView",
  { code: str, severity: str },
  {
    message: str, object: objectOf(D3_DIAGNOSTIC_OBJECT),
    firstSeen: str, lastSeen: str, count: int,
  },
);

const D3_RUNNER_PHASE = shapeOf("RunnerPhaseView", {}, { number: int, name: str });

/** `ProgressView`. NO `runner` BLOCK, and that is a decision rather than an
 *  omission: a Job name and a pod name are infrastructure detail, D0's visible
 *  list does not carry them, and a structural test on the API side keeps them
 *  out. What a reader needs instead is `reason`, `message` and a DIAGNOSTIC's
 *  own `object {kind, name}`, which IS published. */
const D3_PROGRESS = shapeOf(
  "ProgressView",
  {},
  {
    stage: oneOf(PROGRESS_STAGES), reason: str, message: str,
    lastTransitionTime: str, lastObservedTime: str,
    runnerPhase: objectOf(D3_RUNNER_PHASE),
  },
);

const D3_VERIFICATION_SCOPE = shapeOf(
  "VerificationScopeView",
  { level: oneOf(SCOPE_LEVELS) },
  { recordsSampled: int, recordsSampledMatching: int, recordsExpected: int },
);

const D3_NEW_TOPIC = shapeOf("CreatedTopicView", { name: str }, { partitions: int });

const D3_SAMPLE_WINDOW = shapeOf("SampleWindowView", {}, { start: str, end: str });

const D3_COMPLETION = shapeOf(
  "CompletionView",
  {},
  {
    newTopics: listOf(objectOf(D3_NEW_TOPIC)),
    recordsExpected: int, recordsRestored: int,
    recordsSampled: int, recordsSampledMatching: int,
    integrityLevel: str, sampleWindow: objectOf(D3_SAMPLE_WINDOW),
  },
);

const D3_TEARDOWN_FAILURE = shapeOf("TeardownFailureView", { topic: str, error: str });

const D3_TEARDOWN = shapeOf(
  "TeardownView",
  { deletedTruncated: bool },
  {
    attestationKey: str, deleted: listOf(str),
    failed: listOf(objectOf(D3_TEARDOWN_FAILURE)),
  },
);

const D3_CAPTURE = shapeOf("CaptureView", {}, { startedAt: str, finishedAt: str, records: int });

const D3_POLICY_REF = shapeOf("PolicyRefView", {}, { name: str, uid: str, generation: int });

/** `OperationTrust` -- the evidence verdict in TRUST terms, beside and never
 *  instead of `OperationVerification`, which is the signature result. */
const D3_TRUST = shapeOf(
  "OperationTrust",
  { state: oneOf(TRUST_STATES), basis: str },
  {
    keyState: str, policy: objectOf(D3_POLICY_REF),
    signedAt: str, signingTimeRead: str,
  },
);

const D3_READINESS = shapeOf("ReadinessView", { state: str, basis: str });

/** `OperationView` -- PLAT-17.1's `Operation` FLATTENED, so the body is the
 *  frozen sixteen plus D3's additions and a client written against either
 *  reads the one it knows. */
const D3_OPERATION = shapeOf(
  "OperationView",
  {
    kind: oneOf(OPERATION_KINDS),
    name: str, namespace: str, uid: str, resourceVersion: str,
    state: oneOf(OPERATION_STATES), terminal: bool,
    result: objectOf(OPERATION_RESULT),
    evidence: objectOf(OPERATION_EVIDENCE),
    verification: objectOf(OPERATION_VERIFICATION),
    trust: objectOf(D3_TRUST),
    verificationScope: objectOf(D3_VERIFICATION_SCOPE),
    readiness: objectOf(D3_READINESS),
    awaitingApproval: bool,
    stale: bool,
    verifiedSuccess: bool,
    conditions: listOf(objectOf(CONDITION)),
  },
  {
    createdAt: str, lastUpdatedAt: str, stateReason: str, message: str,
    stage: oneOf(PROGRESS_STAGES),
    progress: objectOf(D3_PROGRESS),
    diagnostics: listOf(objectOf(D3_DIAGNOSTIC)),
    capture: objectOf(D3_CAPTURE),
    completion: objectOf(D3_COMPLETION),
    teardown: objectOf(D3_TEARDOWN),
    targetMode: str,
  },
);

const D3_OPERATION_RESPONSE = readOnlyItem("OperationViewResponse", D3_OPERATION);

// ------------------------------------------------------- protection policies

const D3_LAST_POINT = shapeOf(
  "AvailablePointView",
  { topicsTruncated: bool },
  {
    pointId: str, backupRef: objectOf(NAME_REF), recoveryPointAt: str,
    newestRecordAt: str, ageSeconds: int, evidence: str, topics: listOf(str),
  },
);

const D3_LAST_ATTEMPT = shapeOf(
  "LastAttemptView",
  {},
  { backupRef: objectOf(NAME_REF), phase: str, reason: str, at: str },
);

const D3_SCHEDULE_HEALTH = shapeOf(
  "ScheduleHealthView",
  { name: str },
  { suspended: bool, ready: str, nextFireTime: str, lastMissedSlot: str },
);

const D3_DELIVERY = shapeOf(
  "AlertDeliveryView",
  {},
  { state: str, attempts: int, lastAttemptAt: str, lastError: str },
);

const D3_ALERT = shapeOf(
  "AlertView",
  { key: str, kind: str, state: str },
  {
    openedAt: str, resolvedAt: str, transition: int, notifiedTransition: int,
    delivery: objectOf(D3_DELIVERY),
  },
);

const D3_OBJECTIVES = shapeOf(
  "ObjectivesView",
  {
    maxRecoveryPointAgeSeconds: int, maxConsecutiveFailedRuns: int,
    requireVerifiedEvidence: bool, requireCatalogAvailability: bool,
  },
  { maxRehearsalAgeSeconds: int },
);

const D3_PROTECTS = shapeOf(
  "ProtectedSubjectView",
  { sourceRef: objectOf(NAME_REF) },
  {
    topics: listOf(str), scheduleRefs: listOf(objectOf(NAME_REF)),
    destinationRef: objectOf(NAME_REF), catalogRef: objectOf(NAME_REF),
    legacyArchive: objectOf(ARCHIVE),
  },
);

/** One notification route, as BOOLEANS. The three sinks say only whether one
 *  is CONFIGURED: no URL, no routing key and no Secret value crosses this
 *  boundary, which is why they are required and why they are not strings. */
const D3_NOTIFICATION_ROUTE = shapeOf(
  "NotificationRouteView",
  { name: str, pagerDuty: bool, webhook: bool, slack: bool },
);

const D3_NOTIFICATIONS = shapeOf(
  "NotificationsView",
  { sendResolved: bool, renotifyAfterSeconds: int },
  { routes: listOf(objectOf(D3_NOTIFICATION_ROUTE)), kinds: listOf(str) },
);

const D3_PROTECTION = shapeOf(
  "ProtectionPolicyView",
  {
    name: str, namespace: str, uid: str, resourceVersion: str,
    protects: objectOf(D3_PROTECTS), objectives: objectOf(D3_OBJECTIVES),
    evaluationIntervalSeconds: int,
  },
  {
    createdAt: str, generation: int,
    notifications: objectOf(D3_NOTIFICATIONS),
    observedGeneration: int, evaluatedAt: str,
    health: str, availabilityBasis: str,
    lastAvailablePoint: objectOf(D3_LAST_POINT),
    lastAttempt: objectOf(D3_LAST_ATTEMPT),
    consecutiveFailedRuns: int,
    lastMissedSlot: str, sinceLastFire: int, staleSince: str,
    schedules: listOf(objectOf(D3_SCHEDULE_HEALTH)),
    rehearsalLastSucceededAt: str, rehearsalLastFailedAt: str,
    rehearsalLastReason: str, rehearsalLastRestoreRef: objectOf(NAME_REF),
    alerts: listOf(objectOf(D3_ALERT)),
    conditions: listOf(objectOf(CONDITION)),
  },
);

const D3_PROTECTION_RESPONSE = readOnlyItem("ProtectionPolicyResponse", D3_PROTECTION);
const D3_PROTECTION_LIST = envelope("ProtectionPolicyList");

// ----------------------------------------------------------------- catalogs

const D3_CATALOG_COUNTS = shapeOf(
  "CatalogCountsView",
  {},
  {
    total: int, available: int, missing: int, unreadable: int, unverified: int,
    untrustedSigner: int, invalid: int, conflict: int, deleted: int, unsupportedFormat: int,
  },
);

const D3_SIGNER = shapeOf(
  "SignerView",
  { keyId: str },
  { principalHint: str, points: int, trusted: bool },
);

const D3_CATALOG_CURSOR = shapeOf("SyncCursorView", {}, { indexShard: str, complete: bool });

const D3_LAST_SYNC = shapeOf(
  "LastSyncView",
  {},
  { startedAt: str, finishedAt: str, exitCode: int, refusalReason: str },
);

const D3_HISTOGRAM_DAY = shapeOf("HistogramBucketView", { day: str, points: int });

const D3_CATALOG = shapeOf(
  "CatalogView",
  {
    name: str, namespace: str, uid: str, resourceVersion: str,
    intervalSeconds: int, mode: str, deepCheck: str, viewLimit: int,
    counts: objectOf(D3_CATALOG_COUNTS),
    truncated: bool, viewExpired: bool, viewPoints: int,
  },
  {
    createdAt: str, generation: int,
    destinationRef: objectOf(NAME_REF), legacyArchive: objectOf(ARCHIVE),
    observedGeneration: int, observedSyncRequest: str,
    syncedAt: str, viewExpiresAt: str,
    cursor: objectOf(D3_CATALOG_CURSOR),
    histogram: listOf(objectOf(D3_HISTOGRAM_DAY)),
    signers: listOf(objectOf(D3_SIGNER)),
    lastSync: objectOf(D3_LAST_SYNC),
    conditions: listOf(objectOf(CONDITION)),
  },
);

const D3_CATALOG_RESPONSE = item("CatalogResponse", D3_CATALOG);
const D3_CATALOG_LIST = envelope("CatalogList");

const D3_LOCATION = shapeOf("PointLocationView", { locationId: str, availability: str });

/** `PointView` -- ONE POINT, AS THE CATALOG MATERIALISED IT.
 *
 *  `selectable` IS READ AND NEVER RE-DERIVED. D3 section 5.4's rule --
 *  `Available` AND (`Verified` or `VerifiedHistorical`) -- is materialised by
 *  the catalog on purpose, and a page recomputing it from two parsed words
 *  would be a second implementation of the most consequential rule in the
 *  document.
 *
 *  THE FOUR PLAN-BINDING KEYS ARE REQUIRED, and that is why "Restore this
 *  point" can exist: section 5.5 step 4's plan carries
 *  `source.point {point_id, receipt_key, receipt_sha256, manifest_sha256}`, and
 *  a link that could not name them would be an offer to build a plan out of
 *  nothing. */
const D3_POINT = shapeOf(
  "PointView",
  {
    pointId: str, backupId: str, runId: str,
    receiptKey: str, receiptSha256: str,
    availability: str, verification: str, selectable: bool,
  },
  {
    recoveryPointAt: str, coveredFrom: str, coveredTo: str,
    manifestKey: str, manifestSha256: str,
    locations: listOf(objectOf(D3_LOCATION)),
    signerKeyId: str, remedy: str, formatVersion: str,
    // The controller's reached refusal on this point's own Backup, joined
    // server side (`claude/verdict-precedence`). Present only for a refusal;
    // a row that carries it is `selectable: false` whatever its two axes say.
    backupVerdict: str,
  },
);

/** The point page. `truncated` and `viewExpired` are the VIEW's facts; `page`
 *  is this REQUEST's, and the two truncations are rendered as two. */
const D3_POINT_PAGE = shapeOf(
  "PointPageResponse",
  {
    items: listOf(opaque), page: objectOf(PAGE), requestId: str,
    truncated: bool, viewExpired: bool,
  },
  {
    viewExpiresAt: str, incomplete: bool,
    // `Truncated` or `Unavailable` when the Backup-verdict join could not read
    // every Backup; absent when it did. Read as a string, so a spelling a newer
    // server adds is still "incomplete" here and never "complete".
    backupVerdictsIncomplete: str,
  },
);

const D3_SIGNER_PAGE = shapeOf(
  "SignerPageResponse",
  { items: listOf(opaque), requestId: str, fingerprintCommand: str },
  { untrustedPoints: int },
);

/** The connect-existing-archive submission (D3 section 5.5 step 1). */
const CONNECT_ARCHIVE_REQUEST = shapeOf(
  "ConnectArchiveRequest",
  { name: str, syncMode: oneOf(CONNECT_SYNC_MODES) },
  {
    destinationRef: objectOf(NAME_REF), legacyArchive: objectOf(ARCHIVE_REQUEST),
    intervalSeconds: int, deepCheck: oneOf(CONNECT_DEEP_CHECKS), viewLimit: int,
  },
);

// -------------------------------------------------------- retention policies

const D3_GUARANTEES = shapeOf(
  "GuaranteesView",
  {},
  {
    ageExpiry: str, minUsablePoints: str, activeRestoreProtection: str,
    sharedSegments: str, legalHold: str,
  },
);

const D3_CANDIDATE = shapeOf(
  "CandidateView",
  { pointId: str },
  { reason: str, recoveryPointAt: str, objects: int, bytes: int },
);

const D3_PROTECTED_POINT = shapeOf("ProtectedPointView", { pointId: str, reason: str });

const D3_SKIPPED_ENTRY = shapeOf("SkippedEntryView", { reason: str }, { pointId: str, key: str });

const D3_LAST_EVALUATION = shapeOf(
  "RetentionEvaluationView",
  { truncated: bool },
  {
    at: str, pointsEvaluated: int, candidateCount: int,
    kept: listOf(str),
    candidates: listOf(objectOf(D3_CANDIDATE)),
    protected: listOf(objectOf(D3_PROTECTED_POINT)),
    skipped: listOf(objectOf(D3_SKIPPED_ENTRY)),
    planRef: objectOf(NAME_REF), planSha256: str, planExpiresAt: str,
  },
);

const D3_FAILED_DELETION = shapeOf("FailedDeletionView", { pointId: str, code: str });

const D3_LAST_ENFORCEMENT = shapeOf(
  "EnforcementRunView",
  { deletedTruncated: bool },
  {
    runId: str, startedAt: str, finishedAt: str, planSha256: str,
    deleted: listOf(str), failed: listOf(objectOf(D3_FAILED_DELETION)),
    objectsDeleted: int, recordKey: str, recordSha256: str, exitCode: int,
  },
);

const D3_EXTERNAL_LIFECYCLE = shapeOf(
  "ExternalLifecycleView",
  { provider: str, ruleId: str, expirationDays: int, prefix: str },
);

const D3_ENFORCEMENT_SETTINGS = shapeOf(
  "EnforcementSettingsView",
  {
    credentialConfigured: bool, schedule: str, requireApprovedPlan: bool,
    planMaxAgeSeconds: int, maxDeletionsPerRun: int, maxObjectsPerRun: int,
    deadlineSeconds: int,
  },
  { approvedPlanSha256: str },
);

const D3_RETENTION = shapeOf(
  "RetentionPolicyView",
  {
    name: str, namespace: str, uid: str, resourceVersion: str,
    destinationRef: objectOf(NAME_REF), catalogRef: objectOf(NAME_REF),
    scopePrefix: str, mode: str, minUsablePoints: int, holds: int,
    approvedPlanState: oneOf(APPROVED_PLAN_STATES),
    enforcementDegraded: bool, leasedPoints: int,
  },
  {
    createdAt: str, generation: int,
    keepLast: int, keepDays: int,
    externalLifecycle: objectOf(D3_EXTERNAL_LIFECYCLE),
    enforcementSettings: objectOf(D3_ENFORCEMENT_SETTINGS),
    observedGeneration: int, enforcement: str,
    guarantees: objectOf(D3_GUARANTEES),
    lastEvaluation: objectOf(D3_LAST_EVALUATION),
    lastEnforcement: objectOf(D3_LAST_ENFORCEMENT),
    consecutiveRunFailures: int,
    conditions: listOf(objectOf(CONDITION)),
  },
);

const D3_RETENTION_RESPONSE = readOnlyItem("RetentionPolicyResponse", D3_RETENTION);
const D3_RETENTION_LIST = envelope("RetentionPolicyList");

// ------------------------------------------------------------ trust policies

const D3_PRINCIPAL = shapeOf("KeyPrincipalView", { id: str }, { display: str });

/** `TrustKeyView` -- ONE ROW PER KEY, declaration and verdict together.
 *
 *  Section 7.7 asks for one row carrying state, usages, principal, validity
 *  window and the evaluation column; a separate verdict object to join would
 *  have been two objects for one row. `effectiveState` is ALWAYS written and is
 *  `unknown` when the evaluation is not fresh, which is how "unknown is not
 *  valid" arrives already decided.
 *
 *  `spkiPem` IS NOT PUBLISHED AT ALL, and this client no longer declares it: a
 *  key id is what an operator compares out of band. */
const D3_TRUST_KEY = shapeOf(
  "TrustKeyView",
  {
    keyId: str, algorithm: str, usages: listOf(str),
    principal: objectOf(D3_PRINCIPAL), state: str,
    notBefore: str, notAfter: str, effectiveState: str,
  },
  {
    retiredAt: str, revokedAt: str,
    revocationReason: str, revocationEffectiveFrom: str,
    usableForNewSignatures: bool, usableForVerification: str,
  },
);

/** `TrustEvaluationView` -- THE FRESHNESS DECISION, MADE BY THE API AGAINST ITS
 *  OWN CLOCK AND PUBLISHED WITH THE INSTANT IT USED.
 *
 *  This is the reconciliation's most consequential single field. The page used
 *  to decide freshness itself from the `Date` header of the last answer;
 *  `logweir-api` now decides it, says which of D3 section 7.7's causes it was,
 *  and publishes `serverTime` and `freshWithinSeconds` so a reader can check
 *  the arithmetic. The page renders the answer. Legacy mode has no such view
 *  and keeps the page's own rule over the same server instant -- two documents,
 *  two rules. */
const D3_TRUST_EVALUATION = shapeOf(
  "TrustEvaluationView",
  {
    state: oneOf(EVALUATION_STATES), reason: oneOf(EVALUATION_REASONS),
    serverTime: str, freshWithinSeconds: int,
  },
  { evaluatedAt: str, ageSeconds: int },
);

const D3_NAMESPACE_CONFLICT = shapeOf(
  "NamespaceConflictView",
  { namespace: str, policies: listOf(str) },
);

const D3_TRUST_POLICY = shapeOf(
  "TrustPolicyView",
  {
    name: str, uid: str, resourceVersion: str,
    default: bool, allowedTargetClusterIds: int,
    keyCount: int, keysTruncated: bool,
    namespacesTruncated: bool, namespacesFiltered: bool,
    evaluation: objectOf(D3_TRUST_EVALUATION),
  },
  {
    createdAt: str, generation: int,
    namespaces: listOf(str), keys: listOf(objectOf(D3_TRUST_KEY)),
    observedGeneration: int, loaded: bool,
    boundNamespaces: listOf(str),
    conflicts: listOf(objectOf(D3_NAMESPACE_CONFLICT)),
    conditions: listOf(objectOf(CONDITION)),
  },
);

const D3_TRUST_RESPONSE = readOnlyItem("TrustPolicyResponse", D3_TRUST_POLICY);
const D3_TRUST_LIST = envelope("TrustPolicyList");

/** Every D3 shape this client declares, by the name the OpenAPI document
 *  publishes it under. `contract.spec.js`'s drift arm pins each one's
 *  `required` set against that document, exactly as it does for the shapes
 *  PLAT-17.1 and D2 landed; there is no separate "assumed" bucket any more,
 *  and `d3.spec.js` asserts that there is not. */
export const D3_SHAPES = Object.freeze({
  OperationView: D3_OPERATION,
  OperationViewResponse: D3_OPERATION_RESPONSE,
  ProgressView: D3_PROGRESS,
  RunnerPhaseView: D3_RUNNER_PHASE,
  DiagnosticView: D3_DIAGNOSTIC,
  DiagnosticObjectView: D3_DIAGNOSTIC_OBJECT,
  VerificationScopeView: D3_VERIFICATION_SCOPE,
  CompletionView: D3_COMPLETION,
  CreatedTopicView: D3_NEW_TOPIC,
  SampleWindowView: D3_SAMPLE_WINDOW,
  TeardownView: D3_TEARDOWN,
  TeardownFailureView: D3_TEARDOWN_FAILURE,
  CaptureView: D3_CAPTURE,
  ReadinessView: D3_READINESS,
  OperationTrust: D3_TRUST,
  PolicyRefView: D3_POLICY_REF,
  ProtectionPolicyView: D3_PROTECTION,
  ProtectionPolicyResponse: D3_PROTECTION_RESPONSE,
  ProtectionPolicyList: D3_PROTECTION_LIST,
  ProtectedSubjectView: D3_PROTECTS,
  ObjectivesView: D3_OBJECTIVES,
  NotificationsView: D3_NOTIFICATIONS,
  NotificationRouteView: D3_NOTIFICATION_ROUTE,
  AvailablePointView: D3_LAST_POINT,
  LastAttemptView: D3_LAST_ATTEMPT,
  ScheduleHealthView: D3_SCHEDULE_HEALTH,
  AlertView: D3_ALERT,
  AlertDeliveryView: D3_DELIVERY,
  CatalogView: D3_CATALOG,
  CatalogResponse: D3_CATALOG_RESPONSE,
  CatalogList: D3_CATALOG_LIST,
  CatalogCountsView: D3_CATALOG_COUNTS,
  SyncCursorView: D3_CATALOG_CURSOR,
  LastSyncView: D3_LAST_SYNC,
  HistogramBucketView: D3_HISTOGRAM_DAY,
  SignerView: D3_SIGNER,
  SignerPageResponse: D3_SIGNER_PAGE,
  PointView: D3_POINT,
  PointLocationView: D3_LOCATION,
  PointPageResponse: D3_POINT_PAGE,
  ConnectArchiveRequest: CONNECT_ARCHIVE_REQUEST,
  RetentionPolicyView: D3_RETENTION,
  RetentionPolicyResponse: D3_RETENTION_RESPONSE,
  RetentionPolicyList: D3_RETENTION_LIST,
  RetentionEvaluationView: D3_LAST_EVALUATION,
  CandidateView: D3_CANDIDATE,
  ProtectedPointView: D3_PROTECTED_POINT,
  SkippedEntryView: D3_SKIPPED_ENTRY,
  EnforcementRunView: D3_LAST_ENFORCEMENT,
  FailedDeletionView: D3_FAILED_DELETION,
  ExternalLifecycleView: D3_EXTERNAL_LIFECYCLE,
  EnforcementSettingsView: D3_ENFORCEMENT_SETTINGS,
  GuaranteesView: D3_GUARANTEES,
  TrustPolicyView: D3_TRUST_POLICY,
  TrustPolicyResponse: D3_TRUST_RESPONSE,
  TrustPolicyList: D3_TRUST_LIST,
  TrustKeyView: D3_TRUST_KEY,
  TrustEvaluationView: D3_TRUST_EVALUATION,
  KeyPrincipalView: D3_PRINCIPAL,
  NamespaceConflictView: D3_NAMESPACE_CONFLICT,
});

/** The vocabularies D3 adds, by the name the document publishes. The eight
 *  TYPED ones are pinned member-for-member by `d3.spec.js`; the rest are
 *  published as `string` and are this client's rendering lists (see point 2 of
 *  this section's header). */
export const D3_ENUMS = Object.freeze({
  OperationStage: PROGRESS_STAGES,
  TrustState: TRUST_STATES,
  VerificationScopeLevel: SCOPE_LEVELS,
  ApprovedPlanState: APPROVED_PLAN_STATES,
  EvaluationState: EVALUATION_STATES,
  EvaluationReason: EVALUATION_REASONS,
  ConnectSyncMode: CONNECT_SYNC_MODES,
  ConnectDeepCheck: CONNECT_DEEP_CHECKS,
});

/** The words a CONTROLLER writes that the document passes through as `string`.
 *  Membership picks a badge's colour; a word outside a list is rendered
 *  verbatim. */
export const D3_WORDS = Object.freeze({
  health: HEALTH_STATES,
  availabilityBasis: AVAILABILITY_BASES,
  pointEvidence: POINT_EVIDENCE,
  alertKind: ALERT_KINDS,
  alertState: ALERT_STATES,
  deliveryState: DELIVERY_STATES,
  pointAvailability: AVAILABILITY_STATES,
  pointVerification: CATALOG_VERIFICATIONS,
  retentionEnforcement: ENFORCEMENT_STATES,
  guaranteeLevel: GUARANTEE_LEVELS,
  retentionMode: RETENTION_MODES,
  effectiveKeyState: EFFECTIVE_KEY_STATES,
  verificationUse: VERIFICATION_USES,
  keyState: KEY_STATES,
  keyUsage: KEY_USAGES,
  revocationReason: REVOCATION_REASONS,
  trustBasis: TRUST_BASES,
  evidenceResult: EVIDENCE_RESULTS,
  diagnosisCode: DIAGNOSIS_CODES,
  diagnosisSeverity: DIAGNOSIS_SEVERITIES,
});

/** The D3 item responses, by the plural their route hangs off. */
const D3_ITEM_SHAPES = Object.freeze({
  "protection-policies": D3_PROTECTION_RESPONSE,
  catalogs: D3_CATALOG_RESPONSE,
  "retention-policies": D3_RETENTION_RESPONSE,
  "trust-policies": D3_TRUST_RESPONSE,
});

/** The D3 list envelopes and the item each carries, by the same plural. */
const D3_LIST_SHAPES = Object.freeze({
  "protection-policies": [D3_PROTECTION_LIST, D3_PROTECTION],
  catalogs: [D3_CATALOG_LIST, D3_CATALOG],
  "retention-policies": [D3_RETENTION_LIST, D3_RETENTION],
  "trust-policies": [D3_TRUST_LIST, D3_TRUST_POLICY],
});

/** Decodes one D3 object response. */
export function decodeD3Item(plural, value) {
  const shape = D3_ITEM_SHAPES[plural];
  if (shape === undefined) {
    throw contractFailure("D3Response", plural, "no D3 item shape is declared for this route");
  }
  return decodeWith(shape, value);
}

/** Decodes one D3 list response. */
export function decodeD3List(plural, value) {
  const pair = D3_LIST_SHAPES[plural];
  if (pair === undefined) {
    throw contractFailure("D3List", plural, "no D3 list shape is declared for this route");
  }
  return decodeListWith(pair[0], pair[1], value);
}

/** Decodes `GET .../operations/{kind}/{name}` -- the ENVELOPE, `{item,
 *  requestId}`. */
export function decodeD3Operation(value) {
  return decodeWith(D3_OPERATION_RESPONSE, value);
}

/** Decodes ONE STREAM FRAME'S operation document -- the BARE view, no envelope.
 *
 *  THE READ AND THE STREAM SEND TWO DIFFERENT SHAPES AND THAT IS THE SERVER'S
 *  DECISION, not a guess. `GET .../operations/{kind}/{name}` answers
 *  `OperationViewResponse` (`{item, requestId}`, and both are required); the
 *  stream's `operation` and `reset` frames are
 *  `serde_json::to_string(&OperationView)` -- see `send_view` in
 *  `crates/logweir-api/src/status.rs` -- so they carry the flat view and
 *  neither an `item` wrapper nor a `requestId`. A request id belongs to a
 *  REQUEST, and a stream that emitted one per frame would be publishing the
 *  same id 20 times.
 *
 *  Reading a frame with the envelope shape is not a cosmetic mismatch: it
 *  yields `undefined` for every document the stream has ever sent, so the
 *  console's live operation view renders nothing that arrives over the wire
 *  and falls silent behind its own first read. */
export function decodeD3OperationFrame(value) {
  return decodeWith(D3_OPERATION, value);
}

/** Decodes the `trust` block of `GET .../operations/{kind}/{name}`'s envelope,
 *  or answers `null` when the body carries none.
 *
 *  WHY A DETAIL VIEW NEEDS IT (CONSOLE-DETAIL-TRUST-BASIS-DROPPED). A console
 *  DETAIL view reads that route through [`decodeOperation`], which keeps the
 *  frozen sixteen fields of `Operation` and nothing else, so the verdict's
 *  `trust.basis` never reached the page: a `Valid` verdict on
 *  `RecordedBeforeRevocation` read green, as if the key were still trusted.
 *  The block is read here, by the same `OperationTrust` shape the operation
 *  view decodes, and an ABSENT block is `null` -- D3 section 12's "`trust`
 *  absent -> the pre-existing rule", which is what a server that predates D3
 *  and answers the frozen sixteen gets. A block that is present and malformed
 *  is a contract failure, like any other. */
export function decodeOperationTrust(value) {
  const item = (value !== null && typeof value === "object") ? value.item : undefined;
  if (item === null || typeof item !== "object" || item.trust === undefined ||
    item.trust === null) {
    return null;
  }
  return readShape(D3_TRUST, item.trust, { unknown: [] }, "OperationViewResponse", "item.trust");
}

/** Decodes the `verificationScope` block of `GET .../operations/{kind}/{name}`'s
 *  envelope, or answers `null` when the body carries none.
 *
 *  THE SAME GAP AS THE TRUST BLOCK ABOVE, ONE FIELD OVER (console class
 *  sweep, POC round). A console Restore DETAIL reads the operation route
 *  through [`decodeOperation`], which keeps the frozen sixteen fields and
 *  drops D3's additions -- so the History detail, which reads a Restore's
 *  scope from `status.verificationScope` or from the custom resource's
 *  `status.integrity`, found neither in the shared console and said "No
 *  verification scope was recorded for this run" beside an API that had
 *  published one. The block is read here by the operation view's own
 *  `VerificationScopeView` shape; absent is `null`, and present-but-malformed
 *  is a contract failure. */
export function decodeOperationScope(value) {
  const item = (value !== null && typeof value === "object") ? value.item : undefined;
  if (item === null || typeof item !== "object" || item.verificationScope === undefined ||
    item.verificationScope === null) {
    return null;
  }
  return readShape(D3_VERIFICATION_SCOPE, item.verificationScope, { unknown: [] },
    "OperationViewResponse", "item.verificationScope");
}

/** Decodes one page of a catalog's points. */
export function decodeCatalogPoints(value) {
  return decodeListWith(D3_POINT_PAGE, D3_POINT, value);
}

/** Decodes a catalog's signer panel. */
export function decodeCatalogSigners(value) {
  return decodeListWith(D3_SIGNER_PAGE, D3_SIGNER, value);
}

/** Checks the connect-archive body against the request shape before it is
 *  sent, exactly as every other console mutation input is checked. */
export function decodeCatalogRequest(value) {
  return decodeWith(CONNECT_ARCHIVE_REQUEST, value);
}
