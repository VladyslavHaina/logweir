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
  "connectionCreate", "connectionTest", "connectionsRead", "credentialWrite",
  "destinations", "manualBackupCreate", "operationEvents", "operationsRead",
  "preflight", "restoreCreate", "restoresRead", "scheduleCreate",
  "scheduleSetSuspension", "schedulesRead", "topicDiscovery",
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
  { createdAt: str, markerTopic: str },
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

const SCHEDULE_STATUS = shapeOf(
  "ScheduleStatusView",
  {},
  {
    lastFireTime: str, nextFireTime: str, lastMissedSlot: str,
    activeBackup: str, pendingBackup: str,
    ready: objectOf(CONDITION), retentionReport: objectOf(RETENTION_REPORT),
  },
);

const SCHEDULE = shapeOf(
  "Schedule",
  {
    name: str, namespace: str, uid: str, resourceVersion: str,
    schedule: str, sourceRef: objectOf(NAME_REF), topics: listOf(str),
    archive: objectOf(ARCHIVE), suspended: bool,
    concurrencyPolicy: str, status: objectOf(SCHEDULE_STATUS),
  },
  { createdAt: str, retention: objectOf(RETENTION) },
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
  { createdAt: str, planBytes: opaque },
);

const SUBJECT_REF = shapeOf("SubjectRefView", { kind: str, name: str });

const VERIFIED_SUBJECT = shapeOf(
  "VerifiedSubjectView",
  { kind: str, name: str, namespace: str, uid: str },
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
  },
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
const RESTORE_RESPONSE = item("RestoreResponse", RESTORE);
const APPROVAL_RESPONSE = item("ApprovalResponse", APPROVAL);
const APPROVAL_PACKET_RESPONSE = readOnlyItem("ApprovalPacketResponse", APPROVAL_PACKET);
const OPERATION_RESPONSE = readOnlyItem("OperationResponse", OPERATION);


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

const CREATE_SCHEDULE_REQUEST = shapeOf(
  "CreateScheduleRequest",
  {
    schedule: str,
    sourceRef: objectOf(NAME_REF),
    topics: listOf(str),
    archive: objectOf(ARCHIVE_REQUEST),
    suspended: bool,
  },
  { concurrencyPolicy: oneOf(CONCURRENCY_POLICIES), retention: objectOf(RETENTION_REQUEST) },
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
);

const SET_SUSPENSION_REQUEST = shapeOf(
  "SetSuspensionRequest",
  { suspended: bool, expectedResourceVersion: str },
);

/** The request shapes, by the plural whose create route takes them, plus the
 *  one update. `ui/client.js` builds a body for each; the suite checks the
 *  body it built against the shape, and the shape against the schema. */
export const CONSOLE_REQUESTS = Object.freeze({
  connections: CREATE_CONNECTION_REQUEST,
  schedules: CREATE_SCHEDULE_REQUEST,
  restores: CREATE_RESTORE_REQUEST,
  "schedules:set-suspension": SET_SUSPENSION_REQUEST,
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
});

/** @returns {Decoded} */
export function decodeConsoleList(plural, value) {
  const route = CONSOLE_ROUTES[plural];
  if (route === undefined) {
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

/** @returns {Decoded} */
export function decodeApprovalPacket(value) {
  return decodeWith(APPROVAL_PACKET_RESPONSE, value);
}

/** @returns {Decoded} */
export function decodeOperation(value) {
  return decodeWith(OPERATION_RESPONSE, value);
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
