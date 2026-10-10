// console-fixture.js -- a console fixture AS THE PAGE IS HANDED IT, and the
// published document every one of them is held to.
//
// WHY THIS FILE EXISTS (FX-48). A page never sees what the product API sent.
// It sees what `ui/contract.js` DECODED: declared members only, an absent
// optional as `null`, and every member the shape does not name dropped on the
// way through. A test that hands a renderer the fixture file itself is
// therefore testing a document the page is never given -- and that is exactly
// how a defect reached a live install with every row green:
// `PointTopicView` did not declare `schemaDependency`, the decoder dropped it,
// and the rows that held the "Registry not captured" note fed
// `fixtures/console/catalog-point-schema-dependency.json` straight to the
// renderers (PoC batch 6, F-1).
//
// SO THERE ARE TWO WAYS TO HOLD A CONSOLE FIXTURE, AND THEY ARE NAMED APART.
//
//   * [`wire`] is the document as the product API sends it. It is what a
//     transport stub answers with, and what a test changes one fact of BEFORE
//     it is decoded. It is never an argument to a page function.
//   * [`decoded`] is what the page's own read hands the page: the same
//     document through the decoder `ui/client.js` or `ui/operation-watch.js`
//     applies to the route that answers it.
//
// `ui/tests/contract-coverage.spec.js` holds the table below to the directory
// (a fixture with no entry fails), each document to the published schema, and
// each decode to losing nothing; and it refuses a spec that takes a member
// straight off an undecoded fixture.
//
// Not a spec: the gate's glob (`ui/tests/*.spec.js`) does not collect it. It
// reads checked-in files and reaches no network.

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  decodeApprovalPacket,
  decodeApprovalPolicy,
  decodeCadencePreview,
  decodeCancel,
  decodeCatalogPoints,
  decodeCatalogSigners,
  decodeCheckOperation,
  decodeConsoleItem,
  decodeConsoleList,
  decodeD3Item,
  decodeD3List,
  decodeD3Operation,
  decodeDestinationUsage,
  decodeDetailPage,
  decodeDiscoveryLatest,
  decodeManualBackup,
  decodeOperation,
  decodeProblem,
  decodeSession,
  decodeTopicPage,
} from "../contract.js";

const DIRECTORY = fileURLToPath(new URL("./fixtures/console/", import.meta.url));

/** The published contract, read once. */
export const OPENAPI = JSON.parse(readFileSync(
  fileURLToPath(new URL("../../schemas/logweir-api-v1.openapi.json", import.meta.url)), "utf8"));

/** Its schemas, by the name the document gives each. */
export const DEFINITIONS = OPENAPI.components.schemas;

/** Where the console fixtures live, for the arm that lists the directory. */
export const CONSOLE_FIXTURE_DIRECTORY = DIRECTORY;

/** THE DOCUMENT AS THE PRODUCT API SENDS IT -- a fresh copy every call.
 *
 *  For a transport answer, or to change one fact before the document is
 *  decoded. NEVER hand it, or a member of it, to a page function: the page is
 *  never given this. Use [`decoded`]. */
export function wire(name) {
  return JSON.parse(readFileSync(DIRECTORY + name, "utf8"));
}

/** THE `item` OF A WIRE DOCUMENT, FOR COMPOSING ANOTHER WIRE DOCUMENT: a list
 *  answer built around one item, a stream frame (the bare view the event
 *  route sends), a create's answer under another name. What it returns is
 *  still the product API's document and is still never an argument to a page
 *  function -- the name is the reminder, and it is why the lint in
 *  `contract-coverage.spec.js` can refuse `wire(name).item` outright. */
export function wireItem(name) {
  return wire(name).item;
}

// THE DECODER THE CONSOLE'S OWN READ APPLIES, BY THE SCHEMA A ROUTE ANSWERS
// WITH. One entry per published response document this client reads.
// `contract-coverage.spec.js` drives the REAL client over the transport seam
// and fails when a route's answer is decoded by a shape of another name, so
// this table cannot quietly disagree with `ui/client.js`.
const READERS = Object.freeze({
  SessionResponse: decodeSession,
  ConnectionResponse: (body) => decodeConsoleItem("connections", body),
  ConnectionList: (body) => decodeConsoleList("connections", body),
  ScheduleResponse: (body) => decodeConsoleItem("schedules", body),
  ScheduleList: (body) => decodeConsoleList("schedules", body),
  BackupResponse: (body) => decodeConsoleItem("backups", body),
  BackupList: (body) => decodeConsoleList("backups", body),
  RestoreResponse: (body) => decodeConsoleItem("restores", body),
  RestoreList: (body) => decodeConsoleList("restores", body),
  ApprovalResponse: (body) => decodeConsoleItem("approvals", body),
  ApprovalList: (body) => decodeConsoleList("approvals", body),
  ApprovalPacketResponse: decodeApprovalPacket,
  ApprovalPolicyResponse: decodeApprovalPolicy,
  CadencePreviewResponse: decodeCadencePreview,
  ManualBackupResponse: decodeManualBackup,
  Problem: decodeProblem,
  DestinationResponse: (body) => decodeConsoleItem("destinations", body),
  DestinationList: (body) => decodeConsoleList("destinations", body),
  DestinationUsageResponse: decodeDestinationUsage,
  TopicDiscoveryResponse: (body) => decodeConsoleItem("topic-discoveries", body),
  TopicDiscoveryList: (body) => decodeConsoleList("topic-discoveries", body),
  DiscoveryLatestResponse: decodeDiscoveryLatest,
  TopicPageResponse: decodeTopicPage,
  PreflightResponse: (body) => decodeConsoleItem("preflights", body),
  DetailPageResponse: decodeDetailPage,
  CheckOperationResponse: decodeCheckOperation,
  CancelResponse: decodeCancel,
  // The operation route, as the OPERATION PAGE reads it: every member the
  // document requires is required.
  OperationViewResponse: decodeD3Operation,
  ProtectionPolicyResponse: (body) => decodeD3Item("protection-policies", body),
  ProtectionPolicyList: (body) => decodeD3List("protection-policies", body),
  CatalogResponse: (body) => decodeD3Item("catalogs", body),
  CatalogList: (body) => decodeD3List("catalogs", body),
  PointPageResponse: decodeCatalogPoints,
  SignerPageResponse: decodeCatalogSigners,
  RetentionPolicyResponse: (body) => decodeD3Item("retention-policies", body),
  RetentionPolicyList: (body) => decodeD3List("retention-policies", body),
  TrustPolicyResponse: (body) => decodeD3Item("trust-policies", body),
  TrustPolicyList: (body) => decodeD3List("trust-policies", body),
});

/** The same route as a Backup or Restore DETAIL view reads it: the published
 *  view with its later members optional (`decodeOperation`). */
export const DETAIL_READER = decodeOperation;

/** EVERY FILE UNDER `fixtures/console/`, with the schema it is an instance of.
 *
 *  `schema: null` marks a file that is NOT one document of the product API,
 *  with why; nothing else may be unlisted. `detailOnly: true` marks an answer
 *  of the frozen sixteen members alone, which only the detail read accepts. */
export const CONSOLE_FIXTURES = Object.freeze({
  "session.json": { schema: "SessionResponse" },
  "session-viewer.json": { schema: "SessionResponse" },
  "session-manual-backups.json": { schema: "SessionResponse" },
  "connection.json": { schema: "ConnectionResponse" },
  "connections-list.json": { schema: "ConnectionList" },
  "schedule.json": { schema: "ScheduleResponse" },
  "schedule-policy.json": { schema: "ScheduleResponse" },
  "schedule-preset.json": { schema: "ScheduleResponse" },
  "schedule-last-slot.json": { schema: "ScheduleResponse" },
  "schedules-list.json": { schema: "ScheduleList" },
  "backup.json": { schema: "BackupResponse" },
  "backup-poc.json": { schema: "BackupResponse" },
  "backups-list.json": { schema: "BackupList" },
  "restore.json": { schema: "RestoreResponse" },
  "restore-complete-uncovered.json": { schema: "RestoreResponse" },
  "restore-creation-stopped.json": { schema: "RestoreResponse" },
  "restore-creation-unconfirmed.json": { schema: "RestoreResponse" },
  "restore-subset-pass.json": { schema: "RestoreResponse" },
  "restore-time-basis.json": { schema: "RestoreResponse" },
  "restores-list.json": { schema: "RestoreList" },
  "approval.json": { schema: "ApprovalResponse" },
  "approval-revoked-after-use.json": { schema: "ApprovalResponse" },
  "approvals-list.json": { schema: "ApprovalList" },
  "approval-packet.json": { schema: "ApprovalPacketResponse" },
  "cadence-preview-repeated.json": { schema: "CadencePreviewResponse" },
  "cadence-preview-gap.json": { schema: "CadencePreviewResponse" },
  "manual-backup.json": { schema: "ManualBackupResponse" },
  "manual-backup-replayed.json": { schema: "ManualBackupResponse" },
  "problem-validation.json": { schema: "Problem" },
  "problem-conflict.json": { schema: "Problem" },
  "problem-legacy-unknown.json": { schema: "Problem" },
  "problem-rotation-conflict.json": { schema: "Problem" },
  "problem-policy-changed.json": { schema: "Problem" },
  "destination.json": { schema: "DestinationResponse" },
  "destination-unjudged.json": { schema: "DestinationResponse" },
  "destinations-list.json": { schema: "DestinationList" },
  "destination-usage.json": { schema: "DestinationUsageResponse" },
  "discovery-unknown.json": { schema: "TopicDiscoveryResponse" },
  "discovery-limited.json": { schema: "TopicDiscoveryResponse" },
  "discovery-attested.json": { schema: "TopicDiscoveryResponse" },
  "discovery-empty.json": { schema: "TopicDiscoveryResponse" },
  "discovery-running.json": { schema: "TopicDiscoveryResponse" },
  "discovery-cancelled.json": { schema: "TopicDiscoveryResponse" },
  "discovery-failed.json": { schema: "TopicDiscoveryResponse" },
  "discovery-stale.json": { schema: "TopicDiscoveryResponse" },
  "discovery-truncated.json": { schema: "TopicDiscoveryResponse" },
  "discovery-latest.json": { schema: "DiscoveryLatestResponse" },
  "discovery-target-latest.json": { schema: "DiscoveryLatestResponse" },
  "topics-page.json": { schema: "TopicPageResponse" },
  "topics-page-last.json": { schema: "TopicPageResponse" },
  "preflight-ready.json": { schema: "PreflightResponse" },
  "preflight-pending.json": { schema: "PreflightResponse" },
  "preflight-not-ready.json": { schema: "PreflightResponse" },
  "preflight-skipped.json": { schema: "PreflightResponse" },
  "preflight-stale.json": { schema: "PreflightResponse" },
  "preflight-cancelled.json": { schema: "PreflightResponse" },
  "preflight-binding-mismatch.json": { schema: "PreflightResponse" },
  "check-operation-discovery.json": { schema: "CheckOperationResponse" },
  "cancel-discovery.json": { schema: "CancelResponse" },
  "cancel-already-terminal.json": { schema: "CancelResponse" },
  "detail-page.json": { schema: "DetailPageResponse" },
  // An answer of the frozen sixteen alone: an instance of `OperationResponse`,
  // which the document still publishes and no route answers with. Only the
  // detail read accepts it (an absent later block keeps the earlier rule).
  "operation-backup.json": { schema: "OperationResponse", detailOnly: true },
  "operation-backup-preparing.json": { schema: "OperationViewResponse" },
  "operation-unknown.json": { schema: "OperationViewResponse" },
  "operation-restore-completed.json": { schema: "OperationViewResponse" },
  "operation-restore-scratch.json": { schema: "OperationViewResponse" },
  "operation-restore-no-record-check.json": { schema: "OperationViewResponse" },
  "operation-restore-untrusted.json": { schema: "OperationViewResponse" },
  "operation-restore-complete-uncovered.json": { schema: "OperationViewResponse" },
  "operation-restore-subset-pass.json": { schema: "OperationViewResponse" },
  "protection-policy.json": { schema: "ProtectionPolicyResponse" },
  "protection-policy-unknown.json": { schema: "ProtectionPolicyResponse" },
  "protection-policies-list.json": { schema: "ProtectionPolicyList" },
  "catalog.json": { schema: "CatalogResponse" },
  "catalogs-list.json": { schema: "CatalogList" },
  "catalog-points.json": { schema: "PointPageResponse" },
  "catalog-points-states.json": { schema: "PointPageResponse" },
  "catalog-signers.json": { schema: "SignerPageResponse" },
  "retention-policy-enforce.json": { schema: "RetentionPolicyResponse" },
  "retention-policies-list.json": { schema: "RetentionPolicyList" },
  "trust-policy.json": { schema: "TrustPolicyResponse" },
  "trust-policies-list.json": { schema: "TrustPolicyList" },
  "catalog-point-schema-dependency.json": {
    schema: null,
    why: "one chain, three readers: the point record's topics, the catalog sync's entries " +
      "and the API's `PointTopicView`s for them. `pointTopics` is the API's part, and a row " +
      "reads it only inside a `PointPageResponse` it builds and decodes",
  },
});

/** The decode the console applies to the route that answers `name`, as
 *  [`Decoded`] -- `{value, unknown}`. `change` edits the wire document first. */
export function decode(name, change) {
  const entry = CONSOLE_FIXTURES[name];
  if (entry === undefined || entry.schema === null) {
    throw new Error("ui/tests/fixtures/console/" + String(name) + " is not a product-API " +
      "document this table lists; add it to CONSOLE_FIXTURES with its schema");
  }
  const body = wire(name);
  if (typeof change === "function") {
    change(body);
  }
  if (entry.detailOnly === true) {
    return DETAIL_READER(body);
  }
  return readerFor(entry.schema)(body);
}

/** WHAT THE PAGE IS HANDED for `name`: the wire document through the decoder
 *  its own read applies. `change` edits the wire document before the decode,
 *  which is where a test changes the one fact its row is about. */
export function decoded(name, change) {
  return decode(name, change).value;
}

/** The reader for a published response schema, or a throw naming it. */
export function readerFor(schema) {
  const reader = READERS[schema];
  if (reader === undefined) {
    throw new Error("no console read decodes " + String(schema) + "; add its decoder to " +
      "READERS in ui/tests/console-fixture.js");
  }
  return reader;
}

/** The schemas this client has a read for. */
export function readSchemas() {
  return Object.keys(READERS);
}

// ---------------------------------------------------------------------------
// the validator over the published document
// ---------------------------------------------------------------------------

// A `$ref` NODE MAY CARRY ITS OWN `nullable`, AND IT DOES ALL OVER THIS
// DOCUMENT: `{"$ref": "#/.../VisibilityView", "nullable": true}` is how the
// generator spells an optional nested object. Resolving the reference and
// throwing the referencing node away lost that flag, so every fixture with an
// explicit `null` in such a field read as a schema violation -- which is a
// validator that refuses documents the server is allowed to send.
export function resolve(node) {
  if (node.$ref === undefined) {
    return node;
  }
  const target = DEFINITIONS[node.$ref.split("/").pop()];
  return node.nullable === true && target !== undefined && target.nullable !== true
    ? Object.assign({}, target, { nullable: true })
    : target;
}

/** A small validator over the published document: required fields present,
 *  declared types, closed enumerations, and NO PROPERTY THE SCHEMA DOES NOT
 *  NAME. The last one is stricter than OpenAPI requires and is what makes a
 *  fixture that invented a field fail here rather than teach a decoder a
 *  field the server never sends. */
export function validate(node, value, path, findings) {
  const schema = resolve(node);
  if (schema.oneOf !== undefined) {
    const members = [];
    for (const option of schema.oneOf) {
      if (Array.isArray(option.enum)) {
        members.push(...option.enum);
      }
    }
    if (members.length > 0) {
      if (members.indexOf(value) === -1) {
        findings.push(path + ": " + JSON.stringify(value) + " is not one of " + members.join(", "));
      }
      return;
    }
  }
  if (value === null) {
    if (schema.nullable !== true) {
      findings.push(path + ": null, and the schema does not allow it");
    }
    return;
  }
  if (schema.type === "object") {
    if (typeof value !== "object" || Array.isArray(value)) {
      findings.push(path + ": expected an object");
      return;
    }
    for (const key of schema.required || []) {
      if (!Object.prototype.hasOwnProperty.call(value, key)) {
        findings.push((path === "" ? key : path + "." + key) + ": required and absent");
      }
    }
    const properties = schema.properties || {};
    for (const key of Object.keys(value)) {
      const child = path === "" ? key : path + "." + key;
      if (properties[key] === undefined) {
        findings.push(child + ": the schema names no such field");
        continue;
      }
      validate(properties[key], value[key], child, findings);
    }
    return;
  }
  if (schema.type === "array") {
    if (!Array.isArray(value)) {
      findings.push(path + ": expected an array");
      return;
    }
    value.forEach((item, i) =>
      validate(schema.items, item, path + "[" + String(i) + "]", findings));
    return;
  }
  if (schema.type === "string" && typeof value !== "string") {
    findings.push(path + ": expected a string, got " + typeof value);
  }
  if (schema.type === "boolean" && typeof value !== "boolean") {
    findings.push(path + ": expected a boolean, got " + typeof value);
  }
  if ((schema.type === "integer" || schema.type === "number") && typeof value !== "number") {
    findings.push(path + ": expected a number, got " + typeof value);
  }
}

/** The findings of `value` against the published schema `name`. */
export function schemaFindings(name, value) {
  const findings = [];
  validate({ $ref: "#/components/schemas/" + name }, value, "", findings);
  return findings;
}

// ---------------------------------------------------------------------------
// what a decode lost
// ---------------------------------------------------------------------------

/** The JSON paths `wireValue` carries that `decodedValue` does not, compared
 *  leaf by leaf -- INDEPENDENT of the decoder's own `unknown` bookkeeping, so
 *  a decoder that both dropped a member and failed to record it is still
 *  caught. An explicit `null` on the wire is absence and is never "lost". */
export function lostPaths(wireValue, decodedValue, path, out) {
  const at = path === undefined ? "" : path;
  const lost = out === undefined ? [] : out;
  if (wireValue === null || wireValue === undefined) {
    return lost;
  }
  if (Array.isArray(wireValue)) {
    if (!Array.isArray(decodedValue)) {
      lost.push(at);
      return lost;
    }
    wireValue.forEach((item, i) =>
      lostPaths(item, decodedValue[i], at + "[" + String(i) + "]", lost));
    return lost;
  }
  if (typeof wireValue === "object") {
    if (decodedValue === null || typeof decodedValue !== "object" || Array.isArray(decodedValue)) {
      lost.push(at);
      return lost;
    }
    for (const key of Object.keys(wireValue)) {
      lostPaths(wireValue[key], decodedValue[key], at.length === 0 ? key : at + "." + key, lost);
    }
    return lost;
  }
  if (decodedValue !== wireValue) {
    lost.push(at);
  }
  return lost;
}
