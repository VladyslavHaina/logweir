// contract.spec.js -- the typed contract, in both modes, against the schema
// the server publishes.
//
// THE DRIFT ARM IS THE POINT OF THIS FILE. `crates/logweir-api/tests/contract.rs`
// already fails when the checked-in OpenAPI document and the generator
// disagree. Nothing held the JavaScript side to that document, so a field that
// became required on the server would have reached this page as an empty cell.
// Every console decoder in `ui/contract.js` is compared with
// `schemas/logweir-api-v1.openapi.json` below, and every console fixture is
// validated against it, so the fixtures cannot drift from the contract either.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  CONSOLE_SHAPES,
  CONTRACT_REASON,
  decodeConsoleItem,
  decodeConsoleList,
  decodeLegacyList,
  decodeLegacyObject,
  decodeOperation,
  decodeProblem,
  decodeSession,
  isContractFailure,
} from "../contract.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const SCHEMA = JSON.parse(
  readFileSync(fileURLToPath(new URL("../../schemas/logweir-api-v1.openapi.json", import.meta.url)), "utf8"),
);
const DEFINITIONS = SCHEMA.components.schemas;

function fixture(name) {
  return JSON.parse(readFileSync(FIXTURES + name, "utf8"));
}

function console_(name) {
  return fixture("console/" + name);
}

function resolve(node) {
  return node.$ref === undefined ? node : DEFINITIONS[node.$ref.split("/").pop()];
}

/** A small validator over the published document: required fields present,
 *  declared types, closed enumerations, and NO PROPERTY THE SCHEMA DOES NOT
 *  NAME. The last one is stricter than OpenAPI requires and is what makes a
 *  fixture that invented a field fail here rather than teach a decoder a
 *  field the server never sends. */
function validate(node, value, path, findings) {
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
    value.forEach((item, i) => validate(schema.items, item, path + "[" + String(i) + "]", findings));
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

/** Every console fixture, with the schema it claims to be an instance of. */
const CONSOLE_FIXTURES = [
  ["session.json", "SessionResponse"],
  ["connection.json", "ConnectionResponse"],
  ["connections-list.json", "ConnectionList"],
  ["schedule.json", "ScheduleResponse"],
  ["schedules-list.json", "ScheduleList"],
  ["backup.json", "BackupResponse"],
  ["backups-list.json", "BackupList"],
  ["restore.json", "RestoreResponse"],
  ["restores-list.json", "RestoreList"],
  ["approval.json", "ApprovalResponse"],
  ["approvals-list.json", "ApprovalList"],
  ["approval-packet.json", "ApprovalPacketResponse"],
  ["operation-backup.json", "OperationResponse"],
  ["problem-validation.json", "Problem"],
  ["problem-conflict.json", "Problem"],
];

test("console_fixtures_are_instances_of_the_published_schema", () => {
  assert.ok(CONSOLE_FIXTURES.length >= 15, "the console fixture set is not empty");
  for (const [name, schema] of CONSOLE_FIXTURES) {
    assert.ok(DEFINITIONS[schema] !== undefined, schema + " is published");
    const findings = [];
    validate({ $ref: "#/components/schemas/" + schema }, console_(name), "", findings);
    assert.deepEqual(
      findings,
      [],
      "ui/tests/fixtures/console/" + name + " is not an instance of " + schema +
        ". The fixtures and the contract cannot drift: regenerate the fixture or fix the shape.",
    );
  }
});

test("every_console_decoder_requires_exactly_what_the_schema_requires", () => {
  let checked = 0;
  for (const name of Object.keys(CONSOLE_SHAPES)) {
    const published = DEFINITIONS[name];
    assert.ok(published !== undefined, name + " is published in the OpenAPI document");
    if (published.oneOf !== undefined) {
      continue;
    }
    const shape = CONSOLE_SHAPES[name];
    const requiredHere = Object.keys(shape.required).sort();
    const requiredThere = (published.required || []).slice().sort();
    assert.deepEqual(
      requiredHere,
      requiredThere,
      name + ": the fields this client treats as required are not the fields the schema " +
        "requires. A field that became required on the server and stayed optional here is the " +
        "silent field loss PLAT-18.1 exists to stop.",
    );
    const declared = Object.keys(published.properties || {});
    for (const field of requiredHere.concat(Object.keys(shape.optional))) {
      assert.ok(
        declared.indexOf(field) !== -1,
        name + "." + field + " is decoded here and is not a field of the published schema",
      );
    }
    checked += 1;
  }
  assert.ok(checked >= 25, "this arm compared " + String(checked) + " shapes, which is too few");
});

test("a_required_field_that_is_absent_is_a_contract_failure_and_not_an_empty_cell", () => {
  const body = console_("connection.json");
  delete body.item.reachability;
  assert.throws(
    () => decodeConsoleItem("connections", body),
    (error) => {
      assert.ok(isContractFailure(error), "it is a contract failure");
      assert.equal(error.reason, CONTRACT_REASON);
      assert.equal(error.status, 0, "no HTTP status describes a body that lied about its shape");
      assert.equal(error.contract.dto, "Connection");
      assert.equal(error.contract.path, "item.reachability");
      assert.match(error.message, /required by the contract and is absent/);
      return true;
    },
  );
});

test("a_required_field_of_the_wrong_type_is_a_contract_failure", () => {
  const body = console_("connections-list.json");
  body.items[0].bootstrapServers = "kafka-0.orders.svc:9093";
  assert.throws(
    () => decodeConsoleList("connections", body),
    (error) => {
      assert.ok(isContractFailure(error));
      assert.equal(error.contract.path, "items[0].bootstrapServers");
      return true;
    },
  );
});

test("an_enum_member_the_contract_does_not_declare_is_refused", () => {
  const body = console_("backups-list.json");
  body.items[0].operation.state = "almostDone";
  assert.throws(
    () => decodeConsoleList("backups", body),
    (error) => {
      assert.ok(isContractFailure(error));
      assert.match(error.message, /expected one of pending, queued/);
      return true;
    },
  );
});

test("an_unknown_field_is_tolerated_and_recorded_rather_than_refused", () => {
  const body = console_("connections-list.json");
  body.items[0].quorumSize = 3;
  body.items[0].auth.rotationDue = "2027-01-01T00:00:00Z";
  const decoded = decodeConsoleList("connections", body);
  assert.equal(decoded.value.items.length, 2, "the read succeeded");
  assert.deepEqual(
    decoded.unknown.slice().sort(),
    ["items[0].auth.rotationDue", "items[0].quorumSize"],
    "D0's rule is that an older client tolerates a newer field -- and says which ones it " +
      "ignored, so the next reader is not left to find them in a screenshot",
  );
});

test("an_absent_optional_field_reads_as_absent_and_never_as_a_failure", () => {
  const body = console_("connection.json");
  delete body.item.markerTopic;
  body.item.createdAt = null;
  const decoded = decodeConsoleItem("connections", body);
  assert.equal(decoded.value.item.markerTopic, null, "an absent optional field is null");
  assert.equal(decoded.value.item.createdAt, null, "and an explicit null is the same absence");
  assert.deepEqual(decoded.unknown, [], "neither is an unknown field");
});

test("the_session_document_decodes_with_its_grants_and_its_capability_flags", () => {
  const decoded = decodeSession(console_("session.json"));
  assert.equal(decoded.value.authenticationMode, "localAdmin");
  assert.equal(decoded.value.actor.subject, "admin");
  assert.equal(decoded.value.namespaces.length, 1);
  assert.equal(decoded.value.namespaces[0].name, "team-a");
  assert.equal(decoded.value.namespaces[0].capabilities.connectionsRead, true);
  assert.equal(
    decoded.value.namespaces[0].capabilities.approvalSubmit,
    false,
    "a domain with no route reads false rather than being discovered from an error",
  );
  assert.equal(decoded.value.csrfToken, null, "localAdmin mode carries no token");
});

test("a_problem_document_decodes_and_keeps_its_field_errors", () => {
  const decoded = decodeProblem(console_("problem-validation.json"));
  assert.equal(decoded.value.code, "validation_failed");
  assert.equal(decoded.value.status, 422);
  assert.equal(decoded.value.errors.length, 2);
  assert.equal(decoded.value.errors[0].field, "bootstrapServers[0]");
  const conflict = decodeProblem(console_("problem-conflict.json"));
  assert.equal(conflict.value.code, "idempotency_conflict");
  assert.equal(conflict.value.errors, null, "a problem with no field errors carries none");
});

test("an_operation_carries_the_evidence_and_the_recorded_verification", () => {
  const decoded = decodeOperation(console_("operation-backup.json"));
  assert.equal(decoded.value.item.kind, "backup");
  assert.equal(decoded.value.item.verification.state, "valid");
  assert.equal(decoded.value.item.result.exitCode, 0);
  assert.ok(decoded.value.item.evidence.payloadKey.length > 0);
});

// -------------------------------------------------- the legacy custom resources

test("a_custom_resource_is_validated_and_handed_back_unchanged", () => {
  const list = fixture("preview/namespaces/default/kafkaclusters.json");
  const decoded = decodeLegacyList("kafkaclusters", list);
  assert.equal(
    decoded.value,
    list,
    "the legacy decoder returns THE SAME OBJECT. Copying it field by field would be the " +
      "field loss this module exists to prevent: a key nobody listed would vanish.",
  );
  const one = decodeLegacyObject("kafkaclusters", list.items[0]);
  assert.equal(one.value, list.items[0]);
  assert.equal(one.value.status.clusterId, "MkU3NEVCTTlSM0FCQVRMQQ");
});

test("a_custom_resource_missing_a_required_spec_field_is_a_contract_failure", () => {
  const list = fixture("preview/namespaces/default/backupschedules.json");
  const broken = JSON.parse(JSON.stringify(list.items[0]));
  delete broken.spec.schedule;
  assert.throws(
    () => decodeLegacyObject("backupschedules", broken),
    (error) => {
      assert.ok(isContractFailure(error));
      assert.equal(error.contract.dto, "BackupSchedule");
      assert.equal(error.contract.path, "spec.schedule");
      return true;
    },
  );
});

test("a_custom_resource_of_another_kind_is_refused_by_name", () => {
  const cluster = fixture("preview/namespaces/default/kafkaclusters.json").items[0];
  assert.throws(
    () => decodeLegacyObject("backups", cluster),
    (error) => {
      assert.ok(isContractFailure(error));
      assert.equal(error.contract.path, "kind");
      assert.match(error.message, /expected Backup, got "KafkaCluster"/);
      return true;
    },
  );
});

test("every_shipped_legacy_fixture_satisfies_its_own_contract", () => {
  const kinds = [
    ["kafkaclusters", "preview/namespaces/default/kafkaclusters.json"],
    ["backupschedules", "preview/namespaces/default/backupschedules.json"],
    ["backups", "preview/namespaces/default/backups.json"],
    ["restores", "preview/namespaces/default/restores.json"],
    ["approvals", "preview/namespaces/default/approvals.json"],
    ["trustrosters", "preview/trustrosters.json"],
  ];
  for (const [plural, name] of kinds) {
    const decoded = decodeLegacyList(plural, fixture(name));
    assert.ok(decoded.value.items.length > 0, name + " has items");
  }
});
