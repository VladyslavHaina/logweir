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
  CONSOLE_ENUMS,
  CONSOLE_REQUESTS,
  CONSOLE_SHAPES,
  CONTRACT_REASON,
  decodeCancel,
  decodeCheckOperation,
  decodeConsoleItem,
  decodeConsoleList,
  decodeDestinationUsage,
  decodeDetailPage,
  decodeDiscoveryLatest,
  decodeLegacyList,
  decodeLegacyObject,
  decodeOperation,
  decodeProblem,
  decodeRequest,
  decodeSession,
  decodeTopicPage,
  isContractFailure,
} from "../contract.js";
import { TARGET_MODES } from "../plan.js";
import { CONSOLE_FIXTURES, DEFINITIONS, schemaFindings } from "./console-fixture.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));

function fixture(name) {
  return JSON.parse(readFileSync(FIXTURES + name, "utf8"));
}

function console_(name) {
  return fixture("console/" + name);
}

// THE VALIDATOR AND THE FIXTURE TABLE LIVE IN `console-fixture.js` (FX-48). The
// table used to be written out here, by hand, and eleven of the eighty-three
// files under `fixtures/console/` were in it nowhere -- so nothing held them
// to the published schema, and one of them was not an instance of it. It is
// now ONE table, `ui/tests/contract-coverage.spec.js` holds it to the
// directory listing, and this arm reads it rather than keeping a second copy.

test("console_fixtures_are_instances_of_the_published_schema", () => {
  // AN EQUALITY, NOT A FLOOR (review F8). A floor stays green when a fixture is
  // deleted together with the row that used it, which is exactly the change
  // this arm exists to notice.
  const documents = Object.keys(CONSOLE_FIXTURES)
    .filter((name) => CONSOLE_FIXTURES[name].schema !== null);
  assert.equal(documents.length, 82,
    "the console fixture set covers PLAT-17.1, D1, D2 and D3");
  for (const name of documents) {
    const schema = CONSOLE_FIXTURES[name].schema;
    assert.ok(DEFINITIONS[schema] !== undefined, schema + " is published");
    assert.deepEqual(
      schemaFindings(schema, console_(name)),
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
  assert.ok(
    checked >= 121,
    "this arm compared " + String(checked) + " shapes; it covers every response DTO, every list " +
      "and single-item envelope, and every request body the client builds",
  );
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


// ------------------------------------- the closed sets and the request shapes

test("every_closed_set_this_client_holds_is_the_schema_s_own", () => {
  // THE ENUMS WERE HAND-COPIED, AND A HAND-COPIED LIST DRIFTS. A server that
  // adds an eleventh `OperationState` turns every console list of that kind
  // into a whole-page contract failure; nothing went red first until this arm.
  let checked = 0;
  for (const name of Object.keys(CONSOLE_ENUMS)) {
    const published = DEFINITIONS[name];
    assert.ok(published !== undefined, name + " is published in the OpenAPI document");
    assert.ok(Array.isArray(published.oneOf), name + " is an enumeration there");
    const members = [];
    for (const option of published.oneOf) {
      members.push(...(option.enum || []));
    }
    assert.deepEqual(
      CONSOLE_ENUMS[name].slice(),
      members,
      name + ": the members this client accepts are not the members the schema declares, in " +
        "that order.",
    );
    checked += 1;
  }
  assert.equal(checked, 29, "this arm compared " + String(checked) + " sets");
});

test("the_plan_module_s_target_modes_and_the_product_api_s_restore_modes_agree", () => {
  // `ui/plan.js` holds the RUNNER's `TargetMode`; `CONSOLE_ENUMS.RestoreMode`
  // holds the product API's. They are equal by agreement between two
  // components, not by construction, so the agreement is asserted rather than
  // assumed -- and `plan.js` keeps its own copy, because importing the product
  // API's contract into the plan emitter would be the wrong dependency.
  assert.deepEqual(TARGET_MODES.slice(), CONSOLE_ENUMS.RestoreMode.slice());
});

test("the_two_problem_codes_this_client_branches_on_are_published_codes", () => {
  const members = [];
  for (const option of DEFINITIONS.ProblemCode.oneOf) {
    members.push(...(option.enum || []));
  }
  for (const code of ["idempotency_conflict", "state_conflict", "validation_failed"]) {
    assert.ok(members.indexOf(code) !== -1, code + " is a published problem code");
  }
});

test("every_request_shape_this_client_builds_is_the_schema_s_own", () => {
  // THE OTHER HALF OF THE CONTRACT. `ui/client.js` hand-writes the body of
  // every product-API create; a field that becomes required there was a 422 in
  // front of an operator and not a red test.
  const routes = {
    connections: "CreateConnectionRequest",
    schedules: "CreateScheduleRequest",
    restores: "CreateRestoreRequest",
    "schedules:set-suspension": "SetSuspensionRequest",
    // D1 W7: the future-policy replace and the manual run.
    "schedules:policy": "UpdateSchedulePolicyRequest",
    backups: "CreateBackupRequest",
  };
  for (const route of Object.keys(routes)) {
    assert.ok(CONSOLE_REQUESTS[route] !== undefined, route + " has a declared request shape");
    assert.equal(CONSOLE_REQUESTS[route].name, routes[route]);
  }
  for (const name of ["ArchiveRequest", "ConnectionAuthRequest", "CreateConnectionRequest",
    "RetentionRequest", "CreateScheduleRequest", "TopicNamingRequest", "RestoreTargetRequest",
    "CreateRestoreRequest", "SetSuspensionRequest",
    "TopicSelectionRequest", "UpdateSchedulePolicyRequest", "BackupScheduleRefRequest",
    "ReadinessAcknowledgementRequest", "CreateBackupRequest"]) {
    assert.ok(CONSOLE_SHAPES[name] !== undefined, name + " is in the compared surface");
  }
});

test("saved_destination_refs_are_known_optional_restore_request_fields", () => {
  const decoded = decodeRequest("restores", {
    planBytes: "plan bytes\n",
    planHash: "sha256:" + "a".repeat(64),
    approvalRef: { name: "approval" },
    sourceArchive: { url: "logweir-destination://primary" },
    sourceDestinationRef: { name: "primary" },
    evidenceDestinationRef: { name: "evidence" },
    backupSetRef: "01JB7Z0000000000000000000B",
    pointInTime: "2026-09-11T12:00:00Z",
    target: {
      clusterRef: { name: "target" }, mode: "newTopic",
      topicNaming: { prefix: "restore-" },
    },
    deadlineSeconds: 3600,
  });
  assert.deepEqual(decoded.unknown, [],
    "both fields are declared by the browser request decoder, not tolerated as unknown");
  assert.equal(decoded.value.sourceDestinationRef.name, "primary");
  assert.equal(decoded.value.evidenceDestinationRef.name, "evidence");
});
