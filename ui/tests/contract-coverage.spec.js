// contract-coverage.spec.js -- nothing the product API publishes is dropped on
// the way to a page, and no test hands a page a document it is never given.
//
// THE DEFECT THIS FILE IS FOR (FX-48; PoC batch 6, F-1 and F-4). A console
// page never reads a response. It reads what `ui/contract.js` DECODED, and a
// decoder copies declared members only. Two reads lost what the API had
// published, on a live install, with the whole suite green:
//
//   * `PointTopicView` did not declare `schemaDependency`, so the catalog page
//     showed no "Registry not captured" note and the restore review said "not
//     assessed" for a point every other reader called schema-dependent. The
//     rows that held the note fed the fixture file straight to the renderers.
//   * the Restore detail read the operation route -- which answers
//     `OperationViewResponse` -- with the narrower `OperationResponse`, so
//     `completion.integrityLevel` never arrived and the Integrity table read
//     "-" for a level the API had sent.
//
// THE OLD DRIFT ARMS COULD NOT SEE EITHER. `contract.spec.js` and `d3.spec.js`
// compare each shape's REQUIRED set with the document's, and refuse a member
// declared here that the document does not publish. Neither asks the opposite
// question -- what does the document publish that this client drops? -- and
// both walk two hand-written maps, which `PointTopicView` was in neither of.
//
// FIVE ARMS, EACH MECHANICAL.
//
//   1. THE DECLARATIONS. Every shape `ui/contract.js` declares (its own
//      registry: a shape cannot be declared outside it) is compared with the
//      document member for member. A member the document publishes and a shape
//      omits is named, unless [`IGNORED_MEMBERS`] says why the console
//      deliberately does not read it.
//   2. THE ROUTES. The REAL client is driven over the transport seam for every
//      route the document publishes, and each answer must be decoded by the
//      shape the document names for that route, with nothing ignored.
//   3. THE FIXTURES. Every file under `fixtures/console/` is listed with its
//      schema, is an instance of it, and survives the decoder its route uses
//      with no member lost.
//   4. THE TESTS. No spec takes a member straight off an undecoded console
//      fixture: a page function is handed what the page is handed.
//   5. THE PROJECTIONS. Nine kinds are decoded and then PROJECTED into the
//      custom resource's vocabulary, by hand, before a page sees them -- a
//      second place a member can stop. For every projected read, every member
//      the wire document carries is changed in turn, and what the page is
//      handed must change with it, unless [`NOT_HANDED_ON`] says why not.
//
// NOTHING HERE DIALS. The transport is `globalThis.fetch`, replaced for the
// length of a row, exactly as `client.spec.js` replaces it.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { apiClient, mode, resetMode, selectMode, CONSOLE } from "../client.js";
import {
  CONSOLE_SHAPES,
  D3_SHAPES,
  DECLARED_SHAPES,
  TOLERANT_SHAPES,
  bool,
  decodeWith,
  int,
  listOf,
  objectOf,
  observeDecodes,
  opaque,
  shapeOf,
  str,
} from "../contract.js";
import {
  connectArchive,
  listD3,
  readCatalogPoints,
  readCatalogSigners,
  readD3,
  readOperation,
} from "../operation-watch.js";
import { clusterBody } from "../pages/clusters.js";
import { preparePlanDocument } from "../plan.js";
import {
  CONSOLE_FIXTURES,
  CONSOLE_FIXTURE_DIRECTORY,
  DEFINITIONS,
  DETAIL_READER,
  OPENAPI,
  decode,
  lostPaths,
  readSchemas,
  readerFor,
  resolve,
  schemaFindings,
  wire,
} from "./console-fixture.js";

const TESTS = fileURLToPath(new URL("./", import.meta.url));
const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));

// ===========================================================================
// 1. the declarations
// ===========================================================================

/** MEMBERS THE DOCUMENT PUBLISHES AND THE CONSOLE DELIBERATELY DOES NOT READ,
 *  as `"Schema.member": "why"`.
 *
 *  EMPTY, AND MEANT TO STAY SHORT. An entry is a decision somebody made and
 *  wrote down: the member reaches no page, and the reason says why that is
 *  right. It is NOT where a member goes to make this arm green -- declare it
 *  in `ui/contract.js` instead, which costs one word and loses nothing. An
 *  entry for a member that is declared after all, or that the document no
 *  longer publishes, fails below: a stale exemption hides the next omission. */
const IGNORED_MEMBERS = Object.freeze({});

/** The shapes that describe a Kubernetes custom resource and not a document of
 *  the product API. Listed, so "not published" cannot become a hiding place:
 *  any other shape the document does not name fails. */
const CUSTOM_RESOURCE_SHAPES = Object.freeze([
  "ObjectMeta", "KafkaCluster.spec", "BackupSchedule.spec", "Backup.spec", "Restore.spec",
  "Approval.spec", "TrustRoster.spec", "ProtectionPolicy.spec", "RecoveryCatalog.spec",
  "RetentionPolicy.spec", "TrustPolicy.spec",
]);

function declaredMembers(shape) {
  return Object.keys(shape.required).concat(Object.keys(shape.optional));
}

function decoderOf(shape, member) {
  return Object.prototype.hasOwnProperty.call(shape.required, member)
    ? shape.required[member]
    : shape.optional[member];
}

function referenced(node) {
  return node !== null && typeof node === "object" && typeof node.$ref === "string"
    ? node.$ref.split("/").pop()
    : null;
}

/** Whether a published schema is an OBJECT -- its own properties, or a `oneOf`
 *  of objects -- rather than a closed set of words or a scalar. */
function isObjectSchema(definition) {
  if (definition === undefined || definition === null) {
    return false;
  }
  if (definition.type === "object" || definition.properties !== undefined) {
    return true;
  }
  return Array.isArray(definition.oneOf) &&
    definition.oneOf.some((option) => isObjectSchema(resolve(option)));
}

/** The members a published schema carries, with the node that describes each.
 *  A `oneOf` of objects (`CadencePreset`) publishes the union of its branches. */
function publishedMembers(definition) {
  const out = Object.create(null);
  const take = (properties) => {
    for (const key of Object.keys(properties || {})) {
      if (out[key] === undefined) {
        out[key] = properties[key];
      }
    }
  };
  take(definition.properties);
  if (Array.isArray(definition.oneOf)) {
    for (const option of definition.oneOf) {
      take((resolve(option) || {}).properties);
    }
  }
  return out;
}

/** THE COMPARISON THIS ARM IS: `"Schema.member"` for every member the document
 *  publishes on a shape's schema and the shape does not declare. */
export function undeclaredMembers(shape, definitions) {
  const definition = definitions[shape.name];
  if (definition === undefined) {
    return [];
  }
  const declared = declaredMembers(shape);
  return Object.keys(publishedMembers(definition))
    .filter((member) => declared.indexOf(member) === -1)
    .map((member) => shape.name + "." + member)
    .sort();
}

/** `"Schema.member: what"` for every declared member whose DECODER is not the
 *  kind the document publishes: an array read as one value, a nested object
 *  read as a scalar, or a nested object read by a shape of another name. A
 *  member taken whole (`opaque`) loses nothing and is never a finding. */
export function mismatchedMembers(shape, definitions) {
  const definition = definitions[shape.name];
  if (definition === undefined) {
    return [];
  }
  const published = publishedMembers(definition);
  const findings = [];
  for (const member of declaredMembers(shape)) {
    let node = published[member];
    let decoder = decoderOf(shape, member);
    if (node === undefined) {
      continue;
    }
    const where = shape.name + "." + member;
    let whole = decoder === opaque;
    while (!whole && node.type === "array") {
      if (decoder.member === undefined) {
        findings.push(where + ": the document publishes an array and the contract reads " +
          String(decoder.what));
        whole = true;
        break;
      }
      decoder = decoder.member;
      node = node.items;
      whole = decoder === opaque;
    }
    if (whole) {
      continue;
    }
    const name = referenced(node);
    if (name === null || !isObjectSchema(definitions[name])) {
      continue;
    }
    const nested = decoder.shape !== undefined
      ? decoder.shape
      : (typeof decoder.later === "function" ? decoder.later() : null);
    if (nested === null) {
      findings.push(where + ": the document publishes " + name + " and the contract reads " +
        String(decoder.what));
    } else if (nested.name !== name) {
      findings.push(where + ": the document publishes " + name + " and the contract decodes " +
        "it as " + nested.name);
    }
  }
  return findings.sort();
}

function isTolerant(shape) {
  return TOLERANT_SHAPES.some((entry) => entry.shape === shape);
}

test("every_member_the_api_publishes_is_declared_or_deliberately_ignored", () => {
  assert.ok(DECLARED_SHAPES.length >= 230,
    "the registry holds every shape ui/contract.js declares (" +
      String(DECLARED_SHAPES.length) + ")");
  const omitted = [];
  let compared = 0;
  for (const shape of DECLARED_SHAPES) {
    if (DEFINITIONS[shape.name] === undefined) {
      continue;
    }
    compared += 1;
    omitted.push(...undeclaredMembers(shape, DEFINITIONS));
  }
  assert.ok(compared >= 215, "this arm compared " + String(compared) + " shapes with the document");

  const unexplained = omitted.filter((member) => IGNORED_MEMBERS[member] === undefined);
  assert.deepEqual(
    unexplained,
    [],
    "schemas/logweir-api-v1.openapi.json publishes these members and ui/contract.js does not " +
      "declare them, so the decoder DROPS what the API sends and the page that reads one " +
      "renders as if it were absent. Declare each in its shape (required or optional, as the " +
      "document says), or name it in IGNORED_MEMBERS with the reason no page reads it:\n" +
      unexplained.join("\n"),
  );

  // AN EXEMPTION THAT NO LONGER EXEMPTS ANYTHING IS REMOVED, NOT KEPT.
  for (const member of Object.keys(IGNORED_MEMBERS)) {
    assert.ok(omitted.indexOf(member) !== -1,
      "IGNORED_MEMBERS names " + member + ", which is declared after all or is no longer " +
        "published: remove the entry");
    assert.ok(String(IGNORED_MEMBERS[member]).length >= 20,
      "IGNORED_MEMBERS." + member + " says why in a sentence");
  }
});

test("every_declared_shape_is_a_published_schema_held_to_its_required_set", () => {
  const unpublished = [];
  for (const shape of DECLARED_SHAPES) {
    const definition = DEFINITIONS[shape.name];
    if (definition === undefined) {
      unpublished.push(shape.name);
      continue;
    }
    // (a) NO MEMBER IS INVENTED HERE.
    const published = publishedMembers(definition);
    for (const member of declaredMembers(shape)) {
      assert.ok(published[member] !== undefined,
        shape.name + "." + member + " is decoded here and is not a member of the published schema");
    }
    if (definition.oneOf !== undefined) {
      continue;
    }
    // (b) THE REQUIRED SET IS THE DOCUMENT'S -- for every shape in the
    // registry, not only the ones somebody added to a map.
    const requiredHere = Object.keys(shape.required).sort();
    const requiredThere = (definition.required || []).slice().sort();
    if (isTolerant(shape)) {
      for (const member of requiredHere) {
        assert.ok(requiredThere.indexOf(member) !== -1,
          shape.name + " (tolerant): " + member + " is required here and not by the document");
      }
      continue;
    }
    assert.deepEqual(requiredHere, requiredThere,
      shape.name + ": the members this client requires are not the members the schema requires");
    // (c) AND IT IS IN ONE OF THE TWO MAPS, so the older drift arms see it too.
    assert.ok(CONSOLE_SHAPES[shape.name] === shape || D3_SHAPES[shape.name] === shape,
      shape.name + " is declared and is in neither CONSOLE_SHAPES nor D3_SHAPES");
  }
  assert.deepEqual(unpublished.sort(), CUSTOM_RESOURCE_SHAPES.slice().sort(),
    "the only shapes the document does not publish are the custom resources' own");
  assert.deepEqual(TOLERANT_SHAPES.map((entry) => entry.shape.name),
    ["OperationView", "OperationViewResponse"],
    "one view is read a second time with a narrower required set, with its envelope");
  for (const entry of TOLERANT_SHAPES) {
    assert.ok(entry.why.length > 40, entry.shape.name + " says why");
  }
});

test("a_nested_member_is_decoded_by_the_shape_the_document_names_for_it", () => {
  const findings = [];
  for (const shape of DECLARED_SHAPES) {
    findings.push(...mismatchedMembers(shape, DEFINITIONS));
  }
  assert.deepEqual(findings, [],
    "a member declared with the wrong decoder loses what is under it:\n" + findings.join("\n"));
});

test("the_comparison_names_a_member_a_shape_drops", () => {
  // A GUARD WITHOUT A MUTANT IS NOT A GUARD, SO THE MUTANT IS A ROW. This is
  // `PointTopicView` exactly as `ui/contract.js` declared it before FX-48.
  const before = shapeOf("PointTopicView", { name: str, applyRoute: str },
    { partitions: int, replicationFactor: int, configCoverage: str, owner: str });
  assert.deepEqual(undeclaredMembers(before, DEFINITIONS), ["PointTopicView.schemaDependency"],
    "the comparison names the member the old declaration dropped");
  const now = D3_SHAPES.PointTopicView;
  assert.deepEqual(undeclaredMembers(now, DEFINITIONS), []);

  // ... and a nested member read by a shape of another name, or as a scalar.
  const misnamed = shapeOf("PointTopicView", { name: str, applyRoute: str },
    { schemaDependency: objectOf(D3_SHAPES.PointLocationView) });
  assert.deepEqual(mismatchedMembers(misnamed, DEFINITIONS), [
    "PointTopicView.schemaDependency: the document publishes PointSchemaDependencyView and " +
      "the contract decodes it as PointLocationView",
  ]);
  const flattened = shapeOf("PointView", { pointId: str }, { topics: str, locations: listOf(str) });
  assert.deepEqual(mismatchedMembers(flattened, DEFINITIONS), [
    "PointView.locations: the document publishes PointLocationView and the contract reads a string",
    "PointView.topics: the document publishes an array and the contract reads a string",
  ]);

  // ... and a decode that lost a member is seen WITHOUT the decoder's own
  // record of what it ignored.
  const thin = shapeOf("PointSchemaDependencyView", { verdict: str }, { basis: str });
  const row = {
    verdict: "schemaDependent", basis: "complete", sides: ["value"], schemaIds: [7, 42],
  };
  const read = decodeWith(thin, row);
  assert.deepEqual(lostPaths(row, read.value), ["sides", "schemaIds"]);
  assert.deepEqual(read.unknown, ["sides", "schemaIds"]);
  assert.equal(bool.what, "a boolean", "the kit's scalars say what they are");
});

// ===========================================================================
// 3. the fixtures (before the routes: the routes serve them)
// ===========================================================================

test("every_console_fixture_is_listed_is_an_instance_and_survives_its_decoder", () => {
  const files = readdirSync(CONSOLE_FIXTURE_DIRECTORY).filter((f) => f.endsWith(".json")).sort();
  assert.deepEqual(Object.keys(CONSOLE_FIXTURES).sort(), files,
    "every file under ui/tests/fixtures/console/ is listed in CONSOLE_FIXTURES with its " +
      "schema, and nothing is listed that is not there");

  let documents = 0;
  for (const name of files) {
    const entry = CONSOLE_FIXTURES[name];
    if (entry.schema === null) {
      assert.ok(String(entry.why).length >= 40, name + " says why it is not one API document");
      continue;
    }
    documents += 1;
    assert.ok(DEFINITIONS[entry.schema] !== undefined, entry.schema + " is published");
    assert.deepEqual(schemaFindings(entry.schema, wire(name)), [],
      "ui/tests/fixtures/console/" + name + " is not an instance of " + entry.schema);

    // THROUGH THE DECODER ITS ROUTE USES, AND NOTHING IS LOST.
    const seen = [];
    observeDecodes((event) => seen.push(event));
    let read;
    try {
      read = decode(name);
    } finally {
      observeDecodes(null);
    }
    assert.deepEqual(read.unknown, [],
      name + ": the decoder IGNORED these members of a document the API may send");
    assert.deepEqual(lostPaths(wire(name), read.value), [],
      name + ": these members did not survive the decode");
    assert.equal(seen.length, 1, name + " is decoded once");
    assert.equal(seen[0].shape,
      entry.detailOnly === true ? "OperationViewResponse" : entry.schema,
      name + " is decoded by the shape named for its schema");

    // A RUN'S OPERATION ANSWER HAS TWO READERS, and it survives both.
    if (entry.schema === "OperationViewResponse") {
      const detail = DETAIL_READER(wire(name));
      assert.deepEqual(detail.unknown, [], name + ", as a detail view reads it");
      assert.deepEqual(lostPaths(wire(name), detail.value), [],
        name + ": these members did not survive the detail decode");
    }
  }
  // AN EQUALITY, NOT A FLOOR: a fixture deleted with the row that used it is
  // exactly the change this arm exists to notice.
  assert.equal(documents, 82, "the console fixture set");
  assert.equal(files.length, 83);
});

test("a_document_of_the_first_sixteen_members_alone_is_refused_by_the_operation_page", () => {
  // The control of `detailOnly`: the operation PAGE requires what the document
  // requires, so the same answer the detail read tolerates is refused there
  // -- by name, never rendered half empty.
  assert.throws(() => readerFor("OperationViewResponse")(wire("operation-backup.json")),
    /required by the contract and is absent/);
  const detail = decode("operation-backup.json");
  assert.equal(detail.value.item.trust, null, "an absent block is absent, never a default");
  assert.equal(detail.value.item.completion, null);
});

// ===========================================================================
// 2. the routes
// ===========================================================================

const NS = "team-a";

/** The routes the document publishes and this console never reads, with why.
 *  Anything else the document publishes with a JSON answer must be driven
 *  below. */
const ROUTES_NOT_READ = Object.freeze({
  "GET /healthz": "a probe endpoint for the kubelet, not a console read",
  "GET /readyz": "a probe endpoint for the kubelet, not a console read",
  "GET /api/v1/namespaces":
    "the console takes its namespaces from the session document's grants and lists none",
  "GET /api/v1/namespaces/{ns}/rehearsal-schedules":
    "no console page reads rehearsal schedules; the protection page shows the last rehearsal " +
    "from the protection policy's own view",
  "GET /api/v1/namespaces/{ns}/rehearsal-schedules/{name}":
    "no console page reads rehearsal schedules",
});

/** TWO ROUTES ANSWER A SECOND DOCUMENT THE PATH'S ONE `200` SCHEMA CANNOT
 *  NAME. The document publishes both schemas and describes both answers in
 *  prose (`docs/api.md`); what it cannot do is hang two schemas on one status.
 *  Keyed by the route and the request that selects the variant. */
const ROUTE_VARIANTS = Object.freeze({
  "GET /api/v1/namespaces/{ns}/connections/{name}/topic-discoveries ?latest=true": {
    schema: "DiscoveryLatestResponse",
    why: "with latest=true the route answers the two slots instead of a page",
  },
  "GET /api/v1/namespaces/{ns}/operations/{kind}/{name} kind=check": {
    schema: "CheckOperationResponse",
    why: "a discovery or a preflight is a transient check and answers a different document",
  },
});

/** The variant of a route's answer the console never asks for, with why. */
const VARIANTS_NOT_READ = Object.freeze({
  "GET /api/v1/namespaces/{ns}/connections/{name}/topic-discoveries":
    "the console reads this route with latest=true only; no page pages through a " +
    "connection's discoveries",
});

const SUCCESS = ["200", "201", "202"];

/** Every operation the document publishes with a JSON success schema:
 *  `{op, template, method, schema, status, request, pattern, literal}` --
 *  `request` is the schema of the body the route takes, or `null`. */
function publishedOperations() {
  const out = [];
  for (const template of Object.keys(OPENAPI.paths)) {
    const item = OPENAPI.paths[template];
    for (const method of Object.keys(item)) {
      const operation = item[method];
      if (operation === null || typeof operation !== "object" ||
        operation.responses === undefined) {
        continue;
      }
      let schema = null;
      let status = null;
      for (const code of SUCCESS) {
        const content = ((operation.responses[code] || {}).content || {})["application/json"];
        const name = referenced((content || {}).schema);
        if (name !== null) {
          assert.ok(schema === null || schema === name,
            template + ": two success statuses publish two schemas");
          schema = name;
          status = status === null ? Number(code) : status;
        }
      }
      if (schema === null) {
        continue;
      }
      const sends = referenced(((((operation.requestBody || {}).content || {})[
        "application/json"]) || {}).schema);
      const literal = template.replace(/\{[^}]+\}/g, "");
      const pattern = new RegExp("^" + template.split(/(\{[^}]+\})/).map((part) =>
        (/^\{[^}]+\}$/.test(part)
          ? "[^/]+"
          : part.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))).join("") + "$");
      out.push({
        op: method.toUpperCase() + " " + template, template: template,
        method: method.toUpperCase(), schema: schema, status: status, request: sends,
        pattern: pattern, literal: literal.length,
      });
    }
  }
  return out;
}

const OPERATIONS = publishedOperations();

/** The published operation a request addresses: the template with the most
 *  literal text among those that match, so `.../destinations/x:test` is the
 *  test route and not a destination named `x:test`. */
function operationFor(method, url) {
  const path = String(url).split("?")[0];
  let best = null;
  for (const operation of OPERATIONS) {
    if (operation.method === method && operation.pattern.test(path) &&
      (best === null || operation.literal > best.literal)) {
      best = operation;
    }
  }
  return best;
}

/** The key of the answer a request selects: the route, plus the variant. */
function answerKey(operation, url) {
  const text = String(url);
  if (operation.template.endsWith("/connections/{name}/topic-discoveries") &&
    operation.method === "GET" && /[?&]latest=true(&|$)/.test(text)) {
    return operation.op + " ?latest=true";
  }
  if (operation.template.endsWith("/operations/{kind}/{name}")) {
    const kind = text.split("?")[0].split("/").slice(-2)[0];
    if (kind === "discovery" || kind === "preflight") {
      return operation.op + " kind=check";
    }
  }
  return operation.op;
}

/** A session that may read and do everything in `team-a`, so no row below is
 *  refused before the network by a capability flag. */
function everythingGranted() {
  const document = wire("session.json");
  for (const flag of Object.keys(document.capabilities)) {
    document.capabilities[flag] = true;
  }
  for (const grant of document.namespaces) {
    for (const flag of Object.keys(grant.capabilities)) {
      grant.capabilities[flag] = true;
    }
  }
  return document;
}

const APPROVAL_POLICY = Object.freeze({
  requestId: "01JB7ZAPPROVALPOLICY0000000",
  item: {
    namespace: NS, name: "prod-governed", mode: "governed", legacy: false,
    operatorMode: "strict", basis: "binding", requireDistinctPrincipal: true,
    installationDigest: "sha256:" + "b".repeat(64), ordinaryConfirmationAvailable: false,
    ticketRequired: true, maxAgeSeconds: 900, digest: "sha256:" + "c".repeat(64),
    confirmationKeyId: "d".repeat(64),
  },
});

/** WHAT EACH ROUTE IS ANSWERED WITH HERE. A fixture name, or `{body, schema}`
 *  for the one route with no checked-in fixture. Keyed as [`answerKey`]. */
const ANSWERS = Object.freeze({
  "GET /api/v1/session": { body: everythingGranted },
  "GET /api/v1/namespaces/{ns}/connections": "connections-list.json",
  "POST /api/v1/namespaces/{ns}/connections": "connection.json",
  "GET /api/v1/namespaces/{ns}/connections/{name}": "connection.json",
  "GET /api/v1/cadence-previews": "cadence-preview-repeated.json",
  "GET /api/v1/namespaces/{ns}/schedules": "schedules-list.json",
  "POST /api/v1/namespaces/{ns}/schedules": "schedule.json",
  "GET /api/v1/namespaces/{ns}/schedules/{name}": "schedule-policy.json",
  "PUT /api/v1/namespaces/{ns}/schedules/{name}": "schedule-policy.json",
  "POST /api/v1/namespaces/{ns}/schedules/{name}:set-suspension": "schedule.json",
  "GET /api/v1/namespaces/{ns}/backups": "backups-list.json",
  "POST /api/v1/namespaces/{ns}/backups": "manual-backup.json",
  "GET /api/v1/namespaces/{ns}/backups/{name}": "backup.json",
  "GET /api/v1/namespaces/{ns}/restores": "restores-list.json",
  "POST /api/v1/namespaces/{ns}/restores": "restore.json",
  "GET /api/v1/namespaces/{ns}/restores/{name}": "restore.json",
  "POST /api/v1/namespaces/{ns}/restores/{name}/approval": "approval.json",
  "GET /api/v1/namespaces/{ns}/approval-policy": { body: () => structuredClone(APPROVAL_POLICY) },
  "GET /api/v1/namespaces/{ns}/approvals": "approvals-list.json",
  "GET /api/v1/namespaces/{ns}/approvals/{name}": "approval.json",
  "GET /api/v1/namespaces/{ns}/approvals/{name}/packet": "approval-packet.json",
  "GET /api/v1/namespaces/{ns}/destinations": "destinations-list.json",
  "POST /api/v1/namespaces/{ns}/destinations": "destination.json",
  "POST /api/v1/namespaces/{ns}/destinations:from-legacy": "destination.json",
  "GET /api/v1/namespaces/{ns}/destinations/{name}": "destination.json",
  "POST /api/v1/namespaces/{ns}/destinations/{name}:update-access": "destination.json",
  "POST /api/v1/namespaces/{ns}/destinations/{name}:test": "preflight-pending.json",
  "GET /api/v1/namespaces/{ns}/destinations/{name}/usage": "destination-usage.json",
  "GET /api/v1/namespaces/{ns}/connections/{name}/topic-discoveries ?latest=true":
    "discovery-latest.json",
  "POST /api/v1/namespaces/{ns}/connections/{name}/topic-discoveries": "discovery-running.json",
  "GET /api/v1/namespaces/{ns}/topic-discoveries/{id}": "discovery-attested.json",
  "POST /api/v1/namespaces/{ns}/topic-discoveries/{id}:cancel": "cancel-discovery.json",
  "GET /api/v1/namespaces/{ns}/topic-discoveries/{id}/topics": "topics-page.json",
  "POST /api/v1/namespaces/{ns}/preflights": "preflight-pending.json",
  "GET /api/v1/namespaces/{ns}/preflights/{id}": "preflight-ready.json",
  "POST /api/v1/namespaces/{ns}/preflights/{id}:cancel": "cancel-already-terminal.json",
  "GET /api/v1/namespaces/{ns}/preflights/{id}/details": "detail-page.json",
  "GET /api/v1/namespaces/{ns}/operations/{kind}/{name}": "operation-restore-completed.json",
  "GET /api/v1/namespaces/{ns}/operations/{kind}/{name} kind=check":
    "check-operation-discovery.json",
  "GET /api/v1/namespaces/{ns}/protection-policies": "protection-policies-list.json",
  "GET /api/v1/namespaces/{ns}/protection-policies/{name}": "protection-policy.json",
  "GET /api/v1/namespaces/{ns}/catalogs": "catalogs-list.json",
  "POST /api/v1/namespaces/{ns}/catalogs": "catalog.json",
  "GET /api/v1/namespaces/{ns}/catalogs/{name}": "catalog.json",
  "GET /api/v1/namespaces/{ns}/catalogs/{name}/points": "catalog-points.json",
  "GET /api/v1/namespaces/{ns}/catalogs/{name}/signers": "catalog-signers.json",
  "GET /api/v1/namespaces/{ns}/retention-policies": "retention-policies-list.json",
  "GET /api/v1/namespaces/{ns}/retention-policies/{name}": "retention-policy-enforce.json",
  "GET /api/v1/trust-policies": "trust-policies-list.json",
  "GET /api/v1/trust-policies/{name}": "trust-policy.json",
});

const ACCESS = Object.freeze({
  archiveWrite: { mode: "secretKeys", secret: { existing: { name: "s3" } } },
});

/** The custom-resource-shaped objects the pages build for the three creates
 *  that go through `requestBody`, as the pages build them. */
function clusterObject() {
  return clusterBody({
    name: "orders-prod", servers: "kafka-0.orders.svc:9093", role: "source",
    mode: "scramSha512", username: "logweir-reader", secret: "", tls: true,
    enteredPassword: "typed-once-ui-fixture",
  }, { console: true });
}

function scheduleObject() {
  return {
    apiVersion: "logweir.dev/v1alpha1", kind: "BackupSchedule",
    metadata: { name: "orders-hourly" },
    spec: {
      schedule: "0 * * * *", sourceRef: { name: "orders-prod" }, topics: ["orders"],
      archive: { url: "s3://kafka-backups/orders", secretRef: { name: "logweir-s3" } },
      suspend: false, concurrencyPolicy: "Forbid", retention: { keepLast: 3 },
    },
  };
}

function restoreObject(reviewed) {
  return {
    apiVersion: "logweir.dev/v1alpha1", kind: "Restore",
    metadata: { name: reviewed.restoreName },
    spec: {
      planBytes: reviewed.bytes, approvalRef: { name: reviewed.approvalName },
      sourceArchive: { url: "s3://kafka-backups/orders" },
      backupSetRef: "01JB7Z0000000000000000000B", pointInTime: "2026-09-11T12:00:00Z",
      target: { clusterRef: { name: "orders-scratch" }, mode: "scratch",
        topicNaming: { prefix: "drill-" } },
      deadlineSeconds: 3600, coverage: "complete", completeMaxRecords: 1000,
      sourceDestinationRef: { name: "primary" }, evidenceDestinationRef: { name: "primary" },
    },
    // The three members of the REQUEST that are not members of `Restore.spec`.
    ticket: "CHG-1042",
    topicMapping: [{ source: "orders", target: "drill-orders" }],
    originalNameConfirmation: { typedTopics: ["orders"] },
  };
}

const CATALOG_REQUEST = Object.freeze({
  name: "primary", syncMode: "index", destinationRef: { name: "primary" },
});

/** EVERY CONSOLE READ, AS THE PAGES MAKE IT: the client's own method, with
 *  arguments its own checks accept. Each row is one call; the transport says
 *  which routes it addressed. */
async function everyRead() {
  const api = apiClient();
  const reviewed = await preparePlanDocument(
    JSON.parse(readFileSync(FIXTURES + "plan-fields.json", "utf8")));
  return [
    () => api.list(NS, "kafkaclusters"),
    () => api.get(NS, "kafkaclusters", "orders-prod"),
    () => api.create(NS, "kafkaclusters", clusterObject()),
    () => api.previewCadence({ schedule: "0 * * * *" }),
    () => api.list(NS, "backupschedules"),
    () => api.get(NS, "backupschedules", "orders-hourly"),
    () => api.create(NS, "backupschedules", scheduleObject()),
    () => api.editSchedulePolicy(NS, "orders-hourly", {
      expectedGeneration: 1, schedule: "0 * * * *", suspended: false,
      topicSelection: { topics: ["orders"] },
    }),
    () => api.patchSuspend(NS, "orders-hourly", true),
    () => api.list(NS, "backups"),
    () => api.get(NS, "backups", "orders-hourly-20260912-080000"),
    () => api.runBackupNow(NS, { scheduleRef: { name: "orders-hourly" } },
      "intent-0123456789abcdef"),
    () => api.list(NS, "restores"),
    () => api.get(NS, "restores", "orders-drill-20260911"),
    () => api.create(NS, "restores", restoreObject(reviewed)),
    () => api.submitGovernedApproval(NS, "orders-drill-20260911", "sidecar bytes"),
    () => api.approvalPolicy(NS),
    () => api.list(NS, "approvals"),
    () => api.get(NS, "approvals", "approval-x"),
    () => api.destinations(NS),
    () => api.destination(NS, "primary"),
    () => api.createDestination(NS, {
      name: "primary",
      storage: { provider: "s3", bucket: "kafka-backups", addressing: "pathStyle" },
      transport: { security: "tls" }, access: ACCESS,
    }),
    () => api.destinationFromLegacy(NS, { name: "primary", access: ACCESS }),
    () => api.updateDestinationAccess(NS, "primary", { expectedGeneration: 1, access: ACCESS }),
    () => api.testDestination(NS, "primary", {}),
    () => api.destinationUsage(NS, "primary"),
    () => api.latestDiscoveries(NS, "orders-prod"),
    () => api.startDiscovery(NS, "orders-prod", {}),
    () => api.discovery(NS, "disc-1"),
    () => api.discoveryTopics(NS, "disc-1"),
    () => api.cancelDiscovery(NS, "disc-1"),
    () => api.startPreflight(NS, {
      operation: "sourceConnection", sourceConnection: { connectionRef: "orders-prod" },
    }),
    () => api.preflight(NS, "pf-1"),
    () => api.preflightDetails(NS, "pf-1"),
    () => api.cancelPreflight(NS, "pf-1"),
    () => api.checkOperation(NS, "discovery", "disc-1"),
    () => readOperation(NS, "restore", "orders-drill-20260911"),
    () => listD3("protection", NS),
    () => readD3("protection", NS, "orders"),
    () => listD3("catalog", NS),
    () => readD3("catalog", NS, "primary"),
    () => connectArchive(NS, CATALOG_REQUEST, "intent-fedcba9876543210"),
    () => readCatalogPoints(NS, "primary", { limit: 200 }),
    () => readCatalogSigners(NS, "primary"),
    () => listD3("retention", NS),
    () => readD3("retention", NS, "primary-retention"),
    () => listD3("trust", NS),
    () => readD3("trust", NS, "default"),
  ];
}

test("every_route_is_decoded_with_the_shape_the_document_publishes_and_drops_nothing", async () => {
  const served = [];
  const unpublished = [];
  const badRequests = [];
  const badAnswers = [];
  let requests = 0;
  const original = globalThis.fetch;
  globalThis.fetch = (url, init) => {
    const method = String((init || {}).method || "GET").toUpperCase();
    const operation = operationFor(method, url);
    if (operation === null) {
      unpublished.push(method + " " + String(url));
      return Promise.reject(new Error("no published route: " + method + " " + String(url)));
    }
    const key = answerKey(operation, url);
    const answer = ANSWERS[key];
    if (answer === undefined) {
      unpublished.push("no answer in this row for " + key);
      return Promise.reject(new Error("no answer for " + key));
    }
    // WHAT THE CONSOLE SENT IS A REQUEST THE DOCUMENT ACCEPTS: every body is
    // held to the route's published request schema, member for member.
    if (operation.request !== null && typeof (init || {}).body === "string") {
      for (const finding of schemaFindings(operation.request, JSON.parse(init.body))) {
        badRequests.push(key + " sent a body that is not a " + operation.request + ": " +
          finding);
      }
      requests += 1;
    }
    const body = typeof answer === "string" ? wire(answer) : answer.body();
    // AND WHAT THE ROW ANSWERS WITH IS A DOCUMENT THE ROUTE MAY SEND: every
    // answer, the two written out in this file included, is an instance of
    // the schema the document publishes for the route (or for its variant).
    const publishes = ROUTE_VARIANTS[key] === undefined
      ? operation.schema
      : ROUTE_VARIANTS[key].schema;
    for (const finding of schemaFindings(publishes, body)) {
      badAnswers.push(key + " is answered here with a body that is not a " + publishes + ": " +
        finding);
    }
    const text = JSON.stringify(body);
    served.push({ key: key, operation: operation, text: text, decodedBy: [] });
    return Promise.resolve({
      ok: true, status: operation.status, text: () => Promise.resolve(text),
    });
  };
  const events = [];
  observeDecodes((event) => events.push(event));
  try {
    // THE SESSION ROUTE FIRST, THROUGH THE BOOT PROBE ITSELF: the mode is
    // decided by the document the transport answers with.
    resetMode();
    await selectMode();
    assert.equal(mode(), CONSOLE, "the page is behind the product API");
    for (const read of await everyRead()) {
      await read();
    }
  } finally {
    observeDecodes(null);
    globalThis.fetch = original;
    resetMode();
  }

  assert.deepEqual(unpublished, [],
    "the console addressed a route the document does not publish, or this row has no answer " +
      "for one it does");
  assert.deepEqual(badAnswers, [],
    "this row answers a route with a body the document does not publish for it:\n" +
      badAnswers.join("\n"));
  assert.deepEqual(badRequests, [],
    "a body the console builds must be an instance of the request schema the document " +
      "publishes for the route it is sent to:\n" + badRequests.join("\n"));
  assert.equal(requests, 14, "the row held every create and update body the console builds");

  // EACH ANSWER, AND THE DECODE THAT READ IT. An answer is matched to its
  // decode by the document itself: the transport served these exact bytes and
  // the decoder was handed their parse.
  for (const event of events) {
    const text = JSON.stringify(event.value);
    for (const answer of served) {
      if (answer.text === text) {
        answer.decodedBy.push(event);
      }
    }
  }
  const findings = [];
  for (const answer of served) {
    const variant = ROUTE_VARIANTS[answer.key];
    const published = variant === undefined ? answer.operation.schema : variant.schema;
    if (answer.decodedBy.length === 0) {
      findings.push(answer.key + ": the answer was not decoded by any contract shape");
      continue;
    }
    for (const event of answer.decodedBy) {
      if (event.shape !== published) {
        findings.push(answer.key + ": the document publishes " + published + " and the console " +
          "decoded the answer as " + event.shape);
      }
      if (event.unknown.length > 0) {
        findings.push(answer.key + ": decoded as " + event.shape + ", which ignored " +
          event.unknown.join(", "));
      }
      // A LIST'S ITEMS ARE DECODED BY THE SHAPE THE DOCUMENT NAMES FOR THEM.
      if (event.item !== null) {
        const items = (DEFINITIONS[published].properties || {}).items || {};
        const named = referenced(items.items);
        if (named !== event.item) {
          findings.push(answer.key + ": the document publishes a list of " + String(named) +
            " and the console decoded its items as " + event.item);
        }
      }
    }
  }
  assert.deepEqual(findings, [],
    "a route's answer must be decoded by the shape the document publishes for that route, " +
      "with nothing ignored. A narrower shape of another name is how a detail view lost a " +
      "member the API had sent:\n" + findings.join("\n"));

  // AND EVERY ROUTE THE DOCUMENT PUBLISHES WAS READ, OR IS NAMED AS NOT READ.
  const read = new Set(served.map((answer) => answer.key));
  const owed = [];
  for (const operation of OPERATIONS) {
    const variants = Object.keys(ROUTE_VARIANTS)
      .filter((key) => key.indexOf(operation.op + " ") === 0);
    const keys = [operation.op].concat(variants);
    for (const key of keys) {
      const excused = ROUTES_NOT_READ[key] !== undefined || VARIANTS_NOT_READ[key] !== undefined;
      if (read.has(key) && excused) {
        owed.push(key + " is named as not read, and the console read it: remove the entry");
      }
      if (!read.has(key) && !excused) {
        owed.push(key + " is published and no row here reads it");
      }
    }
  }
  assert.deepEqual(owed, [],
    "every route the document publishes with a JSON answer is read by the real client here, " +
      "or named in ROUTES_NOT_READ with why no page reads it:\n" + owed.join("\n"));
  for (const table of [ROUTES_NOT_READ, VARIANTS_NOT_READ]) {
    for (const key of Object.keys(table)) {
      assert.ok(OPERATIONS.some((operation) => operation.op === key),
        key + " is excused and the document publishes no such route");
    }
  }
  for (const key of Object.keys(ROUTE_VARIANTS)) {
    assert.ok(DEFINITIONS[ROUTE_VARIANTS[key].schema] !== undefined,
      ROUTE_VARIANTS[key].schema + " is published");
  }
  assert.ok(served.length >= 50,
    "the row drove the whole client (" + String(served.length) + " answers)");

  // AND THE READERS THE FIXTURE TABLE HOLDS ARE THE ONES THE CLIENT USED: a
  // schema the client decoded an answer with has a reader of that name there.
  const known = readSchemas();
  for (const answer of served) {
    for (const event of answer.decodedBy) {
      assert.ok(known.indexOf(event.shape) !== -1,
        event.shape + " decodes " + answer.key + " and ui/tests/console-fixture.js has no " +
          "reader for it");
    }
  }
});

// ===========================================================================
// 5. the projections
// ===========================================================================
//
// WHY A DECODE THAT LOSES NOTHING IS NOT ENOUGH. The five older kinds
// (`ui/client.js`) and the four D3 families (`ui/operation-watch.js`) are not
// handed to a page as decoded: a projection copies them, member by member,
// into the custom resource's shape, so that one renderer reads both modes.
// A member the projection does not copy is decoded, declared, in no `unknown`
// list -- and gone. That is where the retention report's `note` once stopped,
// and where this sweep found `approvalSubject` and `target.originalName`:
// both required members of the published `Restore`, both decoded, neither
// projected, so the approval page of a shared console said an original-name
// Restore needed an `ordinary` approval.
//
// THE CHECK IS DIFFERENTIAL, SO IT NEEDS NO LIST OF MEMBERS. Each projected
// read is run over its fixtures through the real client; then each leaf of
// the wire document is changed -- a string lengthened, a number moved, a
// boolean flipped, a closed word replaced by each other member in turn -- and
// the read is run again. If what the page is handed is byte for byte what it
// was, that member did not reach it. A member counts as reaching the page
// when a change to it shows in ANY fixture that carries it, so a member a
// projection writes only beside another (a trust basis, beside a recorded
// result) is not a finding.

/** MEMBERS A PROJECTED READ DECODES AND DELIBERATELY DOES NOT HAND ON, as
 *  `"Schema: path"` -- the schema of the document the route answers, and the
 *  member's path in it (a path names everything under it). Each says why, and
 *  an entry nothing uses fails: it would hide the next member that stops. */
const DETAIL_IS_NOT_THE_OPERATION_PAGE =
  "a Backup or Restore DETAIL is a view of the custom resource: it takes the run's state, " +
  "result, evidence, verification, trust basis, scope and integrity level from the operation " +
  "route. This member is shown by the operation page, which is handed the view whole";
const SUMMARY_REPLACED_ON_A_DETAIL =
  "`BackupResponse` is only ever a DETAIL's answer, and on a detail the operation route's own " +
  "state replaces the list summary's; a list row is handed the summary's, as a Restore " +
  "create's answer is (both are held to that here)";
const NOT_HANDED_ON = Object.freeze({
  "ApprovalPacketResponse: item.name": "the packet route is read for its two byte members; " +
    "the identity beside them is the Approval's own, already on the object from its own read",
  "ApprovalPacketResponse: item.namespace": "as item.name: the identity is the Approval's own",
  "ApprovalPacketResponse: item.uid": "as item.name: the identity is the Approval's own",
  "ApprovalPacketResponse: item.planHash": "as item.name: the Approval's own read carries it",
  "ApprovalPacketResponse: item.subjectRef": "as item.name: the Approval's own read carries it",
  "BackupResponse: item.schedule": "the older, coarser name of the schedule: where the run " +
    "also carries `scheduleRef` -- the same name with the revision -- that one is projected, " +
    "and where it does not this one is (list-verdict.spec.js reads such a run)",
  "BackupList: items[].schedule": "as BackupResponse: `scheduleRef` is projected when the " +
    "run carries both",
  "ManualBackupResponse: item.schedule": "as BackupResponse: a manual run always carries " +
    "`scheduleRef`, which has the same name with the revision",
  "BackupResponse: replayed": "`replayed` is a create's answer, and no create answers " +
    "`BackupResponse`: a manual run is answered `ManualBackupResponse`, whose `replayed` is " +
    "handed on",
  "BackupResponse: item.operation.state": SUMMARY_REPLACED_ON_A_DETAIL,
  "BackupResponse: item.operation.stateReason": SUMMARY_REPLACED_ON_A_DETAIL,
  "RestoreResponse: item.selection.scope": "always `partial`: the block's presence is the " +
    "marker, and the projection writes the word itself",
  "RestoreList: items[].selection.scope": "always `partial`: the block's presence is the " +
    "marker, and the projection writes the word itself",
  "RestoreList: items[].targetTopicsAppeared.leftInstruction": "the page prints its own " +
    "sentence, which original-name.spec.js holds equal to the API's, word for word",
  "RestoreList: items[].targetTopicsAppeared.unconfirmedInstruction": "the page prints its " +
    "own sentence, which original-name.spec.js holds equal to the API's, word for word",
  "OperationViewResponse: item.verificationScope.selection.scope":
    "always `partial`: the block's presence is the marker, and the projection writes the word",
  "RestoreResponse: item.targetTopicsAppeared.leftInstruction": "the page prints its own " +
    "sentence, which original-name.spec.js holds equal to the API's, word for word",
  "RestoreResponse: item.targetTopicsAppeared.unconfirmedInstruction": "the page prints its " +
    "own sentence, which original-name.spec.js holds equal to the API's, word for word",
  "OperationViewResponse: item.name": DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.namespace": DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.uid": DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.resourceVersion": DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.createdAt": DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.lastUpdatedAt": DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.message": DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.terminal": DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.verifiedSuccess": DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.result.status": DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.awaitingApproval": DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.stale": DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.stage": DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.progress": DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.diagnostics": DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.readiness": DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.targetMode": DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.teardown": DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.trust.policy": DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.trust.signedAt": DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.trust.signingTimeRead": DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.capture": DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.completion.newTopics": DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.completion.recordsExpected": DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.completion.recordsRestored": DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.completion.recordsSampled": DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.completion.recordsSampledMatching":
    DETAIL_IS_NOT_THE_OPERATION_PAGE,
  "OperationViewResponse: item.completion.sampleWindow": DETAIL_IS_NOT_THE_OPERATION_PAGE,
});

/** A request id names one request. A page shows the id of a request that
 *  FAILED -- it is on the problem document -- and never a successful read's,
 *  so no projection hands one on: excused once, for every schema. */
const REQUEST_ID = "requestId";

function memberDecoder(shape, key) {
  return Object.prototype.hasOwnProperty.call(shape.required, key)
    ? shape.required[key]
    : shape.optional[key];
}

/** The leaves of a wire document, each with how its decoder reads it, walked
 *  by the SHAPE that decodes it -- so a closed word is known to be one. */
function leavesOf(shape, value, path, out, itemShape) {
  for (const key of Object.keys(value)) {
    const decoder = memberDecoder(shape, key);
    const at = path.length === 0 ? key : path + "." + key;
    if (decoder === undefined) {
      continue;
    }
    if (key === "items" && itemShape !== null && Array.isArray(value[key])) {
      value[key].forEach((item, i) =>
        leavesOf(itemShape, item, at + "[" + String(i) + "]", out, null));
      continue;
    }
    leafOf(decoder, value[key], at, out, shape.name + "." + key);
  }
  return out;
}

/** A member the document publishes as a STRING and describes as one of a few
 *  words, which a projection branches on: the words, from the document's own
 *  description of the member. A lengthened string would take the same branch
 *  whatever the projection did with the member. */
const PUBLISHED_WORDS = Object.freeze({
  // "`configMap` or `secret`."
  "TlsCaView.kind": ["configMap", "secret"],
});

function leafOf(decoder, value, at, out, owner) {
  if (value === null || value === undefined) {
    return;
  }
  if (decoder === opaque) {
    out.push({ path: at, members: null });
    return;
  }
  if (decoder.member !== undefined) {
    if (Array.isArray(value)) {
      value.forEach((item, i) =>
        leafOf(decoder.member, item, at + "[" + String(i) + "]", out, owner));
    }
    return;
  }
  const nested = decoder.shape !== undefined
    ? decoder.shape
    : (typeof decoder.later === "function" ? decoder.later() : null);
  if (nested !== null) {
    leavesOf(nested, value, at, out, null);
    return;
  }
  const words = Array.isArray(decoder.members) ? decoder.members : PUBLISHED_WORDS[owner];
  out.push({ path: at, members: words === undefined ? null : words });
}

function valueAt(document, path, replace) {
  const steps = path.replace(/\[(\d+)\]/g, ".$1").split(".");
  let at = document;
  for (let i = 0; i < steps.length - 1; i += 1) {
    at = at[steps[i]];
  }
  const last = steps[steps.length - 1];
  if (replace !== undefined) {
    at[last] = replace(at[last]);
  }
  return at[last];
}

/** The one member that is never given a value the fixture does not carry: a
 *  cursor tells the client another page exists, and it would read one for
 *  ever. Whether a page's cursor is followed is `client.spec.js`'s row. */
const NOT_SAMPLED = Object.freeze(["Page.nextCursor"]);

/** A value a decoder accepts, for a member the fixture does not carry. */
function sampleOf(decoder) {
  if (decoder === opaque) {
    return "sample";
  }
  if (decoder.member !== undefined) {
    return [sampleOf(decoder.member)];
  }
  const nested = decoder.shape !== undefined
    ? decoder.shape
    : (typeof decoder.later === "function" ? decoder.later() : null);
  if (nested !== null) {
    return filled(nested, {}, null);
  }
  if (Array.isArray(decoder.members)) {
    return decoder.members[0];
  }
  if (decoder === bool) {
    return true;
  }
  return decoder === int ? 1 : "sample";
}

/** `value` WITH EVERY MEMBER ITS SHAPE DECLARES: what the fixture carries is
 *  kept, and each member it does not carry is given a value its decoder
 *  accepts. So the arm below changes every DECLARED member of a projected
 *  read, not only the ones somebody happened to put in a fixture -- and with
 *  the first arm (declared is everything published) that is every member the
 *  document publishes. A fixture that carries no `note` cannot hide a
 *  projection that drops `note`. */
function filled(shape, value, itemShape) {
  const out = {};
  for (const bag of [shape.required, shape.optional]) {
    for (const key of Object.keys(bag)) {
      const decoder = bag[key];
      const present = value !== null && typeof value === "object" &&
        value[key] !== undefined && value[key] !== null;
      if (!present) {
        if (NOT_SAMPLED.indexOf(shape.name + "." + key) === -1) {
          out[key] = key === "items" && itemShape !== null ? [] : sampleOf(decoder);
        }
        continue;
      }
      if (key === "items" && itemShape !== null && Array.isArray(value[key])) {
        out[key] = value[key].map((item) => filled(itemShape, item, null));
        continue;
      }
      out[key] = filledBy(decoder, value[key]);
    }
  }
  return out;
}

function filledBy(decoder, value) {
  if (decoder === opaque || value === null || value === undefined) {
    return value;
  }
  if (decoder.member !== undefined) {
    return Array.isArray(value) ? value.map((item) => filledBy(decoder.member, item)) : value;
  }
  const nested = decoder.shape !== undefined
    ? decoder.shape
    : (typeof decoder.later === "function" ? decoder.later() : null);
  return nested === null ? value : filled(nested, value, null);
}

/** Every other value a leaf may be changed to. */
function otherValues(leaf, value) {
  if (leaf.members !== null) {
    return leaf.members.filter((member) => member !== value);
  }
  if (typeof value === "string") {
    return [value + "-changed"];
  }
  if (typeof value === "number") {
    return [value + 1];
  }
  if (typeof value === "boolean") {
    return [!value];
  }
  return [{ changed: true }];
}

/** EVERY PROJECTED READ: the fixture, the shape that decodes it (and its
 *  items), what the other routes of the same read answer, and the read. `"@"`
 *  is the document under test. */
function projectedReads(api, reviewed) {
  const detail = (plural) => () => api.get(NS, plural, "x");
  const list = (plural) => () => api.list(NS, plural);
  const operation = (name) => [/\/operations\//, wire(name)];
  const S = CONSOLE_SHAPES;
  const D = D3_SHAPES;
  return [
    ["connection.json", S.ConnectionResponse, null, [[/\/connections\//, "@"]],
      detail("kafkaclusters")],
    ["connections-list.json", S.ConnectionList, S.Connection, [[/\/connections$/, "@"]],
      list("kafkaclusters")],
    ["schedule.json", S.ScheduleResponse, null, [[/\/schedules\//, "@"]],
      detail("backupschedules")],
    ["schedule-policy.json", S.ScheduleResponse, null, [[/\/schedules\//, "@"]],
      detail("backupschedules")],
    ["schedule-preset.json", S.ScheduleResponse, null, [[/\/schedules\//, "@"]],
      detail("backupschedules")],
    ["schedule-last-slot.json", S.ScheduleResponse, null, [[/\/schedules\//, "@"]],
      detail("backupschedules")],
    ["schedules-list.json", S.ScheduleList, S.Schedule, [[/\/schedules$/, "@"]],
      list("backupschedules")],
    ["backup.json", S.BackupResponse, null,
      [operation("operation-backup-preparing.json"), [/\/backups\//, "@"]], detail("backups")],
    ["backup-poc.json", S.BackupResponse, null,
      [operation("operation-backup-preparing.json"), [/\/backups\//, "@"]], detail("backups")],
    ["backups-list.json", S.BackupList, S.Backup, [[/\/backups$/, "@"]], list("backups")],
    ["manual-backup.json", S.ManualBackupResponse, null, [[/\/backups$/, "@"]],
      () => api.runBackupNow(NS, { scheduleRef: { name: "x" } }, "intent-0123456789abcdef")],
    ["restore.json", S.RestoreResponse, null,
      [operation("operation-restore-completed.json"), [/\/restores\//, "@"]], detail("restores")],
    ["restore-time-basis.json", S.RestoreResponse, null,
      [operation("operation-restore-completed.json"), [/\/restores\//, "@"]], detail("restores")],
    ["restore-creation-stopped.json", S.RestoreResponse, null,
      [operation("operation-restore-completed.json"), [/\/restores\//, "@"]], detail("restores")],
    ["restore-creation-unconfirmed.json", S.RestoreResponse, null,
      [operation("operation-restore-completed.json"), [/\/restores\//, "@"]], detail("restores")],
    ["restore-subset-pass.json", S.RestoreResponse, null,
      [operation("operation-restore-subset-pass.json"), [/\/restores\//, "@"]],
      detail("restores")],
    ["restore-complete-uncovered.json", S.RestoreResponse, null,
      [operation("operation-restore-complete-uncovered.json"), [/\/restores\//, "@"]],
      detail("restores")],
    ["restores-list.json", S.RestoreList, S.Restore, [[/\/restores$/, "@"]], list("restores")],
    ["approval.json", S.ApprovalResponse, null,
      [[/\/packet$/, wire("approval-packet.json")], [/\/approvals\//, "@"]], detail("approvals")],
    ["approval-revoked-after-use.json", S.ApprovalResponse, null,
      [[/\/packet$/, wire("approval-packet.json")], [/\/approvals\//, "@"]], detail("approvals")],
    ["approvals-list.json", S.ApprovalList, S.Approval, [[/\/approvals$/, "@"]],
      list("approvals")],
    ["approval-packet.json", S.ApprovalPacketResponse, null,
      [[/\/packet$/, "@"], [/\/approvals\//, wire("approval.json")]], detail("approvals")],
    // THE CREATES, whose answers carry what only a create answers: `replayed`,
    // and a Restore's frozen approval decision.
    ["connection.json", S.ConnectionResponse, null, [[/\/connections$/, "@"]],
      () => api.create(NS, "kafkaclusters", clusterObject())],
    ["schedule.json", S.ScheduleResponse, null, [[/\/schedules$/, "@"]],
      () => api.create(NS, "backupschedules", scheduleObject())],
    ["restore.json", S.RestoreResponse, null, [[/\/restores$/, "@"]],
      () => api.create(NS, "restores", restoreObject(reviewed))],
    ["approval.json", S.ApprovalResponse, null, [[/\/approval$/, "@"]],
      () => api.submitGovernedApproval(NS, "orders-drill-20260911", "sidecar bytes")],
    ["catalog.json", D.CatalogResponse, null, [[/\/catalogs$/, "@"]],
      () => connectArchive(NS, CATALOG_REQUEST, "intent-fedcba9876543210")],
    // The operation route, as a Restore and a Backup DETAIL read it.
    ["operation-restore-completed.json", D.OperationViewResponse, null,
      [[/\/operations\//, "@"], [/\/restores\//, wire("restore.json")]], detail("restores")],
    ["operation-restore-scratch.json", D.OperationViewResponse, null,
      [[/\/operations\//, "@"], [/\/restores\//, wire("restore.json")]], detail("restores")],
    ["operation-restore-untrusted.json", D.OperationViewResponse, null,
      [[/\/operations\//, "@"], [/\/restores\//, wire("restore.json")]], detail("restores")],
    ["operation-restore-subset-pass.json", D.OperationViewResponse, null,
      [[/\/operations\//, "@"], [/\/restores\//, wire("restore-subset-pass.json")]],
      detail("restores")],
    ["operation-restore-complete-uncovered.json", D.OperationViewResponse, null,
      [[/\/operations\//, "@"], [/\/restores\//, wire("restore-complete-uncovered.json")]],
      detail("restores")],
    ["operation-backup-preparing.json", D.OperationViewResponse, null,
      [[/\/operations\//, "@"], [/\/backups\//, wire("backup.json")]], detail("backups")],
    // The four D3 families.
    ["protection-policy.json", D.ProtectionPolicyResponse, null,
      [[/protection-policies\//, "@"]], () => readD3("protection", NS, "x")],
    ["protection-policy-unknown.json", D.ProtectionPolicyResponse, null,
      [[/protection-policies\//, "@"]], () => readD3("protection", NS, "x")],
    ["protection-policies-list.json", D.ProtectionPolicyList, D.ProtectionPolicyView,
      [[/protection-policies$/, "@"]], () => listD3("protection", NS)],
    ["catalog.json", D.CatalogResponse, null, [[/catalogs\//, "@"]],
      () => readD3("catalog", NS, "x")],
    ["catalogs-list.json", D.CatalogList, D.CatalogView, [[/catalogs$/, "@"]],
      () => listD3("catalog", NS)],
    ["retention-policy-enforce.json", D.RetentionPolicyResponse, null,
      [[/retention-policies\//, "@"]], () => readD3("retention", NS, "x")],
    ["retention-policies-list.json", D.RetentionPolicyList, D.RetentionPolicyView,
      [[/retention-policies$/, "@"]], () => listD3("retention", NS)],
    ["trust-policy.json", D.TrustPolicyResponse, null, [[/trust-policies\//, "@"]],
      () => readD3("trust", NS, "x")],
    ["trust-policies-list.json", D.TrustPolicyList, D.TrustPolicyView,
      [[/trust-policies$/, "@"]], () => listD3("trust", NS)],
  ];
}

/** For each `"Schema: path"` a projected read carries: whether a change to it
 *  ever changed what the page is handed. */
async function projectionReach() {
  resetMode();
  await selectMode({ probe: async () => ({ ok: true, status: 200, body: everythingGranted() }) });
  const original = globalThis.fetch;
  const serve = (table) => {
    globalThis.fetch = (url) => {
      const path = String(url).split("?")[0];
      for (const [pattern, body] of table) {
        if (pattern.test(path)) {
          return Promise.resolve({
            ok: true, status: 200, text: () => Promise.resolve(JSON.stringify(body)),
          });
        }
      }
      return Promise.reject(new Error("the row has no answer for " + String(url)));
    };
  };
  const reach = new Map();
  let changes = 0;
  try {
    const reviewed = await preparePlanDocument(
      JSON.parse(readFileSync(FIXTURES + "plan-fields.json", "utf8")));
    for (const [name, shape, itemShape, table, read] of projectedReads(apiClient(), reviewed)) {
      const routes = (document) => table.map(([pattern, body]) =>
        [pattern, body === "@" ? document : body]);
      // THE FIXTURE, COMPLETED: every member the shape declares is present.
      const whole = filled(shape, wire(name), itemShape);
      serve(routes(structuredClone(whole)));
      const baseline = JSON.stringify(await read());
      for (const leaf of leavesOf(shape, whole, "", [], itemShape)) {
        const key = shape.name + ": " + leaf.path.replace(/\[\d+\]/g, "[]");
        let reached = false;
        for (const other of otherValues(leaf, valueAt(whole, leaf.path))) {
          const document = structuredClone(whole);
          valueAt(document, leaf.path, () => other);
          serve(routes(document));
          changes += 1;
          let handed;
          try {
            handed = JSON.stringify(await read());
          } catch (refused) {
            // A change the client REFUSES reached it: it was read.
            handed = null;
          }
          if (handed !== baseline) {
            reached = true;
            break;
          }
        }
        reach.set(key, reach.get(key) === true || reached);
      }
    }
  } finally {
    globalThis.fetch = original;
    resetMode();
  }
  return { reach: reach, changes: changes };
}

/** The `NOT_HANDED_ON` entry that excuses `key`, or `null`. */
function excusedBy(key) {
  const [schema, path] = key.split(": ");
  if (path === REQUEST_ID) {
    return REQUEST_ID;
  }
  for (const entry of Object.keys(NOT_HANDED_ON)) {
    const [entrySchema, entryPath] = entry.split(": ");
    if (entrySchema === schema && (path === entryPath || path.indexOf(entryPath + ".") === 0 ||
      path.indexOf(entryPath + "[") === 0)) {
      return entry;
    }
  }
  return null;
}

test("every_member_a_projected_read_decodes_reaches_what_the_page_is_handed", async () => {
  const { reach, changes } = await projectionReach();
  assert.ok(reach.size >= 1100 && changes >= 3500,
    "the row changed every member of every projected read (" + String(reach.size) +
      " members, " + String(changes) + " changes)");
  const stopped = [];
  const used = new Set();
  for (const [key, reached] of reach) {
    const excuse = excusedBy(key);
    if (reached) {
      // AN EXCUSED MEMBER THAT REACHES THE PAGE IS NOT EXCUSED ANY MORE.
      if (excuse !== null && excuse !== REQUEST_ID) {
        stopped.push(key + " reaches the page and NOT_HANDED_ON still excuses it: remove " +
          "the entry");
      }
      continue;
    }
    if (excuse === null) {
      stopped.push(key);
    } else {
      used.add(excuse);
    }
  }
  assert.deepEqual(stopped.sort(), [],
    "these members are decoded and then NOT handed to the page: the projection in " +
      "ui/client.js or ui/operation-watch.js does not copy them, so a page that reads one " +
      "renders as if the API had not sent it. Copy it in the projection (under the custom " +
      "resource's own name where it has one), or name it in NOT_HANDED_ON with why:\n" +
      stopped.sort().join("\n"));
  for (const entry of Object.keys(NOT_HANDED_ON)) {
    assert.ok(used.has(entry), "NOT_HANDED_ON names " + entry + ", and no projected read " +
      "carries a member it excuses: remove the entry");
    assert.ok(NOT_HANDED_ON[entry].length >= 40, entry + " says why in a sentence");
  }
});

// ===========================================================================
// 4. the tests
// ===========================================================================

/** A member taken straight off an undecoded console fixture: the loader's
 *  call, a closing parenthesis, and a `.` or a `[`. Three spellings of the
 *  loader are in use (`fixture("console/x.json")`, `con("x.json")`,
 *  `console_("x.json")`), plus `wire("x.json")` and a bare `JSON.parse` of the
 *  file. */
const RAW_MEMBER = new RegExp(
  "(?:" +
    "\\b(?:fixture|wire)\\(\\s*\"console/[^\"]+\"\\s*\\)" + "|" +
    "\\b(?:con|console_|wire)\\(\\s*\"[a-z0-9-]+\\.json\"\\s*\\)" + "|" +
    "fixtures/console/[^\"]+\"[^;\\n]*\\)\\s*\\)" +
  ")\\s*(?:\\.|\\[)",
);

/** A CATALOG POINT, OR A PAGE OF THEM, WRITTEN OUT AS THE ARGUMENT of one of
 *  the functions the catalog page, the wizard and the schedule page read a
 *  point with. This is the surface the schema dependency was lost on, and a
 *  row written in a test is the product API's document as much as a fixture
 *  file is: it goes through `handedPoint` / `handedPoints` / `handedPage`
 *  (ui/tests/console-fixture.js), or through `readCatalogPoints` itself. */
const POINT_LITERAL = new RegExp(
  "\\b(?:renderPoints|pointRow|sourceFactsOfEntry|schemaDependentTopicsOf|" +
    "consumerPositionsNote|catalogPointOffer|renderSchemaDependentPoints)\\(\\s*\\{",
);

/** The lines of `source` that take a member off an undecoded console fixture,
 *  or hand a catalog function a point written out in place. */
export function rawMemberLines(source) {
  const out = [];
  String(source).split("\n").forEach((line, i) => {
    const code = line.replace(/^\s*(?:\/\/|\*).*$/, "");
    if (RAW_MEMBER.test(code) || POINT_LITERAL.test(code)) {
      out.push(String(i + 1) + ": " + line.trim());
    }
  });
  return out;
}

test("no_spec_takes_a_member_off_an_undecoded_console_fixture", () => {
  const specs = readdirSync(TESTS).filter((f) => f.endsWith(".spec.js")).sort();
  assert.ok(specs.length >= 50, "the lint read the whole suite (" + String(specs.length) + ")");
  const found = [];
  for (const spec of specs) {
    // This file's own patterns and controls are the one exception, by name.
    if (spec === "contract-coverage.spec.js") {
      continue;
    }
    for (const line of rawMemberLines(readFileSync(TESTS + spec, "utf8"))) {
      found.push(spec + ":" + line);
    }
  }
  assert.deepEqual(found, [],
    "a page is never handed the product API's document: it is handed what ui/contract.js " +
      "decoded. Take the member off `decoded(name)` (ui/tests/console-fixture.js), or change " +
      "the wire document first and decode it -- `decoded(name, (body) => { ... })`; and hand " +
      "a catalog function a point through `handedPoint` / `handedPage`, not one written out " +
      "in place:\n" + found.join("\n"));
});

test("the_lint_refuses_each_spelling_and_ignores_a_whole_document", () => {
  const refused = [
    "const d = fixture(\"console/destination.json\").item;",
    "const p = con(\"catalog-points-states.json\").items;",
    "const n = console_(\"session.json\").namespaces[0].name;",
    "const w = wire(\"restore.json\").item.name;",
    "  new URL(\"./fixtures/console/preflight-ready.json\", import.meta.url), \"utf8\")).item;",
    "const first = fixture(\"console/backups-list.json\")[\"items\"];",
    "const html = renderPoints({ items: [row], page: {} }, \"team-a\", \"c1\", \"dest\");",
    "assert.equal(consumerPositionsNote({ consumerPositions: cp }), \"\");",
    "const facts = sourceFactsOfEntry({ pointId: \"p1\", topics: [] }, \"archive\");",
  ];
  for (const line of refused) {
    assert.equal(rawMemberLines(line).length, 1, line);
  }
  const allowed = [
    "const body = wire(\"restore.json\");",
    "return { status: 200, body: fixture(\"console/restore.json\") };",
    "const page = decodeCatalogPoints(con(\"catalog-points.json\")).value;",
    "const item = decoded(\"destination.json\").item;",
    "// const d = fixture(\"console/destination.json\").item;",
    "const plan = fixture(\"plan-fields.json\").topics;",
    "const html = renderPoints(handedPage([row]), \"team-a\", \"c1\", \"dest\");",
    "const facts = sourceFactsOfEntry(handedPoint(wireRow), \"archive\");",
  ];
  for (const line of allowed) {
    assert.deepEqual(rawMemberLines(line), [], line);
  }
});
