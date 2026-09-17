// client.spec.js -- the mode is chosen once, the two modes answer the pages
// with the same shape, and a disagreement from either server lands beside the
// field it is about.
//
// EVERY TEST HERE DRIVES THE REAL TRANSPORT. `ui/api.js`'s one call site is
// stubbed at the platform boundary, so the identifier under assertion is the
// one `path(...)` built and the headers are the ones `api.js` set. Nothing
// opens a socket.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  CONSOLE,
  LEGACY,
  LIST_PAGE_BUDGET,
  apiClient,
  bindingRevision,
  rolesFor,
  applyGrants,
  granted,
  grantedNamespaces,
  mode,
  resetMode,
  selectMode,
  sessionDocument,
} from "../client.js";
import {
  createOnce,
  createdOutcome,
  dropDraft,
  fieldErrors,
  formKey,
  keepDraft,
  readDraft,
  readOptions,
} from "../lifecycle.js";
import {
  CLUSTER_DRAFT_FIELDS,
  CLUSTER_FIELD_PATHS,
  CLUSTER_FORM,
  clusterBody,
} from "../pages/clusters.js";
import { preparePlanDocument } from "../plan.js";
import { decodeRequest, isContractFailure } from "../contract.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));

function fixture(name) {
  return JSON.parse(readFileSync(FIXTURES + name, "utf8"));
}

const SESSION = () => fixture("console/session.json");

/** Replaces the one platform call `ui/api.js` makes, records every request and
 *  answers from a table keyed by the identifier. Returns the log and the
 *  restore. */
function transport(answer) {
  const seen = [];
  const original = globalThis.fetch;
  globalThis.fetch = (u, init) => {
    seen.push({ url: u, init: init || {} });
    const reply = answer(u, init || {});
    if (reply === undefined || reply === null) {
      return Promise.reject(new Error("the suite has no answer for " + String(u)));
    }
    return Promise.resolve({
      ok: reply.status >= 200 && reply.status < 300,
      status: reply.status,
      text: () => Promise.resolve(reply.body === undefined ? "" : JSON.stringify(reply.body)),
    });
  };
  return { seen: seen, restore: () => { globalThis.fetch = original; } };
}

/** Pins console mode from a session document without a request. */
async function console_(document) {
  resetMode();
  return selectMode({ probe: async () => ({ ok: true, status: 200, body: document || SESSION() }) });
}

/** Pins legacy mode: what a `kubectl proxy` path filter answers. */
async function legacy() {
  resetMode();
  return selectMode({ probe: async () => ({ ok: false, status: 403, body: null }) });
}

const VALUES = Object.freeze({
  name: "orders-prod", servers: "kafka-0.orders.svc:9093", role: "source",
  mode: "scramSha512", username: "logweir-reader", secret: "orders-scram", tls: true,
});

// ======================================================= the mode is decided

test("the_mode_is_chosen_once_at_boot_and_never_per_request", async () => {
  resetMode();
  let probes = 0;
  const record = await selectMode({
    probe: async () => {
      probes += 1;
      return { ok: true, status: 200, body: SESSION() };
    },
  });
  assert.equal(record.mode, CONSOLE);
  const wire = transport((u) =>
    u.indexOf("/connections") !== -1
      ? { status: 200, body: fixture("console/connections-list.json") }
      : { status: 200, body: fixture("console/schedules-list.json") });
  try {
    const api = apiClient();
    await api.list("team-a", "kafkaclusters");
    await api.list("team-a", "backupschedules");
    await api.list("team-a", "kafkaclusters");
    assert.equal(probes, 1, "one session request decided the mode for the life of the page");
    assert.equal(wire.seen.length, 3, "and no request of its own went with the three reads");
    assert.equal(mode(), CONSOLE);
  } finally {
    wire.restore();
  }
});

test("concurrent_first_calls_share_the_one_probe", async () => {
  resetMode();
  let probes = 0;
  const answers = await Promise.all([
    selectMode({ probe: async () => { probes += 1; return { ok: false, status: 403 }; } }),
    selectMode(),
    selectMode(),
  ]);
  assert.equal(probes, 1, "the in-flight probe is shared, not restarted");
  for (const answer of answers) {
    assert.equal(answer.mode, LEGACY);
  }
});

test("a_refusal_a_body_that_is_not_a_session_and_no_answer_are_all_legacy_mode", async () => {
  for (const reply of [
    { ok: false, status: 403, body: null },
    { ok: true, status: 200, body: { items: [] } },
    { ok: true, status: 200, body: null },
  ]) {
    resetMode();
    const record = await selectMode({ probe: async () => reply });
    assert.equal(record.mode, LEGACY, JSON.stringify(reply) + " is not the product API");
    assert.equal(sessionDocument(), null);
    assert.deepEqual(grantedNamespaces(), []);
  }
  resetMode();
  const thrown = await selectMode({ probe: async () => { throw new Error("no route"); } });
  assert.equal(thrown.mode, LEGACY, "a transport failure is a page that is not behind the API");
});

test("the_session_grants_are_the_namespace_list_in_console_mode", async () => {
  await console_();
  assert.deepEqual(grantedNamespaces(), ["team-a"]);
  const context = { allowed: ["from-runtime-js"], selected: "from-runtime-js" };
  assert.equal(applyGrants(context, grantedNamespaces()), true, "the grants replace the runtime list");
  assert.deepEqual(context.allowed, ["team-a"]);
  assert.equal(context.selected, "team-a", "one grant selects itself");
  assert.equal(applyGrants(context, grantedNamespaces()), false, "and applying them twice changes nothing");
  await legacy();
  assert.equal(
    applyGrants({ allowed: ["a"], selected: "a" }, grantedNamespaces()),
    false,
    "legacy mode has no grants to apply: the namespace is the explicit selection PLAT-13.1 introduced",
  );
});

test("a_capability_the_session_withholds_is_refused_before_the_network", async () => {
  const document = SESSION();
  document.namespaces[0].capabilities.connectionsRead = false;
  await console_(document);
  const wire = transport(() => ({ status: 200, body: fixture("console/connections-list.json") }));
  try {
    await assert.rejects(
      () => apiClient().list("team-a", "kafkaclusters"),
      (error) => {
        assert.equal(error.status, 403);
        assert.equal(error.reason, "namespace_forbidden");
        assert.match(error.message, /connectionsRead/);
        return true;
      },
    );
    assert.equal(wire.seen.length, 0, "nothing was sent");
    assert.equal(granted("team-b", "connectionsRead"), false, "an ungranted namespace is not reachable");
  } finally {
    wire.restore();
  }
});

// ============================================== console mode reads and writes

test("console_mode_lists_through_api_v1_and_projects_onto_the_resource_the_page_reads", async () => {
  await console_();
  const wire = transport(() => ({ status: 200, body: fixture("console/connections-list.json") }));
  try {
    const collection = await apiClient().list("team-a", "kafkaclusters");
    assert.equal(wire.seen[0].url, "/api/v1/namespaces/team-a/connections?limit=200");
    assert.equal(wire.seen[0].init.method, "GET");
    assert.equal(collection.items.length, 2);
    const first = collection.items[0];
    assert.equal(first.kind, "KafkaCluster");
    assert.equal(first.metadata.name, "orders-prod");
    assert.equal(first.metadata.uid, "1b2c3d4e-5f60-4718-8293-a4b5c6d7e8f9");
    assert.deepEqual(first.spec.bootstrapServers, [
      "kafka-0.orders.svc:9093", "kafka-1.orders.svc:9093",
    ]);
    assert.equal(first.spec.auth.mode, "scramSha512");
    assert.equal(
      first.spec.auth.secretRef.name,
      "orders-scram",
      "the product API spells it credentialRef; the page reads secretRef, and the adapter is " +
        "the only place that knows",
    );
    assert.equal(first.status.reachable, true);
    assert.equal(collection.items[1].status.reachable, undefined,
      "an unknown reachability is ABSENT, not false: the page's own badge says `unknown` for it");
    assert.deepEqual(
      first.__contract.absent,
      ["status.conditions", "spec.auth.secretRef.passwordKey", "spec.auth.tlsCa"],
      "what the projection cannot supply is named on the object, not quietly defaulted -- and " +
        "since PLAT-07.2 that includes connection contract v1's two references, which the " +
        "product API's ConnectionAuthView does not carry",
    );
  } finally {
    wire.restore();
  }
});

test("console_mode_reads_a_backup_detail_with_the_evidence_from_its_operation_route", async () => {
  await console_();
  const wire = transport((u) =>
    u.indexOf("/operations/") !== -1
      ? { status: 200, body: fixture("console/operation-backup.json") }
      : { status: 200, body: fixture("console/backup.json") });
  try {
    const object = await apiClient().get("team-a", "backups", "orders-hourly-20260912-080000");
    assert.equal(wire.seen.length, 2, "the detail view reads the object and its operation");
    assert.equal(wire.seen[1].url,
      "/api/v1/namespaces/team-a/operations/backup/orders-hourly-20260912-080000");
    assert.equal(object.status.phase, "Succeeded");
    assert.equal(object.status.exitCode, 0);
    assert.equal(object.status.evidence.verification.result, "Valid");
    assert.equal(object.status.evidence.receiptKey.length > 0, true);
    assert.equal(object.spec.scheduleRef.name, "orders-hourly");
    assert.equal(object.status.windowCovered.fromMs, 1789196400000);
  } finally {
    wire.restore();
  }
});

test("a_normalized_state_the_resource_has_no_phase_for_shows_the_api_s_own_word", async () => {
  await console_();
  const body = fixture("console/backups-list.json");
  body.items[0].operation.state = "verifying";
  body.items[0].operation.verificationState = "pending";
  body.items[0].operation.verifiedSuccess = false;
  const wire = transport(() => ({ status: 200, body: body }));
  try {
    const collection = await apiClient().list("team-a", "backups");
    assert.equal(
      collection.items[0].status.phase,
      "Verifying",
      "the controller never wrote this phase, and rounding it to Succeeded would be this page " +
        "claiming an outcome nobody recorded",
    );
  } finally {
    wire.restore();
  }
});

test("the_trust_roster_is_refused_by_name_in_console_mode", async () => {
  await console_();
  const wire = transport(() => ({ status: 200, body: {} }));
  try {
    await assert.rejects(
      () => apiClient().listCluster("trustrosters"),
      (error) => {
        assert.equal(error.reason, "NoConsoleRoute");
        assert.equal(error.kind, "refused");
        assert.match(error.message, /cluster-scoped/);
        return true;
      },
    );
    assert.equal(wire.seen.length, 0, "and nothing was asked of any server");
  } finally {
    wire.restore();
  }
});

test("console_mode_creates_with_an_idempotency_key_that_is_a_function_of_the_object", async () => {
  await console_();
  const wire = transport((u, init) =>
    init.method === "POST"
      ? { status: 201, body: fixture("console/connection.json") }
      : undefined);
  try {
    const api = apiClient();
    const body = clusterBody(VALUES);
    const first = await api.create("team-a", "kafkaclusters", body);
    const second = await api.create("team-a", "kafkaclusters", body);
    assert.equal(wire.seen.length, 2);
    assert.equal(wire.seen[0].url, "/api/v1/namespaces/team-a/connections");
    const key = wire.seen[0].init.headers["Idempotency-Key"];
    assert.equal(key, "logweir-ui.connections.team-a.orders-prod");
    assert.equal(
      wire.seen[1].init.headers["Idempotency-Key"],
      key,
      "a retry composes the same key, so the API resolves it to the object the first one made",
    );
    assert.equal(wire.seen[0].init.headers["Content-Type"], "application/json");
    const sent = JSON.parse(wire.seen[0].init.body);
    assert.deepEqual(Object.keys(sent).sort(), ["auth", "bootstrapServers", "role"]);
    assert.deepEqual(sent.auth, {
      mode: "scramSha512", tls: true, username: "logweir-reader",
      credentialRef: { name: "orders-scram" },
    });
    assert.equal(sent.metadata, undefined, "the product API mints the name; the body has none");
    assert.equal(first.kind, "KafkaCluster");
    assert.equal(createdOutcome(first), "created");
    assert.equal(createdOutcome(second), "created");
  } finally {
    wire.restore();
  }
});

test("a_replayed_create_reads_as_existing_and_never_as_a_second_object", async () => {
  await console_();
  const replay = fixture("console/connection.json");
  replay.replayed = true;
  const wire = transport((u, init) =>
    init.method === "POST" ? { status: 200, body: replay } : undefined);
  try {
    const outcome = await createOnce(apiClient(), "team-a", "kafkaclusters", clusterBody(VALUES), {});
    assert.equal(
      outcome.outcome,
      "existing",
      "200 with replayed: true is the API saying this operation already happened",
    );
    assert.equal(outcome.object.metadata.name, "orders-prod");
  } finally {
    wire.restore();
  }
});

test("an_idempotency_conflict_reads_as_a_conflict_and_keeps_what_was_typed", async () => {
  await console_();
  const wire = transport((u, init) =>
    init.method === "POST"
      ? { status: 409, body: fixture("console/problem-conflict.json") }
      : undefined);
  const key = formKey("team-a", CLUSTER_FORM);
  keepDraft(key, VALUES, CLUSTER_DRAFT_FIELDS);
  try {
    await assert.rejects(
      () => createOnce(apiClient(), "team-a", "kafkaclusters", clusterBody(VALUES), {}),
      (error) => {
        assert.equal(error.kind, "conflict");
        assert.equal(error.status, 409);
        assert.equal(error.reason, "idempotency_conflict");
        return true;
      },
    );
    assert.equal(readDraft(key).name, "orders-prod", "the draft is untouched by a refusal");
  } finally {
    dropDraft(key);
    wire.restore();
  }
});

// =============================== the server disagrees about a field, in both modes

test("a_console_field_error_lands_beside_the_field_and_the_draft_is_kept", async () => {
  await console_();
  const wire = transport((u, init) =>
    init.method === "POST"
      ? { status: 422, body: fixture("console/problem-validation.json") }
      : undefined);
  const key = formKey("team-a", CLUSTER_FORM);
  keepDraft(key, VALUES, CLUSTER_DRAFT_FIELDS);
  try {
    let caught = null;
    try {
      await createOnce(apiClient(), "team-a", "kafkaclusters", clusterBody(VALUES), {});
    } catch (error) {
      caught = error;
    }
    assert.ok(caught !== null, "the server refused");
    assert.equal(caught.status, 422);
    assert.equal(caught.reason, "validation_failed");
    const rendered = fieldErrors(caught, CLUSTER_FIELD_PATHS);
    assert.deepEqual(
      rendered.fields.servers,
      ["must be host:port with no scheme or userinfo"],
      "bootstrapServers[0] was translated into spec.bootstrapServers[0], which the page's own " +
        "table maps onto its `servers` input -- verbatim, with no rewording",
    );
    assert.deepEqual(rendered.fields.username, ["scramSha512 requires a username"]);
    assert.deepEqual(rendered.unmatched, []);
    assert.equal(readDraft(key).servers, VALUES.servers, "and every value typed is still there");
  } finally {
    dropDraft(key);
    wire.restore();
  }
});

test("a_legacy_field_error_lands_beside_the_same_field_from_the_same_page_table", async () => {
  await legacy();
  const status = {
    kind: "Status", status: "Failure", reason: "Invalid", code: 422,
    message: "KafkaCluster.logweir.dev \"orders-prod\" is invalid",
    details: {
      name: "orders-prod", kind: "kafkaclusters",
      causes: [
        { reason: "FieldValueInvalid", field: "spec.bootstrapServers[0]",
          message: "must be host:port with no scheme or userinfo" },
        { reason: "FieldValueRequired", field: "spec.auth.username",
          message: "scramSha512 requires a username" },
      ],
    },
  };
  const wire = transport((u, init) =>
    (init.method || "GET") === "POST" ? { status: 422, body: status } : undefined);
  const key = formKey("team-a", CLUSTER_FORM);
  keepDraft(key, VALUES, CLUSTER_DRAFT_FIELDS);
  try {
    let caught = null;
    try {
      await createOnce(apiClient(), "team-a", "kafkaclusters", clusterBody(VALUES), {});
    } catch (error) {
      caught = error;
    }
    assert.ok(caught !== null);
    const rendered = fieldErrors(caught, CLUSTER_FIELD_PATHS);
    assert.deepEqual(rendered.fields.servers, ["must be host:port with no scheme or userinfo"]);
    assert.deepEqual(rendered.fields.username, ["scramSha512 requires a username"]);
    assert.equal(readDraft(key).servers, VALUES.servers);
  } finally {
    dropDraft(key);
    wire.restore();
  }
});

test("the_client_s_own_checks_refuse_the_same_body_in_both_modes_without_sending_it", async () => {
  const bad = clusterBody(Object.assign({}, VALUES, { servers: "kafka-0.orders.svc" }));
  for (const pin of [console_, legacy]) {
    await pin();
    const wire = transport(() => ({ status: 201, body: fixture("console/connection.json") }));
    try {
      await assert.rejects(
        () => apiClient().create("team-a", "kafkaclusters", bad),
        (error) => {
          assert.equal(error.kind, "invalid");
          assert.equal(error.reason, "ClientValidation");
          const rendered = fieldErrors(error, CLUSTER_FIELD_PATHS);
          assert.match(rendered.fields.servers[0], /host:port/);
          return true;
        },
      );
      assert.equal(wire.seen.length, 0, "one validation module, and nothing was sent in either mode");
    } finally {
      wire.restore();
    }
  }
});

// ============================================= navigation, plans and disposal

test("a_read_carries_the_route_signal_and_a_create_never_does", async () => {
  await console_();
  const controller = new AbortController();
  const lifecycle = { signal: controller.signal, isCurrent: () => true };
  const wire = transport((u, init) =>
    init.method === "POST"
      ? { status: 201, body: fixture("console/connection.json") }
      : { status: 200, body: fixture("console/connections-list.json") });
  try {
    const api = apiClient();
    await api.list("team-a", "kafkaclusters", readOptions(lifecycle));
    await api.create("team-a", "kafkaclusters", clusterBody(VALUES));
    assert.equal(wire.seen[0].init.signal, controller.signal, "the view's read is the view's");
    assert.equal(
      wire.seen[1].init.signal,
      undefined,
      "and a create the server may already have taken is nobody's to cancel",
    );
  } finally {
    wire.restore();
  }
});

test("the_submitted_plan_is_the_reviewed_object_and_its_own_hash", async () => {
  await console_();
  const fields = fixture("plan-fields.json");
  const reviewed = await preparePlanDocument(fields);
  const wire = transport((u, init) =>
    init.method === "POST" ? { status: 201, body: fixture("console/restore.json") } : undefined);
  try {
    await apiClient().create("team-a", "restores", {
      apiVersion: "logweir.dev/v1alpha1",
      kind: "Restore",
      metadata: { name: reviewed.restoreName },
      spec: {
        planBytes: reviewed.bytes,
        approvalRef: { name: reviewed.approvalName },
        sourceArchive: { url: "s3://kafka-backups/orders" },
        backupSetRef: "01JB7Z0000000000000000000B",
        pointInTime: "2026-09-11T12:00:00Z",
        target: {
          clusterRef: { name: "orders-scratch" }, mode: "scratch",
          topicNaming: { prefix: "drill-20260911-" },
        },
        deadlineSeconds: 3600,
      },
    });
    const sent = JSON.parse(wire.seen[0].init.body);
    assert.equal(sent.planBytes, reviewed.bytes, "byte for byte the document that was reviewed");
    assert.equal(sent.planHash, reviewed.hash, "and the hash that was shown beside it");
    assert.equal(
      await preparePlanDocument(fields),
      reviewed,
      "preparing the same document twice is the SAME OBJECT, so a re-render on submit is not " +
        "an equal string -- it is a different object and this assertion fails",
    );
  } finally {
    wire.restore();
  }
});

test("plan_bytes_this_page_never_prepared_are_refused_before_anything_is_sent", async () => {
  await console_();
  const wire = transport(() => ({ status: 201, body: fixture("console/restore.json") }));
  try {
    await assert.rejects(
      () => apiClient().create("team-a", "restores", {
        apiVersion: "logweir.dev/v1alpha1", kind: "Restore",
        metadata: { name: "restore-deadbeef" },
        spec: {
          planBytes: "name: \"a document from somewhere else\"\n",
          approvalRef: { name: "approval-deadbeef" },
          sourceArchive: { url: "s3://kafka-backups/orders" },
          backupSetRef: "01JB7Z0000000000000000000B",
          pointInTime: "2026-09-11T12:00:00Z",
          target: {
            clusterRef: { name: "orders-scratch" }, mode: "scratch",
            topicNaming: { prefix: "drill-" },
          },
          deadlineSeconds: 3600,
        },
      }),
      (error) => {
        assert.equal(error.reason, "NoConsoleRoute");
        assert.match(error.message, /not prepared by this page's plan module/);
        return true;
      },
    );
    assert.equal(wire.seen.length, 0);
  } finally {
    wire.restore();
  }
});

// ================================================================ the storage

test("no_token_from_the_session_is_ever_written_to_browser_storage", async () => {
  const document = SESSION();
  document.csrfToken = "a-synchroniser-token";
  await console_(document);
  const writes = [];
  const store = { setItem: (k, v) => writes.push([k, v]), getItem: () => null };
  const originals = {
    local: Object.getOwnPropertyDescriptor(globalThis, "localStorage"),
    sessionStore: Object.getOwnPropertyDescriptor(globalThis, "sessionStorage"),
    doc: Object.getOwnPropertyDescriptor(globalThis, "document"),
  };
  const cookies = [];
  Object.defineProperty(globalThis, "localStorage", { value: store, configurable: true });
  Object.defineProperty(globalThis, "sessionStorage", { value: store, configurable: true });
  Object.defineProperty(globalThis, "document", {
    value: { set cookie(v) { cookies.push(v); }, get cookie() { return ""; } },
    configurable: true,
  });
  const wire = transport((u, init) =>
    init.method === "POST"
      ? { status: 201, body: fixture("console/connection.json") }
      : { status: 200, body: fixture("console/connections-list.json") });
  try {
    const api = apiClient();
    await api.list("team-a", "kafkaclusters");
    await api.create("team-a", "kafkaclusters", clusterBody(VALUES));
    assert.deepEqual(writes, [], "nothing was written to browser storage");
    assert.deepEqual(cookies, [], "and this page writes no cookie");
    assert.equal(
      wire.seen[1].init.headers["X-CSRF-Token"],
      "a-synchroniser-token",
      "the token travels on the unsafe request and lives nowhere but this module's memory",
    );
    assert.equal(
      wire.seen[0].init.headers,
      undefined,
      "and a read carries no header at all",
    );
  } finally {
    wire.restore();
    for (const [name, descriptor] of [["localStorage", originals.local],
      ["sessionStorage", originals.sessionStore], ["document", originals.doc]]) {
      if (descriptor === undefined) {
        delete globalThis[name];
      } else {
        Object.defineProperty(globalThis, name, descriptor);
      }
    }
  }
});

// ================================================================ legacy mode

test("legacy_mode_reads_the_kubernetes_api_and_hands_the_object_through_unchanged", async () => {
  await legacy();
  const list = fixture("preview/namespaces/default/kafkaclusters.json");
  const wire = transport(() => ({ status: 200, body: list }));
  try {
    const collection = await apiClient().list("default", "kafkaclusters");
    assert.equal(wire.seen[0].url, "/apis/logweir.dev/v1alpha1/namespaces/default/kafkaclusters");
    assert.deepEqual(collection, list, "what the API server sent is what the page reads");
    assert.equal(collection.items[0].__contract, undefined, "and nothing was added to it");
  } finally {
    wire.restore();
  }
});

test("a_legacy_response_that_is_not_the_contract_is_a_contract_failure_the_page_renders", async () => {
  await legacy();
  const wire = transport(() => ({ status: 200, body: { items: [{ metadata: { name: "x" } }] } }));
  try {
    await assert.rejects(
      () => apiClient().list("default", "kafkaclusters"),
      (error) => {
        assert.ok(isContractFailure(error), "not an empty table: a named failure");
        assert.equal(error.contract.path, "spec");
        return true;
      },
    );
  } finally {
    wire.restore();
  }
});

// ====================================== every mount half goes through the client

test("every_page_s_mount_half_reads_through_the_one_client_and_renders", async () => {
  // THE MUTANT THIS EXISTS FOR IS ONE THAT ACTUALLY HAPPENED. Pointing the
  // pages at `ui/client.js` replaced their `import { get, list }` with one
  // import of the adapter, and two call sites kept calling the bare
  // identifiers -- which are no longer defined. Every page module still
  // PARSED, every render test still passed (they call the render halves
  // directly), and the page showed `list is not defined` in an error box. The
  // whole behaviour suite was green. This drives the MOUNT halves.
  await legacy();
  const pages = await Promise.all([
    import("../pages/clusters.js"),
    import("../pages/schedules.js"),
    import("../pages/backups.js"),
    import("../pages/history.js"),
    import("../pages/keys.js"),
  ]);
  const [clusters, schedules, backups, history, keys] = pages;
  const preview = (name) => fixture("preview/" + name);
  const wire = transport((u) => {
    if (u.indexOf("/trustrosters") !== -1) {
      return { status: 200, body: preview("trustrosters.json") };
    }
    for (const [plural, file] of [
      ["kafkaclusters", "namespaces/default/kafkaclusters.json"],
      ["backupschedules", "namespaces/default/backupschedules.json"],
      ["backups", "namespaces/default/backups.json"],
      ["restores", "namespaces/default/restores.json"],
      ["approvals", "namespaces/default/approvals.json"],
    ]) {
      if (u.indexOf("/" + plural) !== -1) {
        const list = preview(file);
        const name = u.split("/" + plural + "/")[1];
        return { status: 200, body: name === undefined ? list : list.items[0] };
      }
    }
    return undefined;
  });
  // The tiny surface a mount half touches: what `render.replace` needs, plus
  // the two lookups a form's wiring makes. Finding no control is a rendered
  // page with nothing to wire, which is what a DOM-free mount of a form page
  // is.
  function fakeNode() {
    return {
      children: [],
      get firstChild() { return this.children.length === 0 ? null : this.children[0]; },
      removeChild() { return this.children.shift(); },
      appendChild(child) { this.children.push(child); return child; },
      querySelector() { return null; },
      querySelectorAll() { return []; },
    };
  }
  const parse = (html) => [{ html: html }];
  const lifecycle = { signal: undefined, isCurrent: () => true };
  const mounts = [
    ["clusters", (n) => clusters.mountClusters(n, "default", parse, lifecycle)],
    ["cluster detail", (n) => clusters.mountClusterDetail(n, "default", "orders-prod", parse, lifecycle)],
    ["schedules", (n) => schedules.mountSchedules(n, "default", parse, lifecycle)],
    ["backups", (n) => backups.mountBackups(n, "default", parse, lifecycle)],
    ["backup detail", (n) =>
      backups.mountBackupDetail(n, "default", "orders-hourly-20260912-080000", parse, lifecycle)],
    ["history", (n) => history.mountHistory(n, "default", parse, lifecycle)],
    ["keys", (n) => keys.mountKeys(n, parse, undefined, lifecycle)],
  ];
  try {
    for (const [name, mount] of mounts) {
      const target = fakeNode();
      await mount(target);
      const html = (target.children[target.children.length - 1] || {}).html || "";
      assert.ok(
        html.indexOf("class=\"error\"") === -1,
        name + " rendered an error box instead of its view: " + html.slice(0, 300),
      );
      assert.ok(html.length > 200, name + " rendered nothing worth looking at");
    }
    assert.ok(wire.seen.length >= mounts.length, "each mount reached the transport");
    for (const request of wire.seen) {
      assert.ok(
        request.url.indexOf("/apis/logweir.dev/v1alpha1/") === 0,
        "and every one of them addressed the mode that was selected: " + request.url,
      );
    }
  } finally {
    wire.restore();
  }
});


// =========================== the second read of a detail, and the whole list

test("a_drifted_operation_dto_is_a_rendered_contract_failure_and_not_an_empty_evidence_block", async () => {
  // THE HOLE THE REVIEW FOUND. `enrich` swallowed every failure of the
  // operation route, contract failures included, so a server that renamed
  // `verification.state` produced a backup detail with NO evidence block and
  // no error -- indistinguishable from "the controller recorded nothing",
  // which is the exact cell `ui/contract.js`'s header says this client exists
  // to stop and `ui/README.md` promises is never absorbed.
  await console_();
  const drifted = fixture("console/operation-backup.json");
  drifted.item.verification.verdict = drifted.item.verification.state;
  delete drifted.item.verification.state;
  const wire = transport((u) =>
    u.indexOf("/operations/") !== -1
      ? { status: 200, body: drifted }
      : { status: 200, body: fixture("console/backup.json") });
  try {
    await assert.rejects(
      () => apiClient().get("team-a", "backups", "orders-hourly-20260912-080000"),
      (error) => {
        assert.ok(isContractFailure(error), "it is a contract failure");
        assert.equal(error.contract.dto, "OperationVerification");
        assert.equal(error.contract.path, "item.verification.state");
        return true;
      },
    );
  } finally {
    wire.restore();
  }
});

test("a_detail_s_second_read_that_is_merely_refused_still_renders_the_object", async () => {
  await console_();
  for (const status of [403, 404]) {
    const wire = transport((u) =>
      u.indexOf("/operations/") !== -1
        ? { status: status, body: { type: "t", title: "t", status: status, code: "not_found",
          detail: "no such operation", requestId: "r", retryable: false } }
        : { status: 200, body: fixture("console/backup.json") });
    try {
      const object = await apiClient().get("team-a", "backups", "orders-hourly-20260912-080000");
      assert.equal(object.metadata.name, "orders-hourly-20260912-080000",
        String(status) + ": the detail view stands without the extra");
      assert.equal(object.status.evidence, undefined, "and no evidence was invented");
    } finally {
      wire.restore();
    }
  }
});

test("a_console_list_follows_its_cursor_instead_of_showing_a_prefix", async () => {
  // A console list that stopped at the API's page size would show fewer rows
  // than the legacy mode shows, in the same table, with nothing on screen
  // saying so -- and `#/history` is what somebody reads during an incident.
  await console_();
  const first = fixture("console/connections-list.json");
  first.page = { limit: 200, nextCursor: "opaque-cursor-1", snapshot: "60219" };
  const second = fixture("console/connections-list.json");
  second.items = [second.items[0]];
  second.items[0].name = "orders-third";
  second.items[0].uid = "3b2c3d4e-5f60-4718-8293-a4b5c6d7e8fb";
  second.page = { limit: 200, nextCursor: null, snapshot: "60219" };
  let page = 0;
  const wire = transport(() => ({ status: 200, body: page++ === 0 ? first : second }));
  try {
    const collection = await apiClient().list("team-a", "kafkaclusters");
    assert.equal(wire.seen.length, 2, "the second page was asked for");
    assert.equal(wire.seen[0].url,
      "/api/v1/namespaces/team-a/connections?limit=200",
      "and the first asked for the API's maximum page size");
    assert.equal(wire.seen[1].url,
      "/api/v1/namespaces/team-a/connections?limit=200&cursor=opaque-cursor-1",
      "echoing the cursor exactly as it arrived");
    assert.equal(collection.items.length, 3, "every row is in the table");
    assert.equal(collection.items[2].metadata.name, "orders-third");
    assert.equal(collection.__page.nextCursor, null, "and the list is complete");
  } finally {
    wire.restore();
  }
});

test("a_namespace_larger_than_the_budget_is_refused_by_name_and_never_truncated", async () => {
  await console_();
  const endless = fixture("console/connections-list.json");
  endless.page = { limit: 200, nextCursor: "always-more", snapshot: "1" };
  const wire = transport(() => ({ status: 200, body: endless }));
  try {
    await assert.rejects(
      () => apiClient().list("team-a", "kafkaclusters"),
      (error) => {
        assert.equal(error.reason, "ListTooLarge");
        assert.match(error.message, /is not showing you the first/);
        return true;
      },
      "showing a prefix as if it were the whole namespace is the failure this refuses",
    );
    assert.equal(wire.seen.length, LIST_PAGE_BUDGET, "it stopped at its own budget");
  } finally {
    wire.restore();
  }
});

test("every_request_body_this_client_builds_satisfies_the_published_request_shape", async () => {
  // THE OTHER HALF OF THE CONTRACT, checked on the bytes that actually go out.
  await console_();
  const sent = [];
  const wire = transport((u, init) => {
    if (init.method !== "POST") {
      return { status: 200, body: fixture("console/schedule.json") };
    }
    sent.push({ url: u, body: JSON.parse(init.body) });
    return { status: 201, body: u.indexOf("/connections") !== -1
      ? fixture("console/connection.json")
      : (u.indexOf("/restores") !== -1
        ? fixture("console/restore.json")
        : fixture("console/schedule.json")) };
  });
  const reviewed = await preparePlanDocument(fixture("plan-fields.json"));
  try {
    const api = apiClient();
    await api.create("team-a", "kafkaclusters", clusterBody(VALUES));
    await api.create("team-a", "backupschedules", {
      apiVersion: "logweir.dev/v1alpha1", kind: "BackupSchedule",
      metadata: { name: "orders-hourly" },
      spec: {
        schedule: "0 * * * *", sourceRef: { name: "orders-prod" }, topics: ["orders"],
        archive: { url: "s3://kafka-backups/orders", secretRef: { name: "logweir-s3" } },
        suspend: false, concurrencyPolicy: "Forbid", retention: { keepLast: 3 },
      },
    });
    await api.create("team-a", "restores", {
      apiVersion: "logweir.dev/v1alpha1", kind: "Restore",
      metadata: { name: reviewed.restoreName },
      spec: {
        planBytes: reviewed.bytes, approvalRef: { name: reviewed.approvalName },
        sourceArchive: { url: "s3://kafka-backups/orders" },
        backupSetRef: "01JB7Z0000000000000000000B", pointInTime: "2026-09-11T12:00:00Z",
        target: { clusterRef: { name: "orders-scratch" }, mode: "scratch",
          topicNaming: { prefix: "drill-" } },
        deadlineSeconds: 3600,
      },
    });
    await api.patchSuspend("team-a", "orders-hourly", true);
    assert.equal(sent.length, 4);
    const routes = ["connections", "schedules", "restores", "schedules:set-suspension"];
    for (let i = 0; i < routes.length; i += 1) {
      const decoded = decodeRequest(routes[i], sent[i].body);
      assert.deepEqual(
        decoded.unknown,
        [],
        routes[i] + ": a mutation input carrying a field the schema does not name is a 422 " +
          "from the product API, not a tolerated extra",
      );
    }
  } finally {
    wire.restore();
  }
});


test("the_session_s_roles_and_binding_revision_are_carried_and_never_guessed", async () => {
  // PLAT-17.2 added both to `SessionResponse`, and the drift arm in
  // `contract.spec.js` is what found it. The page must CARRY them, not derive
  // them: inferring "this actor is an operator" from which capability flags
  // happen to be true would be inventing an authorisation decision the server
  // already made -- and would invent a different one the moment a domain's
  // route lands and flips a flag from `implemented && allowed` to true.
  const localAdmin = SESSION();
  await console_(localAdmin);
  assert.deepEqual(
    rolesFor("team-a"), [],
    "localAdmin mode has no roles at all, and empty is that mode -- not 'none of the above'",
  );
  assert.equal(bindingRevision(), "", "and no binding table produced its grants");

  const shared = SESSION();
  shared.bindingRevision = "rev-17";
  shared.namespaces[0].roles = ["approver", "operator"];
  await console_(shared);
  assert.deepEqual(rolesFor("team-a"), ["approver", "operator"], "what the binding table said");
  assert.equal(bindingRevision(), "rev-17");
  assert.deepEqual(rolesFor("team-b"), [], "a namespace with no grant holds no roles");
  assert.equal(
    granted("team-a", "restoreCreate"),
    shared.namespaces[0].capabilities.restoreCreate,
    "and the FLAGS are still what decides whether a call is made; a role decides nothing here",
  );

  await legacy();
  assert.deepEqual(rolesFor("team-a"), [], "legacy mode has no product roles");
  assert.equal(bindingRevision(), "", "and no binding table: the API server's RBAC is the story");
});

test("a_session_grant_that_omits_its_roles_is_a_contract_failure", async () => {
  resetMode();
  const broken = SESSION();
  delete broken.namespaces[0].roles;
  const record = await selectMode({
    probe: async () => ({ ok: true, status: 200, body: broken }),
  });
  assert.equal(
    record.mode,
    LEGACY,
    "a session document that is not one leaves the page in legacy mode rather than half-read",
  );
});
