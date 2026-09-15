// mutation.spec.js -- PLAT-13.2, PLAT-12.1 and PLAT-12.2 under node --test:
// drafts that survive failures, one mutation state, idempotent creates, the
// restore wizard's guided submit, and the approval subject that is shown
// exactly as it is submitted.
//
// HOW THESE ROWS DRIVE THE REAL PAGE CODE WITHOUT A BROWSER. The page modules
// render strings and the mount half adopts them into a node. Here the node is
// a small fake that keeps what was adopted, and answers `querySelector` from
// the NEWEST adopted markup: an `<input>`'s value is whatever the markup says.
// So "the draft survived a re-render" is asserted over the value the re-render
// actually wrote, not over a variable the test holds. The API is a fake
// Kubernetes: an in-memory object store that assigns UIDs, answers a taken
// name with `409 AlreadyExists` and a missing one with `404 NotFound`, and can
// be told to lose a response AFTER it stored the object. Nothing here dials;
// `ui_lint.rs::the_ui_behaviour_suite_never_dials` holds that.
//
// Every row uses its own namespace, because the draft and mutation registries
// are module state that lives as long as the loaded page -- which is the point.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";

import { apiError } from "../api.js";
import { createRouteLifecycle } from "../app.js";
import {
  compareSpec,
  createMutation,
  fieldErrors,
  formKey,
  keepDraft,
  mutationFor,
  readDraft,
} from "../lifecycle.js";
import { PRIVATE_KEY_REFUSAL } from "../render.js";
import { CLUSTER_DRAFT_FIELDS, CLUSTER_FORM, mountClusters } from "../pages/clusters.js";
import { SCHEDULE_FORM, mountSchedules } from "../pages/schedules.js";
import {
  WIZARD_FORM,
  initialState,
  mountRestoreWizard,
  preparePlan,
  submitRestore,
} from "../pages/restore-wizard.js";
import {
  APPROVAL_FORM,
  NO_AWAITING_SENTENCE,
  approvalState,
  formOffered,
  mountApprovals,
  renderApprovalState,
  submitApproval,
  subjectOf,
} from "../pages/approvals.js";
import { mountRestoreDetail } from "../pages/history.js";
import { planHash } from "../plan.js";

const fixture = (name) => JSON.parse(readFileSync(new URL("./fixtures/" + name, import.meta.url)));

// ------------------------------------------------------------ the fake DOM

function decodeEntities(text) {
  return text
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, "\"")
    .replace(/&#39;/g, "'")
    .replace(/&amp;/g, "&");
}

function attributesOf(tag) {
  const out = {};
  const pattern = /([a-zA-Z][\w-]*)(?:="([^"]*)")?/g;
  let match;
  const body = tag.replace(/^<\w+/, "").replace(/>$/, "");
  while ((match = pattern.exec(body)) !== null) {
    out[match[1]] = match[2] === undefined ? "" : decodeEntities(match[2]);
  }
  return out;
}

class FakeElement {
  constructor(tag, attributes, view) {
    this.tagName = tag.toUpperCase();
    this.attributes = Object.assign({}, attributes);
    this.view = view;
    this.children = [];
    this.listeners = [];
    this.value = "";
    this.checked = false;
    this.disabled = "disabled" in this.attributes;
    this.files = [];
  }
  get firstChild() {
    return this.children.length === 0 ? null : this.children[0];
  }
  removeChild(child) {
    this.children.splice(this.children.indexOf(child), 1);
    return child;
  }
  appendChild(child) {
    this.children.push(child);
    if (child && typeof child.html === "string") {
      this.view.adopt(child.html, this.isRoot === true);
    }
    return child;
  }
  addEventListener(type, handler, options) {
    const entry = { type: type, handler: handler };
    this.listeners.push(entry);
    if (options && options.signal) {
      options.signal.addEventListener("abort", () => {
        const at = this.listeners.indexOf(entry);
        if (at !== -1) {
          this.listeners.splice(at, 1);
        }
      }, { once: true });
    }
  }
  dispatch(type, event) {
    const results = [];
    for (const entry of this.listeners.slice()) {
      if (entry.type === type) {
        results.push(entry.handler(event || { preventDefault() {} }));
      }
    }
    return Promise.all(results);
  }
  setAttribute(name, value) {
    this.attributes[name] = String(value);
  }
  getAttribute(name) {
    return name in this.attributes ? this.attributes[name] : null;
  }
  focus() {
    this.view.focused = this;
  }
  querySelector(selector) {
    return this.view.find(selector, this);
  }
  querySelectorAll() {
    return [];
  }
}

/** One page node. `adopt` records markup; `find` answers from the newest
 *  markup that carries what is asked for, building one fake per element per
 *  adoption so listeners stay attached to the element they were added to. */
function fakeView() {
  const view = {
    chunks: [],
    focused: null,
    adopt(html, fromRoot) {
      if (fromRoot) {
        view.chunks = [];
      }
      view.chunks.push({ html: html, elements: null });
    },
    html() {
      return view.chunks.map((c) => c.html).join("");
    },
    elementsOf(chunk) {
      if (chunk.elements !== null) {
        return chunk.elements;
      }
      const elements = [];
      const tag = /<(input|select|textarea|button|fieldset|form|div|section|code|p|pre)\b[^>]*>/g;
      let match;
      while ((match = tag.exec(chunk.html)) !== null) {
        const name = match[1];
        const element = new FakeElement(name, attributesOf(match[0]), view);
        element.start = match.index;
        if (name === "input") {
          element.value = element.attributes.value || "";
          element.checked = "checked" in element.attributes;
        } else if (name === "select") {
          const end = chunk.html.indexOf("</select>", match.index);
          const options = chunk.html.slice(match.index, end);
          const selected = /<option value="([^"]*)" selected>/.exec(options) || /<option value="([^"]*)"/.exec(options);
          element.value = selected === null ? "" : decodeEntities(selected[1]);
        } else if (name === "textarea") {
          const end = chunk.html.indexOf("</textarea>", match.index);
          element.value = decodeEntities(chunk.html.slice(match.index + match[0].length, end));
        } else if (name === "form") {
          element.end = chunk.html.indexOf("</form>", match.index);
        }
        elements.push(element);
      }
      for (const form of elements.filter((e) => e.tagName === "FORM")) {
        const inside = elements.filter((e) => e.start > form.start && e.start < form.end);
        form.elements = {};
        for (const control of inside) {
          if (["INPUT", "SELECT", "TEXTAREA"].includes(control.tagName) && control.attributes.name) {
            form.elements[control.attributes.name] = control;
          }
        }
        form.querySelector = (selector) => {
          const wanted = selector.toUpperCase();
          return inside.find((e) => e.tagName === wanted) || null;
        };
      }
      chunk.elements = elements;
      return elements;
    },
    find(selector) {
      for (let i = view.chunks.length - 1; i >= 0; i -= 1) {
        const elements = view.elementsOf(view.chunks[i]);
        let found;
        if (selector.startsWith("#")) {
          found = elements.find((e) => e.attributes.id === selector.slice(1));
        } else if (selector === "[aria-invalid=\"true\"]") {
          found = elements.find((e) => e.attributes["aria-invalid"] === "true");
        } else {
          found = undefined;
        }
        if (found !== undefined) {
          return found;
        }
      }
      return null;
    },
  };
  const root = new FakeElement("main", {}, view);
  root.isRoot = true;
  root.querySelector = (selector) => view.find(selector);
  root.querySelectorAll = () => [];
  view.root = root;
  return view;
}

const parse = (html) => [{ html: html }];

// --------------------------------------------------------- the fake cluster

function statusError(code, reason, message, details) {
  return apiError(
    { status: code },
    JSON.stringify({ kind: "Status", status: "Failure", code: code, reason: reason, message: message, details: details }),
  );
}

function clone(value) {
  return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
}

/** An in-memory Kubernetes for one test: creates assign a UID, a taken name is
 *  a 409 AlreadyExists, a missing one a 404. `loseNextCreate` stores the object
 *  and then fails the request as a dropped connection would. */
function fakeKubernetes() {
  const objects = new Map();
  let serial = 0;
  const k8s = {
    calls: [],
    objects: objects,
    loseNextCreate: false,
    holdNextCreate: null,
    rejectNextCreate: null,
    key(ns, plural, name) {
      return ns + "/" + plural + "/" + name;
    },
    put(ns, plural, object) {
      const stored = clone(object);
      serial += 1;
      stored.metadata = Object.assign({}, stored.metadata, {
        namespace: ns,
        uid: "uid-" + String(serial),
        creationTimestamp: "2026-09-15T12:00:0" + String(serial % 10) + "Z",
      });
      objects.set(k8s.key(ns, plural, stored.metadata.name), stored);
      return clone(stored);
    },
    count(ns, plural) {
      return Array.from(objects.keys()).filter((k) => k.startsWith(ns + "/" + plural + "/")).length;
    },
    async list(ns, plural) {
      k8s.calls.push({ verb: "list", ns: ns, plural: plural });
      const items = Array.from(objects.entries())
        .filter(([k]) => k.startsWith(ns + "/" + plural + "/"))
        .map(([, v]) => clone(v));
      return { items: items };
    },
    async get(ns, plural, name) {
      k8s.calls.push({ verb: "get", ns: ns, plural: plural, name: name });
      const found = objects.get(k8s.key(ns, plural, name));
      if (found === undefined) {
        throw statusError(404, "NotFound", plural + " \"" + name + "\" not found");
      }
      return clone(found);
    },
    async create(ns, plural, body) {
      k8s.calls.push({ verb: "create", ns: ns, plural: plural, body: clone(body) });
      if (k8s.rejectNextCreate !== null) {
        const rejection = k8s.rejectNextCreate;
        k8s.rejectNextCreate = null;
        throw rejection;
      }
      if (k8s.holdNextCreate !== null) {
        const hold = k8s.holdNextCreate;
        k8s.holdNextCreate = null;
        await hold;
      }
      if (objects.has(k8s.key(ns, plural, body.metadata.name))) {
        throw statusError(409, "AlreadyExists", plural + " \"" + body.metadata.name + "\" already exists", {
          name: body.metadata.name, kind: plural,
        });
      }
      const stored = k8s.put(ns, plural, body);
      if (plural === "backupschedules" && stored.spec.concurrencyPolicy === undefined) {
        stored.spec.concurrencyPolicy = "Forbid";
        objects.set(k8s.key(ns, plural, stored.metadata.name), clone(stored));
      }
      if (k8s.loseNextCreate) {
        k8s.loseNextCreate = false;
        throw new TypeError("Failed to fetch");
      }
      return stored;
    },
    async patchSuspend(ns, name, value) {
      k8s.calls.push({ verb: "patch", ns: ns, name: name, value: value });
      const found = objects.get(k8s.key(ns, "backupschedules", name));
      found.spec.suspend = value;
      return clone(found);
    },
    creates(plural) {
      return k8s.calls.filter((c) => c.verb === "create" && (plural === undefined || c.plural === plural));
    },
  };
  return k8s;
}

function settle() {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

async function settled(times) {
  for (let i = 0; i < (times || 6); i += 1) {
    await settle();
  }
}

function typeCluster(view, values) {
  const form = view.find("#cluster-form");
  for (const [name, value] of Object.entries(values)) {
    if (name === "tls") {
      form.elements.tls.checked = value;
    } else {
      form.elements[name].value = value;
    }
  }
  form.dispatch("input");
  return form;
}

// =================================================================== rows

test("a_draft_keeps_only_declared_fields_and_never_private_key_text", () => {
  const key = formKey("drafts-ns", "any-form");
  const kept = keepDraft(key, {
    name: "orders",
    password: "hunter2",
    note: "-----BEGIN PRIVATE KEY-----\nMIG\n",
    tls: true,
  }, ["name", "note", "tls"]);
  assert.deepEqual(kept, { name: "orders", tls: true },
    "an undeclared field (password) is never captured, and private-key text is dropped even from a declared field");
  assert.deepEqual(readDraft(key), { name: "orders", tls: true });
  assert.equal(readDraft(formKey("other-ns", "any-form")), null, "a draft belongs to its namespace");
  assert.ok(!CLUSTER_DRAFT_FIELDS.some((f) => /pass|secretValue|token/i.test(f)),
    "the cluster form declares no credential field; its Secret field is a NAME");
});

test("a_reload_starts_from_an_empty_draft_and_nothing_touches_browser_storage", async () => {
  const key = formKey("reload-ns", CLUSTER_FORM);
  keepDraft(key, { name: "kept-in-memory" }, CLUSTER_DRAFT_FIELDS);
  assert.equal(readDraft(key).name, "kept-in-memory");
  // A fresh module instance is what a reload gets: the same file, evaluated again.
  const reloaded = await import("../lifecycle.js?reload=" + String(Date.now()));
  assert.equal(reloaded.readDraft(key), null, "drafts live in page memory only; a reload starts empty");

  // AND THE WHOLE FORM JOURNEY NEVER READS OR WRITES BROWSER STORAGE.
  const touched = [];
  for (const name of ["localStorage", "sessionStorage"]) {
    Object.defineProperty(globalThis, name, {
      configurable: true,
      get() {
        touched.push(name);
        throw new Error(name + " was touched");
      },
    });
  }
  try {
    const k8s = fakeKubernetes();
    const view = fakeView();
    const routes = createRouteLifecycle();
    await mountClusters(view.root, "storage-ns", parse, routes.begin(), k8s);
    typeCluster(view, { name: "bad name", servers: "broker:9092" });
    await view.find("#cluster-form").dispatch("submit");
    await settled();
    typeCluster(view, { name: "good-name", servers: "broker:9092" });
    await view.find("#cluster-form").dispatch("submit");
    await settled();
    assert.equal(k8s.count("storage-ns", "kafkaclusters"), 1);
  } finally {
    delete globalThis.localStorage;
    delete globalThis.sessionStorage;
  }
  assert.deepEqual(touched, [], "no browser storage was read or written");
});

test("an_invalid_field_keeps_the_draft_and_sends_nothing", async () => {
  const k8s = fakeKubernetes();
  const view = fakeView();
  const route = createRouteLifecycle().begin();
  await mountClusters(view.root, "invalid-ns", parse, route, k8s);
  typeCluster(view, {
    name: "Not_A_Name", servers: "broker-0.kafka:9092, broker-1.kafka:9092", role: "target",
    username: "kept-user",
  });
  await view.find("#cluster-form").dispatch("submit");
  await settled();

  assert.equal(k8s.creates().length, 0, "the page's own check refused before any request");
  const form = view.find("#cluster-form");
  assert.equal(form.elements.name.value, "Not_A_Name", "the refused value is still in the field");
  assert.equal(form.elements.servers.value, "broker-0.kafka:9092, broker-1.kafka:9092", "and so is every other value");
  assert.equal(form.elements.role.value, "target");
  assert.equal(form.elements.username.value, "kept-user");
  assert.equal(form.elements.name.getAttribute("aria-invalid"), "true", "the field is marked invalid");
  assert.match(view.html(), /id="cluster-name-error"[^>]*>a KafkaCluster name is lowercase/, "with its message beside it");
  assert.equal(form.elements.servers.getAttribute("aria-invalid"), null, "a valid field is not marked");
  assert.equal(view.focused, form.elements.name, "focus lands on the field that needs fixing");
  assert.match(view.html(), /was not created: fix the fields marked below\. Your input is kept\./);
});

test("an_api_422_puts_each_cause_beside_its_field_and_keeps_the_draft", async () => {
  const k8s = fakeKubernetes();
  k8s.rejectNextCreate = statusError(422, "Invalid", "KafkaCluster.logweir.dev \"orders\" is invalid", {
    name: "orders",
    causes: [
      { reason: "FieldValueInvalid", message: "Invalid value: \"x\": spec.role in body should be at least 1 chars long", field: "spec.role" },
      { reason: "FieldValueForbidden", message: "Forbidden: something else entirely", field: "spec.unknownThing" },
    ],
  });
  const view = fakeView();
  await mountClusters(view.root, "api422-ns", parse, createRouteLifecycle().begin(), k8s);
  typeCluster(view, { name: "orders", servers: "broker:9092", role: "source" });
  await view.find("#cluster-form").dispatch("submit");
  await settled();
  const form = view.find("#cluster-form");
  assert.equal(form.elements.name.value, "orders");
  assert.equal(form.elements.role.getAttribute("aria-invalid"), "true", "spec.role's cause is beside the role field");
  assert.ok(view.html().includes("spec.role in body should be at least 1 chars long"), "verbatim");
  assert.ok(view.html().includes("spec.unknownThing: Forbidden: something else entirely"),
    "a cause no field claims is still shown, beside the form");
  assert.ok(view.html().includes("422 Invalid"), "with the API server's own status and reason");
});

test("a_network_failure_keeps_the_draft_and_says_the_outcome_is_unknown", async () => {
  const k8s = fakeKubernetes();
  k8s.rejectNextCreate = new TypeError("Failed to fetch");
  const view = fakeView();
  await mountClusters(view.root, "network-ns", parse, createRouteLifecycle().begin(), k8s);
  typeCluster(view, { name: "orders", servers: "broker:9092", secret: "kafka-reader" });
  await view.find("#cluster-form").dispatch("submit");
  await settled();
  const form = view.find("#cluster-form");
  assert.equal(form.elements.name.value, "orders", "kept");
  assert.equal(form.elements.secret.value, "kafka-reader", "kept");
  assert.match(view.html(), /whether KafkaCluster orders was created is unknown\. Your input is kept\. Submitting again is safe/);
  const state = mutationFor(formKey("network-ns", CLUSTER_FORM)).state;
  assert.equal(state.phase, "failed");
  assert.equal(state.kind, "unknown");
  assert.equal(form.querySelector("fieldset").disabled, false, "and the form can be submitted again");
});

test("a_timeout_keeps_the_draft_and_a_late_answer_still_settles_the_record", async () => {
  const ns = "timeout-ns";
  let fire = null;
  mutationFor(formKey(ns, CLUSTER_FORM), {
    timeoutMs: 1000,
    setTimer: (fn) => { fire = fn; return 1; },
    clearTimer: () => {},
  });
  const k8s = fakeKubernetes();
  let release;
  k8s.holdNextCreate = new Promise((resolve) => { release = resolve; });
  const view = fakeView();
  await mountClusters(view.root, ns, parse, createRouteLifecycle().begin(), k8s);
  typeCluster(view, { name: "slow", servers: "broker:9092" });
  view.find("#cluster-form").dispatch("submit");
  await settled();
  assert.equal(mutationFor(formKey(ns, CLUSTER_FORM)).state.phase, "pending");
  assert.equal(view.find("#cluster-form").querySelector("fieldset").disabled, true, "pending disables the form");

  fire();
  await settled();
  const timedOut = mutationFor(formKey(ns, CLUSTER_FORM)).state;
  assert.equal(timedOut.kind, "unknown");
  assert.equal(timedOut.timedOut, true);
  assert.equal(view.find("#cluster-form").elements.name.value, "slow", "the draft survives the timeout");
  assert.match(view.html(), /The API server did not answer in time/);

  release();
  await settled();
  assert.equal(mutationFor(formKey(ns, CLUSTER_FORM)).state.phase, "succeeded",
    "the request was never cancelled, and its late answer replaces the timeout");
  assert.equal(readDraft(formKey(ns, CLUSTER_FORM)), null, "the draft is consumed by the object it made");
  assert.equal(k8s.count(ns, "kafkaclusters"), 1);
});

test("a_double_click_creates_one_request", async () => {
  const k8s = fakeKubernetes();
  let release;
  k8s.holdNextCreate = new Promise((resolve) => { release = resolve; });
  const view = fakeView();
  await mountClusters(view.root, "double-ns", parse, createRouteLifecycle().begin(), k8s);
  const form = typeCluster(view, { name: "once", servers: "broker:9092" });
  form.dispatch("submit");
  form.dispatch("submit");
  // The first submit re-rendered the form; a click on the re-rendered one too.
  view.find("#cluster-form").dispatch("submit");
  await settled();
  release();
  await settled();
  assert.equal(k8s.creates("kafkaclusters").length, 1, "three submits while pending, one POST");
  assert.equal(k8s.count("double-ns", "kafkaclusters"), 1);

  // The record's own guard, independent of any button.
  // Inert timers: this executor never settles, and a real 30 s timer would
  // hold the test process open for no reason.
  const record = createMutation({ setTimer: () => 0, clearTimer: () => {} });
  let calls = 0;
  const first = record.run(() => { calls += 1; return new Promise(() => {}); });
  const second = record.run(() => { calls += 1; return Promise.resolve(); });
  assert.ok(first instanceof Promise);
  assert.equal(second, null, "a second run while pending is refused without calling its executor");
  assert.equal(calls, 1);
});

test("a_retry_after_a_lost_response_reuses_the_name_and_resolves_to_the_existing_object", async () => {
  const k8s = fakeKubernetes();
  k8s.loseNextCreate = true;
  const view = fakeView();
  await mountClusters(view.root, "lost-ns", parse, createRouteLifecycle().begin(), k8s);
  typeCluster(view, { name: "orders", servers: "broker:9092" });
  await view.find("#cluster-form").dispatch("submit");
  await settled();
  assert.equal(k8s.count("lost-ns", "kafkaclusters"), 1, "the first request DID create the object");
  assert.equal(mutationFor(formKey("lost-ns", CLUSTER_FORM)).state.kind, "unknown", "but its answer never arrived");
  const uid = k8s.objects.get("lost-ns/kafkaclusters/orders").metadata.uid;

  await view.find("#cluster-form").dispatch("submit");
  await settled();
  const creates = k8s.creates("kafkaclusters");
  assert.equal(creates.length, 2, "the retry was sent");
  assert.equal(creates[1].body.metadata.name, creates[0].body.metadata.name, "under the same name");
  assert.equal(k8s.count("lost-ns", "kafkaclusters"), 1, "and made no second object");
  const state = mutationFor(formKey("lost-ns", CLUSTER_FORM)).state;
  assert.equal(state.phase, "succeeded");
  assert.equal(state.result.outcome, "existing");
  assert.equal(state.result.object.metadata.uid, uid, "it resolved to the object the lost request made");
  assert.match(view.html(), new RegExp("KafkaCluster orders already existed with exactly this content \\(uid " + uid + "\\); nothing new was created"));
});

test("already_exists_with_different_content_is_a_conflict_and_nothing_is_overwritten", async () => {
  const k8s = fakeKubernetes();
  k8s.put("conflict-ns", "kafkaclusters", {
    apiVersion: "logweir.dev/v1alpha1", kind: "KafkaCluster",
    metadata: { name: "orders" },
    spec: { bootstrapServers: ["somebody-else:9092"], auth: { mode: "plaintext", tls: false }, role: "source" },
  });
  const before = clone(k8s.objects.get("conflict-ns/kafkaclusters/orders"));
  const view = fakeView();
  await mountClusters(view.root, "conflict-ns", parse, createRouteLifecycle().begin(), k8s);
  typeCluster(view, { name: "orders", servers: "mine:9092" });
  await view.find("#cluster-form").dispatch("submit");
  await settled();
  const state = mutationFor(formKey("conflict-ns", CLUSTER_FORM)).state;
  assert.equal(state.kind, "conflict");
  assert.deepEqual(state.error.differences, ["spec.bootstrapServers[0]"]);
  assert.deepEqual(k8s.objects.get("conflict-ns/kafkaclusters/orders"), before, "the existing object is untouched");
  assert.equal(view.find("#cluster-form").elements.servers.value, "mine:9092", "the draft is kept");
  assert.match(view.html(), /KafkaCluster orders already exists with different content \(uid uid-1\); nothing was changed\. Your input is kept\. It differs at: spec\.bootstrapServers\[0\]\./);
});

test("success_consumes_the_draft_and_the_new_object_is_listed", async () => {
  const k8s = fakeKubernetes();
  const view = fakeView();
  const route = createRouteLifecycle().begin();
  await mountClusters(view.root, "success-ns", parse, route, k8s);
  typeCluster(view, { name: "orders", servers: "broker:9092" });
  assert.equal(readDraft(formKey("success-ns", CLUSTER_FORM)).name, "orders", "typing keeps a draft");
  await view.find("#cluster-form").dispatch("submit");
  await settled();
  assert.equal(readDraft(formKey("success-ns", CLUSTER_FORM)), null);
  assert.match(view.html(), />orders<\/a>/, "the list was read again and names the new cluster");
  assert.match(view.html(), /Created KafkaCluster orders \(uid uid-1\)\./);
  assert.equal(view.find("#cluster-form").elements.name.value, "", "the form is empty for the next one");
});

test("a_schedule_retry_ignores_the_server_default_and_a_later_suspension", () => {
  const submitted = {
    schedule: "0 * * * *", sourceRef: { name: "demo" }, topics: ["orders"],
    archive: { url: "s3://b/p", secretRef: { name: "logweir-s3" } }, suspend: false,
  };
  const stored = Object.assign(clone(submitted), { concurrencyPolicy: "Forbid", suspend: true, retention: null });
  const rules = { defaults: { concurrencyPolicy: "Forbid", suspend: false }, ignore: ["suspend"] };
  assert.deepEqual(compareSpec(submitted, stored, rules), { equal: true, differences: [] });
  stored.topics = ["orders", "payments"];
  assert.deepEqual(compareSpec(submitted, stored, rules).differences, ["spec.topics"]);
  assert.equal(compareSpec({ planBytes: "a  \n" }, { planBytes: "a\n" }).equal, false,
    "strings compare byte for byte: trailing whitespace is a difference");
});

test("the_schedule_form_keeps_its_draft_when_a_suspend_toggle_fails", async () => {
  const k8s = fakeKubernetes();
  k8s.put("toggle-ns", "backupschedules", {
    apiVersion: "logweir.dev/v1alpha1", kind: "BackupSchedule", metadata: { name: "hourly" },
    spec: { schedule: "0 * * * *", sourceRef: { name: "demo" }, topics: ["orders"], archive: { url: "s3://b/p" }, suspend: false },
  });
  const view = fakeView();
  await mountSchedules(view.root, "toggle-ns", parse, createRouteLifecycle().begin(), k8s);
  const form = view.find("#schedule-form");
  form.elements.name.value = "nightly";
  form.elements.topics.value = "orders, payments";
  form.dispatch("input");
  assert.deepEqual(
    [readDraft(formKey("toggle-ns", SCHEDULE_FORM)).name, readDraft(formKey("toggle-ns", SCHEDULE_FORM)).topics],
    ["nightly", "orders, payments"],
  );
  await view.find("#schedule-form").dispatch("submit");
  await settled();
  // The draft is invalid (no source, no archive): refused, with every value kept.
  const again = view.find("#schedule-form");
  assert.equal(again.elements.name.value, "nightly");
  assert.equal(again.elements.topics.value, "orders, payments");
  assert.equal(again.elements.source.getAttribute("aria-invalid"), "true");
  assert.equal(k8s.creates().length, 0);
});

// ------------------------------------------------------ the restore wizard

function wizardKubernetes(ns) {
  const k8s = fakeKubernetes();
  for (const cluster of fixture("wizard-clusters.json").items) {
    k8s.put(ns, "kafkaclusters", cluster);
  }
  for (const backup of fixture("wizard-backups.json").items) {
    k8s.put(ns, "backups", backup);
  }
  return k8s;
}

function wizardState(ns) {
  return initialState(ns, fixture("wizard-clusters.json"), fixture("wizard-backups.json"));
}

test("the_guided_submit_routes_an_unapproved_restore_to_awaiting_approval", async () => {
  const ns = "guided-await-ns";
  const k8s = wizardKubernetes(ns);
  const state = wizardState(ns);
  const prepared = await preparePlan(state);
  const result = await submitRestore(state, k8s, undefined, { reviewedHash: prepared.hash });
  assert.equal(result.outcome, "created");
  assert.equal(k8s.count(ns, "restores"), 1);
  assert.equal(
    result.route,
    "#/approvals?subject=" + prepared.restoreName + "&hash=" + encodeURIComponent(prepared.hash) +
      "&name=" + prepared.approvalName + "&ns=" + ns,
    "no Approval authorises it, so it lands on its approval page with the reviewed identity",
  );
});

test("the_guided_submit_routes_to_the_operation_view_when_an_approval_authorises_exactly_this_restore", async () => {
  const ns = "guided-durable-ns";
  const k8s = wizardKubernetes(ns);
  const state = wizardState(ns);
  const prepared = await preparePlan(state);
  const first = await submitRestore(state, k8s, undefined, { reviewedHash: prepared.hash });
  const restore = first.object;
  const approval = {
    apiVersion: "logweir.dev/v1alpha1", kind: "Approval", metadata: { name: prepared.approvalName },
    spec: { subjectRef: { kind: "Restore", name: restore.metadata.name }, planHash: prepared.hash, approvalBytes: "{}", sidecarBytes: "{}" },
  };
  k8s.put(ns, "approvals", approval);
  const stored = k8s.objects.get(ns + "/approvals/" + prepared.approvalName);
  stored.status = {
    verified: true, matchedKeyId: "abc",
    verifiedSubjectRef: { apiVersion: "logweir.dev/v1alpha1", kind: "Restore", name: restore.metadata.name, namespace: ns, uid: restore.metadata.uid },
    conditions: [{ type: "Verified", status: "True", reason: "Verified" }],
  };

  // Submitting the same reviewed plan again -- a retry, a second click after a
  // reload -- resolves to the Restore that exists, and now opens its operation.
  const again = await submitRestore(wizardState(ns), k8s, undefined, { reviewedHash: prepared.hash });
  assert.equal(again.outcome, "existing");
  assert.equal(again.object.metadata.uid, restore.metadata.uid);
  assert.equal(k8s.count(ns, "restores"), 1, "no second Restore");
  assert.equal(again.route, "#/history?ns=" + ns + "&name=" + restore.metadata.name);

  // A verified Approval for ANOTHER execution of that name is not this one's.
  stored.status.verifiedSubjectRef.uid = "uid-of-an-older-restore";
  const foreign = await submitRestore(wizardState(ns), k8s, undefined, { reviewedHash: prepared.hash });
  assert.ok(foreign.route.startsWith("#/approvals?subject="), "no path reuses an approval bound to another execution");
});

test("a_plan_changed_after_review_is_refused_and_nothing_is_sent", async () => {
  const ns = "reviewed-ns";
  const k8s = wizardKubernetes(ns);
  const state = wizardState(ns);
  const shown = await preparePlan(state);
  state.fields.target.topicPrefix = "changed-after-review-";
  await assert.rejects(
    submitRestore(state, k8s, undefined, { reviewedHash: shown.hash }),
    (error) => error.kind === "refused" && /the plan changed after it was displayed/.test(error.message),
  );
  assert.equal(k8s.creates().length, 0);
});

test("a_lost_restore_response_retried_resolves_to_the_same_restore_and_a_conflict_is_not_overwritten", async () => {
  const ns = "wizard-lost-ns";
  const k8s = wizardKubernetes(ns);
  const state = wizardState(ns);
  const prepared = await preparePlan(state);
  k8s.loseNextCreate = true;
  await assert.rejects(submitRestore(state, k8s, undefined, { reviewedHash: prepared.hash }), TypeError);
  assert.equal(k8s.count(ns, "restores"), 1, "the lost request created it");
  const retried = await submitRestore(state, k8s, undefined, { reviewedHash: prepared.hash });
  assert.equal(retried.outcome, "existing");
  assert.equal(k8s.count(ns, "restores"), 1, "the retry did not duplicate it");
  assert.equal(k8s.creates("restores")[1].body.metadata.name, prepared.restoreName);

  // The same minted name carrying a different spec is a conflict. The archive
  // credential's NAME is on Restore.spec and not in the plan bytes, so it
  // changes the spec without changing the minted name.
  const occupiedNs = "wizard-conflict-ns";
  const k8s2 = wizardKubernetes(occupiedNs);
  const other = wizardState(occupiedNs);
  const otherPrepared = await preparePlan(other);
  const occupied = await submitRestore(other, k8s2, undefined, { reviewedHash: otherPrepared.hash });
  const before = clone(k8s2.objects.get(occupiedNs + "/restores/" + occupied.object.metadata.name));
  other.archiveSecretName = "a-different-secret";
  const samePlan = await preparePlan(other);
  assert.equal(samePlan.restoreName, otherPrepared.restoreName, "the same bytes mint the same name");
  await assert.rejects(
    submitRestore(other, k8s2, undefined, { reviewedHash: samePlan.hash }),
    (error) => error.kind === "conflict" && error.differences.includes("spec.sourceArchive.secretRef.name"),
  );
  assert.deepEqual(k8s2.objects.get(occupiedNs + "/restores/" + occupied.object.metadata.name), before, "untouched");
  assert.equal(k8s2.count(occupiedNs, "restores"), 1);
});

test("the_wizard_double_click_sends_one_create_and_success_navigates_to_awaiting_approval", async () => {
  const ns = "wizard-mount-ns";
  const k8s = wizardKubernetes(ns);
  const view = fakeView();
  const originalWindow = globalThis.window;
  globalThis.window = { location: { hash: "#/restore?ns=" + ns } };
  try {
    const route = createRouteLifecycle().begin();
    let release;
    k8s.holdNextCreate = new Promise((resolve) => { release = resolve; });
    await mountRestoreWizard(view.root, ns, parse, k8s, route);
    const button = view.find("#create-restore");
    assert.ok(button !== null, "the one guided submit is rendered");
    assert.equal(view.find("#request-approval"), null, "and there is no second, navigating button");
    button.dispatch("click");
    button.dispatch("click");
    await settled();
    assert.equal(view.find("#create-restore").disabled, true, "pending disables the button in place");
    release();
    await settled(12);
    assert.equal(k8s.creates("restores").length, 1, "two clicks, one create");
    assert.ok(window.location.hash.startsWith("#/approvals?subject=restore-"), "it ended on Awaiting approval: " + window.location.hash);
    assert.equal(readDraft(formKey(ns, WIZARD_FORM)), null);
  } finally {
    globalThis.window = originalWindow;
  }
});

test("a_rejected_restore_keeps_every_wizard_edit_and_marks_the_field", async () => {
  const ns = "wizard-422-ns";
  const k8s = wizardKubernetes(ns);
  const view = fakeView();
  const route = createRouteLifecycle().begin();
  await mountRestoreWizard(view.root, ns, parse, k8s, route);
  const prefix = view.find("#topic-prefix");
  prefix.value = "incident-4471-";
  await prefix.dispatch("change");
  await settled();
  assert.equal(readDraft(formKey(ns, WIZARD_FORM)).topicPrefix, "incident-4471-", "the edit is a draft at once");
  k8s.rejectNextCreate = statusError(422, "Invalid", "Restore.logweir.dev is invalid", {
    causes: [{ reason: "FieldValueInvalid", field: "spec.target.topicNaming.prefix", message: "prefix refused by a policy" }],
  });
  await view.find("#create-restore").dispatch("click");
  await settled(12);
  assert.equal(view.find("#topic-prefix").value, "incident-4471-", "the edit survived the refusal");
  assert.equal(view.find("#topic-prefix").getAttribute("aria-invalid"), "true");
  assert.ok(view.html().includes("prefix refused by a policy"));

  // Leaving and coming back in the same page keeps it too; a draft for another
  // backup set would not be applied.
  const view2 = fakeView();
  await mountRestoreWizard(view2.root, ns, parse, k8s, createRouteLifecycle().begin());
  assert.equal(view2.find("#topic-prefix").value, "incident-4471-");
  assert.ok(view2.html().includes("Your unsubmitted edits to this plan"));
});

// ------------------------------------------------------ approvals, PLAT-12.2

async function restoreAwaitingApproval(ns) {
  const k8s = wizardKubernetes(ns);
  const state = wizardState(ns);
  const prepared = await preparePlan(state);
  const created = await submitRestore(state, k8s, undefined, { reviewedHash: prepared.hash });
  return { k8s: k8s, prepared: prepared, restore: created.object };
}

test("a_standalone_approvals_visit_offers_the_waiting_restores_or_says_there_are_none", async () => {
  const empty = fakeView();
  await mountApprovals(empty.root, "standalone-empty-ns", { ns: "standalone-empty-ns", subject: "", hash: "", name: "" }, parse, fakeKubernetes(), createRouteLifecycle().begin());
  assert.ok(empty.html().includes(NO_AWAITING_SENTENCE), "a clear empty state");
  assert.equal(empty.find("#approval-form"), null, "and no form that would have to assume a subject");

  const ns = "standalone-ns";
  const { k8s, restore } = await restoreAwaitingApproval(ns);
  const view = fakeView();
  await mountApprovals(view.root, ns, { ns: ns, subject: "", hash: "", name: "" }, parse, k8s, createRouteLifecycle().begin());
  assert.ok(
    view.html().includes("<a href=\"#/approvals?subject=" + restore.metadata.name + "&amp;ns=" + ns + "\">" + restore.metadata.name + "</a>"),
    "the waiting Restore is offered as a choice",
  );
  assert.equal(view.find("#approval-form"), null, "the choice is made before any form appears");
});

test("the_subject_page_derives_every_submitted_value_from_the_restore", async () => {
  const ns = "subject-ns";
  const { k8s, prepared, restore } = await restoreAwaitingApproval(ns);
  const view = fakeView();
  // A route naming only the Restore: everything else is read and computed.
  await mountApprovals(view.root, ns, { ns: ns, subject: restore.metadata.name, hash: "", name: "" }, parse, k8s, createRouteLifecycle().begin());
  const form = view.find("#approval-form");
  assert.ok(form !== null, "a Restore awaiting approval gets the form");
  const hash = "sha256:" + createHash("sha256").update(restore.spec.planBytes, "utf8").digest("hex");
  assert.equal(form.elements.subjectName.value, restore.metadata.name);
  assert.equal(form.elements.subjectUid.value, restore.metadata.uid);
  assert.equal(form.elements.planHash.value, hash, "the hash of the Restore's own bytes, computed independently here");
  assert.equal(form.elements.approvalName.value, prepared.approvalName);
  for (const name of ["subjectKind", "subjectName", "subjectUid", "planHash", "approvalName"]) {
    assert.ok("readonly" in form.elements[name].attributes, name + " is read-only");
  }
  assert.match(view.html(), /awaiting approval<\/span><p class="note">No Approval named/);

  form.elements.approvalBytes.value = "{\"plan_hash\": \"" + hash + "\"}\n";
  form.elements.sidecarBytes.value = "{\"signatures\": []}\n";
  form.dispatch("input");
  await view.find("#approval-form").dispatch("submit");
  await settled(12);
  const created = k8s.creates("approvals");
  assert.equal(created.length, 1);
  assert.deepEqual(created[0].body.spec.subjectRef, { kind: "Restore", name: restore.metadata.name });
  assert.equal(created[0].body.spec.planHash, hash);
  assert.equal(created[0].body.metadata.name, prepared.approvalName);
  assert.match(view.html(), /awaiting verification/, "the page now shows the recorded, undecided approval");
  assert.equal(view.find("#approval-form"), null, "and offers no second form under the same name");
});

test("an_edited_or_forged_subject_cannot_diverge_from_the_submitted_subject", async () => {
  const ns = "forged-ns";
  const { k8s, restore } = await restoreAwaitingApproval(ns);
  const view = fakeView();
  await mountApprovals(view.root, ns, { ns: ns, subject: restore.metadata.name, hash: "", name: "" }, parse, k8s, createRouteLifecycle().begin());
  const form = view.find("#approval-form");
  // What a devtools edit, or a script, does to a read-only input.
  form.elements.subjectName.value = "restore-somebody-else";
  form.elements.approvalBytes.value = "{}\n";
  form.elements.sidecarBytes.value = "{}\n";
  await form.dispatch("submit");
  await settled(12);
  assert.equal(k8s.creates("approvals").length, 0, "nothing was sent");
  assert.match(view.html(), /Nothing was sent: the approval subject shown on this page \(name restore-somebody-else\) is not the subject it would submit/);

  // The pure half, for every subject value.
  const subject = subjectOf(restore, await planHash(restore.spec.planBytes), ns);
  for (const field of ["kind", "name", "uid", "planHash", "approvalName"]) {
    const displayed = Object.assign({}, subject, { [field]: "forged" });
    await assert.rejects(
      submitApproval(subject, { approvalBytes: "{}", sidecarBytes: "{}" }, k8s, { displayed: displayed }),
      (error) => error.kind === "refused" && error.mismatch === field,
      field,
    );
  }
  assert.equal(k8s.creates("approvals").length, 0);
});

test("a_route_that_disagrees_with_the_restore_is_refused_without_a_form", async () => {
  const ns = "mismatch-ns";
  const { k8s, prepared, restore } = await restoreAwaitingApproval(ns);
  const view = fakeView();
  await mountApprovals(view.root, ns, {
    ns: ns, subject: restore.metadata.name, hash: "sha256:" + "0".repeat(64), name: prepared.approvalName,
  }, parse, k8s, createRouteLifecycle().begin());
  assert.equal(view.find("#approval-form"), null);
  assert.ok(view.html().includes("This link does not match Restore " + restore.metadata.name));
  assert.ok(view.html().includes("plan hash: the link says <code>sha256:" + "0".repeat(64) + "</code>"));

  // A Restore that does not exist gets a sentence, not a form.
  const missing = fakeView();
  await mountApprovals(missing.root, ns, { ns: ns, subject: "restore-nope", hash: "", name: "" }, parse, k8s, createRouteLifecycle().begin());
  assert.equal(missing.find("#approval-form"), null);
  assert.ok(missing.html().includes("No Restore named restore-nope exists"));
});

test("a_restore_recreated_since_the_page_read_it_is_refused", async () => {
  const ns = "recreated-ns";
  const { k8s, restore } = await restoreAwaitingApproval(ns);
  const subject = subjectOf(restore, await planHash(restore.spec.planBytes), ns);
  k8s.objects.get(ns + "/restores/" + restore.metadata.name).metadata.uid = "uid-recreated";
  await assert.rejects(
    submitApproval(subject, { approvalBytes: "{}", sidecarBytes: "{}" }, k8s),
    (error) => error.kind === "refused" && error.mismatch === "uid",
  );
  assert.equal(k8s.creates("approvals").length, 0);
});

test("a_private_key_paste_is_refused_cleared_and_never_kept", async () => {
  const ns = "secret-ns";
  const { k8s, restore } = await restoreAwaitingApproval(ns);
  const view = fakeView();
  await mountApprovals(view.root, ns, { ns: ns, subject: restore.metadata.name, hash: "", name: "" }, parse, k8s, createRouteLifecycle().begin());
  const form = view.find("#approval-form");
  const key = "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg\n-----END PRIVATE KEY-----\n";
  form.elements.approvalBytes.value = key;
  form.elements.sidecarBytes.value = "{\"signatures\": []}\n";
  form.dispatch("input");
  const draftKey = formKey(ns, APPROVAL_FORM, restore.metadata.name);
  assert.equal(JSON.stringify(readDraft(draftKey) || {}).indexOf("PRIVATE KEY"), -1, "typing it never put it in the draft");
  await form.dispatch("submit");
  await settled(12);
  assert.equal(k8s.creates("approvals").length, 0, "nothing was sent");
  assert.equal(k8s.calls.filter((c) => c.plural === "restores" && c.verb === "get").length, 1,
    "not even the subject re-read: only the page's own mount read the Restore");
  assert.equal(view.find("#approval-json").value, "", "the key text is cleared from the field");
  assert.equal(JSON.stringify(readDraft(draftKey) || {}).indexOf("PRIVATE KEY"), -1, "and is not in the draft");
  assert.equal(readDraft(draftKey).sidecarBytes, "{\"signatures\": []}\n", "the other document is kept");
  assert.ok(view.html().includes("Nothing was sent: " + PRIVATE_KEY_REFUSAL));
  assert.equal(view.html().indexOf("MIGHAgEAMBMGByqGSM49"), -1, "the key text is not echoed anywhere");
});

test("an_approval_retry_after_a_lost_response_resolves_to_the_same_approval", async () => {
  const ns = "approval-lost-ns";
  const { k8s, restore } = await restoreAwaitingApproval(ns);
  const subject = subjectOf(restore, await planHash(restore.spec.planBytes), ns);
  const documents = { approvalBytes: "{\"a\": 1}\n", sidecarBytes: "{\"s\": 1}\n" };
  k8s.loseNextCreate = true;
  await assert.rejects(submitApproval(subject, documents, k8s), TypeError);
  const retried = await submitApproval(subject, documents, k8s);
  assert.equal(retried.outcome, "existing");
  assert.equal(k8s.count(ns, "approvals"), 1);
  await assert.rejects(
    submitApproval(subject, { approvalBytes: "{\"a\": 2}\n", sidecarBytes: "{\"s\": 1}\n" }, k8s),
    (error) => error.kind === "conflict" && error.differences.includes("spec.approvalBytes"),
    "different documents under the same name are a conflict, never a silent reuse",
  );
});

test("approval_states_show_pending_denied_expired_and_never_reuse_another_binding", async () => {
  const ns = "states-ns";
  const { restore, prepared } = await restoreAwaitingApproval(ns);
  const subject = subjectOf(restore, prepared.hash, ns);
  const base = {
    metadata: { name: prepared.approvalName },
    spec: { subjectRef: { kind: "Restore", name: restore.metadata.name }, planHash: prepared.hash },
  };
  const bound = { apiVersion: "logweir.dev/v1alpha1", kind: "Restore", name: restore.metadata.name, namespace: ns, uid: restore.metadata.uid };
  const cases = [
    [null, "absent", "awaiting approval"],
    [Object.assign(clone(base), { status: {} }), "awaiting-verification", "awaiting verification"],
    [Object.assign(clone(base), { status: { verified: true, matchedKeyId: "k1", verifiedSubjectRef: bound, conditions: [{ type: "Verified", status: "True", reason: "Verified" }] } }), "verified", "approved: verified by weirkeeper"],
    [Object.assign(clone(base), { status: { verified: false, conditions: [{ type: "Verified", status: "False", reason: "SignatureInvalid", message: "no signature verified" }] } }), "refused", "refused by weirkeeper"],
    [Object.assign(clone(base), { status: { verified: false, conditions: [{ type: "Verified", status: "False", reason: "KeyIdExpired", message: "notAfter passed" }] } }), "expired", "expired"],
    [Object.assign(clone(base), { spec: Object.assign(clone(base.spec), { subjectRef: { kind: "Restore", name: "restore-other" } }) }), "foreign-subject", "bound to another subject"],
    [Object.assign(clone(base), { status: { verified: true, verifiedSubjectRef: Object.assign({}, bound, { uid: "uid-older" }) } }), "foreign-execution", "bound to another execution"],
    [Object.assign(clone(base), { spec: Object.assign(clone(base.spec), { planHash: "sha256:" + "f".repeat(64) }), status: { verified: true, verifiedSubjectRef: bound } }), "plan-mismatch", "names another plan"],
  ];
  for (const [approval, expected, words] of cases) {
    const found = approvalState(approval, subject);
    assert.equal(found.state, expected, expected);
    assert.ok(renderApprovalState(found, subject, prepared.approvalName).includes(">" + words + "</span>"), expected + " in words");
    const offered = formOffered({ restore: restore, subject: subject, found: found, mismatches: [] });
    assert.equal(offered, expected === "absent", expected + ": a form only when no Approval holds the name");
  }

  // The operation view renders the same states, and never a form.
  const k8s = wizardKubernetes(ns);
  k8s.objects.set(ns + "/restores/" + restore.metadata.name, restore);
  k8s.put(ns, "approvals", Object.assign(clone(base), { spec: Object.assign(clone(base.spec), { approvalBytes: "{}", sidecarBytes: "{}" }) }));
  k8s.objects.get(ns + "/approvals/" + prepared.approvalName).status = { verified: true, verifiedSubjectRef: Object.assign({}, bound, { uid: "uid-older" }) };
  const view = fakeView();
  await mountRestoreDetail(view.root, ns, restore.metadata.name, parse, createRouteLifecycle().begin(), k8s);
  assert.ok(view.html().includes("bound to another execution"));
  assert.equal(view.find("#approval-form"), null);
});

test("api_errors_keep_the_status_details_for_field_messages", () => {
  const error = statusError(422, "Invalid", "BackupSchedule is invalid", {
    causes: [{ field: "spec.topics[1]", message: "glob refused", reason: "FieldValueInvalid" }],
  });
  assert.equal(error.status, 422);
  assert.equal(error.reason, "Invalid");
  assert.deepEqual(fieldErrors(error, [["spec.topics", "topics"]]), {
    fields: { topics: ["glob refused"] },
    unmatched: [],
  });
});
