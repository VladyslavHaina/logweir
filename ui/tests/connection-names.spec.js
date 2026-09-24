// connection-names.spec.js -- defect P7 (poc-install, 2026-09-24): in console
// (shared) mode the Clusters form's "name" field was silently discarded.
//
// The product API's connection create has no name member and mints
// `conn-<26 base32>` from the request's idempotency scope (`docs/api.md`), so
// an operator who typed `source` got `conn-u5cb27icdjt5vdf6sc4wehb4nb` and was
// told nothing. The console now offers no choice it cannot honour: in console
// mode there is no name input, the form says who names the connection, and an
// intent held in the draft is what makes a double click or a retry ONE
// connection. Legacy (kubectl proxy) mode keeps its name field -- there the
// typed name IS the object's name.
//
// Every row carries its NEGATIVE CONTROL: the assertion the code before P7
// fails.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { resetMode, selectMode } from "../client.js";
import { dropDraft, formKey, readDraft } from "../lifecycle.js";
import {
  CLUSTER_DRAFT_FIELDS,
  CLUSTER_FORM,
  CONNECTION_NAME_MINTED_SENTENCE,
  mintConnectionIntent,
  mountClusters,
  renderClusterForm,
  submitCluster,
} from "../pages/clusters.js";
import { LIFE, fakeView, parse } from "./fake-view.js";

const fixture = (name) =>
  JSON.parse(readFileSync(new URL("./fixtures/" + name, import.meta.url), "utf8"));

async function consoleMode() {
  resetMode();
  await selectMode({
    probe: async () => ({ ok: true, status: 200, body: fixture("console/session.json") }),
  });
}

const VALUES = Object.freeze({
  name: "", servers: "kafka-source:9092", role: "source", mode: "plaintext", username: "",
  secret: "", passwordKey: "", tls: false, tlsCaKind: "none", tlsCaName: "", tlsCaKey: "",
});

test("the_console_form_offers_no_name_the_server_would_discard", () => {
  const consoleForm = renderClusterForm({ minted: true });
  assert.doesNotMatch(consoleForm, /id="cluster-name"/,
    "NEGATIVE CONTROL: no name input in console mode (the form before P7 always rendered one)");
  assert.match(consoleForm, /id="cluster-name-minted"/);
  assert.ok(consoleForm.includes("conn- followed by 26 characters"),
    "the form says who names the connection and what the name looks like");
  assert.match(CONNECTION_NAME_MINTED_SENTENCE, /shown below once the connection exists/);

  // LEGACY MODE KEEPS ITS FIELD: behind kubectl proxy the typed name is the
  // object's name, and the API server honours it.
  const legacyForm = renderClusterForm({});
  assert.match(legacyForm, /<input id="cluster-name" name="name" required/);
  assert.doesNotMatch(legacyForm, /id="cluster-name-minted"/);
});

test("a_console_create_needs_no_name_and_sends_the_draft_intent_as_its_key_seed", async () => {
  await consoleMode();
  try {
    const sent = [];
    const api = {
      create: async (ns, plural, body) => {
        sent.push(body);
        return { metadata: { name: "conn-u5cb27icdjt5vdf6sc4wehb4nb", uid: "u" }, __contract: {} };
      },
    };
    const intent = mintConnectionIntent();
    assert.match(intent, /^logweir-ui\.connection\.[0-9a-f]{32}$/);
    assert.notEqual(mintConnectionIntent(), intent, "random, never a counter");
    const made = await submitCluster("team-a", Object.assign({}, VALUES, { intent: intent }), api);
    assert.equal(made.object.metadata.name, "conn-u5cb27icdjt5vdf6sc4wehb4nb",
      "the created object carries the name the SERVER gave it");
    assert.equal(sent.length, 1, "NEGATIVE CONTROL: a blank name is not refused in console mode");
    assert.equal(sent[0].metadata.name, intent,
      "the idempotency seed is the draft's intent, not a word the operator typed");
    await submitCluster("team-a", Object.assign({}, VALUES, { intent: intent }), api);
    assert.equal(sent[1].metadata.name, intent, "a resend of the same draft is the same key");

    await assert.rejects(() => submitCluster("team-a", Object.assign({}, VALUES), api),
      (error) => error.kind === "invalid",
      "a console draft with no intent is refused before anything is sent");
  } finally {
    resetMode();
  }
});

test("the_console_clusters_form_mints_one_intent_per_draft", async () => {
  await consoleMode();
  const ns = "team-a";
  const key = formKey(ns, CLUSTER_FORM);
  dropDraft(key);
  try {
    assert.ok(CLUSTER_DRAFT_FIELDS.includes("intent"), "the draft keeps the intent");
    const sent = [];
    let fail = true;
    const api = {
      list: async () => ({ items: [] }),
      create: async (namespace, plural, body) => {
        sent.push(body);
        if (fail) {
          throw Object.assign(new Error("upstream timeout"), { status: 504, kind: "unknown" });
        }
        return { metadata: { name: "conn-i4wvodbvduxnow4xiblklfzbdk", uid: "u" }, __contract: {} };
      },
    };
    const view = fakeView();
    await mountClusters(view.root, ns, parse, LIFE(), api);
    assert.equal(view.find("#cluster-name"), null, "no name input in console mode");
    view.find("#cluster-servers").value = "kafka-target:9092";
    await view.find("#cluster-form").dispatch("submit");
    await new Promise((resolve) => setTimeout(resolve, 10));
    assert.equal(sent.length, 1);
    const first = sent[0].metadata.name;
    assert.match(first, /^logweir-ui\.connection\.[0-9a-f]{32}$/);
    assert.equal((readDraft(key) || {}).intent, first, "the intent is held by the draft");

    // A RETRY AFTER AN UNKNOWN OUTCOME IS THE SAME KEY, so the product API
    // replays the first create instead of making a second connection.
    fail = false;
    await view.find("#cluster-form").dispatch("submit");
    await new Promise((resolve) => setTimeout(resolve, 10));
    assert.equal(sent.length, 2);
    assert.equal(sent[1].metadata.name, first, "NEGATIVE CONTROL: one intent per draft");
    assert.equal(readDraft(key), null, "and the created connection ends the draft");
  } finally {
    dropDraft(key);
    resetMode();
  }
});
