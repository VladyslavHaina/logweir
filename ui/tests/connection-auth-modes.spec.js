// connection-auth-modes.spec.js -- PROD-01.3 in the console: SASL/PLAIN over
// TLS, SCRAM-SHA-256 and mTLS client certificates, and the security
// follow-up's write-only credential entry.
//
// THE THREAT the console half closes: the product API no longer accepts the
// NAME of an existing Secret for a connection's credential, because a
// connection that could name any Secret could make Logweir present another
// team's credential to brokers of its author's choosing. In console mode the
// credential is TYPED ONCE, rides off the custom resource's own fields, and is
// never kept in a draft, rendered back or written anywhere but the request.
//
// Every row carries its NEGATIVE CONTROL: the accepted twin of what it refuses.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { readDraft, keepDraft, formKey, dropDraft } from "../lifecycle.js";
import {
  CLUSTER_DRAFT_FIELDS,
  CLUSTER_FORM,
  CONSOLE_TLS_CA_KINDS,
  WRITE_ONLY_CLUSTER_FIELDS,
  clusterBody,
  renderClusterForm,
  validateCluster,
} from "../pages/clusters.js";
import { CONNECTION_AUTH_MODES } from "../contract.js";
import { renderPlanBytes } from "../plan.js";

const fixture = (name) =>
  JSON.parse(readFileSync(new URL("./fixtures/" + name, import.meta.url), "utf8"));

const SECRET = "typed-once-9f2c";

const BASE = Object.freeze({
  name: "minted", servers: "kafka-0:9093", role: "source", mode: "plain",
  username: "app-key", tls: true, tlsCaKind: "none",
});

test("the_five_modes_are_the_servers_five", () => {
  assert.deepEqual(CONNECTION_AUTH_MODES, ["plaintext", "scramSha512", "scramSha256", "plain", "mtls"]);
  const form = renderClusterForm({});
  for (const mode of CONNECTION_AUTH_MODES) {
    assert.ok(form.includes("<option value=\"" + mode + "\""), mode + " is offered");
  }
});

test("the_console_form_types_the_credential_once_and_never_renders_or_keeps_it", () => {
  const key = formKey("auth-ns", CLUSTER_FORM);
  // A draft that somehow saw every field -- including the typed values.
  keepDraft(key, Object.assign({}, BASE, {
    enteredPassword: SECRET, enteredCertificatePem: SECRET, enteredPrivateKeyPem: SECRET,
  }), CLUSTER_DRAFT_FIELDS);
  try {
    const draft = readDraft(key);
    for (const field of WRITE_ONLY_CLUSTER_FIELDS) {
      assert.equal(CLUSTER_DRAFT_FIELDS.indexOf(field), -1, field + " is on no draft list");
      assert.equal(draft[field], undefined, field + " is never kept");
    }
    const console_ = renderClusterForm({ minted: true, draft: draft });
    assert.match(console_, /name="enteredPassword" type="password" autocomplete="new-password" value=""/);
    assert.match(console_, /name="enteredCertificatePem"/);
    assert.match(console_, /name="enteredPrivateKeyPem"/);
    assert.equal(console_.indexOf(SECRET), -1, "nothing typed is ever rendered back");
    assert.equal(console_.indexOf("name=\"secret\""), -1, "the console names no Secret");
    assert.equal(console_.indexOf("name=\"passwordKey\""), -1);
    for (const kind of CONSOLE_TLS_CA_KINDS) {
      assert.ok(console_.includes("<option value=\"" + kind + "\""), kind);
    }
    assert.equal(console_.indexOf("<option value=\"secret\""), -1, "no Secret-held CA in the console");
    // CONTROL: the legacy (kubectl proxy) form takes references, no value.
    const legacy = renderClusterForm({ draft: draft });
    assert.equal(legacy.indexOf("type=\"password\""), -1, "no password input in legacy mode");
    assert.match(legacy, /name="secret"/);
    assert.match(legacy, /name="clientCertSecret"/);
    assert.match(legacy, /logweir-binding/, "the legacy note says what a hand-made Secret needs");
  } finally {
    dropDraft(key);
  }
});

test("plain_without_tls_is_refused_by_name_and_plain_over_tls_is_accepted", () => {
  const refused = validateCluster(Object.assign({}, BASE, { tls: false, enteredPassword: SECRET }),
    { console: true });
  assert.match(refused.tls, /^PlainWithoutTls: /);
  assert.equal(JSON.stringify(refused).indexOf(SECRET), -1, "no message echoes the value");
  const accepted = validateCluster(Object.assign({}, BASE, { enteredPassword: SECRET }),
    { console: true });
  assert.deepEqual({ ...accepted }, {}, "PLAIN over TLS with a typed password is accepted");
  // A SASL mode with no typed password is refused in the console.
  const missing = validateCluster(Object.assign({}, BASE, { mode: "scramSha256" }), { console: true });
  assert.match(missing.enteredPassword, /scramSha256 needs the SASL password/);
});

test("mtls_needs_tls_and_a_certificate_in_either_mode", () => {
  const mtls = Object.assign({}, BASE, { mode: "mtls", username: "" });
  const noTls = validateCluster(Object.assign({}, mtls, { tls: false }), { console: true });
  assert.match(noTls.tls, /requires TLS on/);
  const noCert = validateCluster(mtls, { console: true });
  assert.match(noCert.enteredCertificatePem, /client certificate/);
  assert.deepEqual({ ...validateCluster(Object.assign({}, mtls, {
    enteredCertificatePem: "-----BEGIN CERTIFICATE-----", enteredPrivateKeyPem: "k",
  }), { console: true }) }, {});
  // Legacy: a client-certificate Secret REFERENCE instead.
  assert.match(validateCluster(mtls).clientCertSecret, /tls.crt and tls.key/);
  assert.deepEqual({ ...validateCluster(Object.assign({}, mtls, { clientCertSecret: "orders-client" })) }, {});
});

test("the_console_body_carries_the_value_off_the_resource_and_the_legacy_body_a_reference", () => {
  const body = clusterBody(Object.assign({}, BASE, { enteredPassword: SECRET }), { console: true });
  assert.deepEqual(body.__credential, { password: SECRET }, "the one reader can see it");
  assert.equal(JSON.stringify(body).indexOf(SECRET), -1,
    "but it is not a field of the resource: no JSON of it carries the value");
  assert.equal(Object.keys(body).indexOf("__credential"), -1, "non-enumerable");
  assert.equal(body.spec.auth.secretRef, undefined, "the console names no Secret");
  const mtls = clusterBody(Object.assign({}, BASE, {
    mode: "mtls", enteredCertificatePem: "C", enteredPrivateKeyPem: SECRET,
  }), { console: true });
  assert.deepEqual(mtls.__credential, { certificatePem: "C", privateKeyPem: SECRET });
  assert.equal(mtls.spec.auth.username, undefined, "mtls has no SASL username");
  // CONTROL: legacy mode builds the references and attaches no value.
  const legacy = clusterBody(Object.assign({}, BASE, {
    mode: "mtls", clientCertSecret: "orders-client", enteredPrivateKeyPem: SECRET,
  }));
  assert.deepEqual(legacy.spec.auth.clientCertificate, { name: "orders-client" });
  assert.equal(legacy.__credential, undefined);
});

test("the_plan_document_spells_the_new_modes_as_the_runner_parses_them", () => {
  const fields = fixture("plan-fields.json");
  fields.target.auth = { mode: "mtls", tls: true };
  const mtls = renderPlanBytes(fields);
  assert.match(mtls, / {2}auth:\n {4}mode: "mtls"\n {4}tls: true\n/);
  assert.equal(/ {4}username:/.test(mtls.split("target:")[1]), false, "mtls names no username");
  fields.target.auth = { mode: "scramSha256", username: "u", tls: false };
  assert.match(renderPlanBytes(fields), / {4}mode: "scramSha256"\n {4}username: "u"\n {4}tls: false\n/);
  fields.target.auth = { mode: "plain", username: "u", tls: false };
  assert.throws(() => renderPlanBytes(fields), /PlainWithoutTls/);
  fields.target.auth = { mode: "plain", username: "u", tls: true };
  assert.match(renderPlanBytes(fields), / {4}mode: "plain"\n/);
});
