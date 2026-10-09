// FX-20: the console never makes a NEW destination name an existing Secret.
//
// A destination that could name any existing Secret could make Logweir sign
// requests with a credential its author cannot read and send them to an
// endpoint its author chose. The product API refuses `secret.existing` on a
// create (`existing_credential_refused`) and accepts it on a rotation only for a
// Secret the destination already names; this file holds the console's half:
// the create form does not offer `existing`, its default is `new` (the value is
// entered once and becomes a Secret bound to the destination), the create
// validation says why beside the control, and the rotation form still offers
// `existing` and says what it may name. Each behaviour has its negative
// control beside it.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import {
  CREATE_GRANT_SOURCES,
  DESTINATION_DEFAULTS,
  EXISTING_ON_ROTATION,
  GRANT_SOURCES,
  renderDestinationForm,
  renderRotateForm,
  renderTestPanel,
  validateDestination,
} from "../pages/destinations.js";

const base = {
  name: "primary",
  bucket: "kafka-backups",
  addressing: "pathStyle",
  security: "tls",
};

function sourceOptions(html, prefix, role) {
  const start = html.indexOf("id=\"" + prefix + "-" + role + "-source\"");
  assert.notEqual(start, -1, prefix + " " + role + " has a source selector");
  const end = html.indexOf("</select>", start);
  return [...html.slice(start, end).matchAll(/<option value="([^"]+)"/g)].map((m) => m[1]);
}

test("fx20_the_create_form_offers_no_existing_secret_and_starts_on_new", () => {
  assert.deepEqual(
    CREATE_GRANT_SOURCES.slice().sort(),
    GRANT_SOURCES.filter((s) => s !== "existing").slice().sort(),
  );
  assert.equal(DESTINATION_DEFAULTS.archiveWriteSource, "new",
    "BEFORE: the create form started archiveWrite on `existing`");
  const html = renderDestinationForm({ draft: {}, state: { phase: "idle" } });
  for (const role of ["archiveWrite", "archiveRead", "evidenceWrite", "evidenceRead"]) {
    assert.ok(!sourceOptions(html, "destination", role).includes("existing"),
      role + ": the create form offers `existing`");
    assert.ok(sourceOptions(html, "destination", role).includes("new"), role);
  }
  // CONTROL: the rotation form still offers it, and says what it may name.
  const rotate = renderRotateForm({ name: "primary", generation: 3 }, { mayOperate: true });
  assert.ok(sourceOptions(rotate, "rotate", "archiveWrite").includes("existing"),
    "the rotation keeps a Secret this destination already names");
  assert.ok(rotate.includes("only a Secret this destination already names"), rotate);
  assert.match(EXISTING_ON_ROTATION, /NEW Secret bound to this destination/);
});

test("fx20_a_create_naming_an_existing_secret_is_refused_beside_the_control", () => {
  const refused = validateDestination(Object.assign({}, base,
    { archiveWriteSource: "existing", archiveWriteSecret: "lwd-victim-archive-write" }));
  assert.match(refused.archiveWriteSource, /does not name an existing Secret/);
  assert.match(refused.archiveWriteSource, /bound to this destination/);
  // CONTROLS: the entered credential, and a workload identity, are accepted.
  assert.deepEqual(Object.keys(validateDestination(Object.assign({}, base, {
    archiveWriteSource: "new", archiveWriteAccessKeyId: "AKIA", archiveWriteSecretAccessKey: "x",
  }))), []);
  assert.deepEqual(Object.keys(validateDestination(Object.assign({}, base,
    { archiveWriteSource: "workloadIdentity" }))), []);
});

test("fx20_the_inline_archive_secret_field_says_what_the_secret_must_carry", async () => {
  const { INLINE_ARCHIVE_BINDING_HELP, renderScheduleForm } = await import("../pages/schedules.js");
  assert.match(INLINE_ARCHIVE_BINDING_HELP, /logweir-binding/);
  assert.match(INLINE_ARCHIVE_BINDING_HELP, /CredentialBindingMismatch/);
  const html = renderScheduleForm({ draft: {}, state: { phase: "idle" }, destinations: [] });
  assert.ok(html.includes("name=\"archiveSecret\""), "the inline Secret field is on the form");
  assert.ok(html.includes("CredentialBindingMismatch"),
    "the field says what the Secret must carry, beside it");
});

// FX-20c: the destination's Test access renders the binding row a refused
// grant produces -- the grant by its `spec.access` field, its destination, its
// Secret, and whether the binding was absent or foreign -- under a verdict
// that is not ready. The fixture is the one the product API's row
// (`crates/logweir-api/tests/destinations.rs`,
// `fx20c_a_destination_test_refused_on_a_binding_is_answered_by_grant`) sends
// and the runner's message (`crates/logweir/tests/check_grant_binding.rs`) is
// held to.
const bindingMismatch = () => JSON.parse(readFileSync(
  new URL("./fixtures/console/preflight-binding-mismatch.json", import.meta.url), "utf8")).item;

function rowOf(html, id) {
  const at = html.indexOf("<code>" + id + "</code>");
  assert.notEqual(at, -1, id + " is rendered");
  return html.slice(at, html.indexOf("</tr>", at));
}

test("fx20c_test_access_renders_the_refused_grant_by_name_under_a_not_ready_verdict", () => {
  const item = bindingMismatch();
  const html = renderTestPanel({ name: "fx20-thief" }, { test: item, mayOperate: true });
  const headAt = html.indexOf("preflight-head");
  const head = html.slice(headAt, html.indexOf("</p>", headAt));
  assert.match(head, /not ready/);
  assert.doesNotMatch(head, /badge-green/);
  const row = rowOf(html, "destination.credentialBound");
  assert.match(row, /CredentialBindingMismatch/);
  assert.match(row, /badge-unverified">not ready/);
  assert.match(row, new RegExp(
    "data-field=\"message\">archiveWrite \\(Secret <code>lwd-primary-archive-write</code>: " +
    "foreign binding\\)"));
  // REVIEW LOW-2: the remedy gives this destination its own Secret, and never
  // tells anyone to bind the refused Secret here.
  assert.match(row, /remedy: .*its own Secret.*scripts\/bind-credential\.py/);
  assert.doesNotMatch(row, /status\.credentialBinding/);
  assert.match(row, /archiveWrite=CredentialBindingMismatch/);
  assert.match(row, /remedy: .*logweir-binding/);
  assert.match(row, /scope: BackupDestination\/fx20-thief/);
  // The archive-write row is still listed as knowable only at execution.
  assert.match(html, /Only knowable at execution time/);

  // CONTROL: the F6 result as it was BEFORE this row existed -- every other
  // row ready, the aggregate `ready` -- names no grant and reads green. The
  // assertions above fail on it.
  const before = Object.assign({}, item, {
    state: "ready",
    reason: "Ready",
    checks: item.checks.filter((c) => c.id !== "destination.credentialBound"),
  });
  const old = renderTestPanel({ name: "fx20-thief" }, { test: before, mayOperate: true });
  assert.doesNotMatch(old, /credentialBound|lwd-primary-archive-write|CredentialBindingMismatch/);
  assert.match(old, /badge-green">ready/);
});

// FX-20c review M-1: a RetentionPolicy whose run was refused on its
// credential's binding is published `enforcement: RecommendationOnly` and
// `guarantees.ageExpiry: NotEnforced` (with `Enforced=False/
// CredentialBindingMismatch`) by the controller
// (`crates/weirkeeper/tests/retention_policy_controller.rs`,
// `fx20c_a_binding_refusal_stands_on_enforced_until_a_later_run`). The panel
// then says nothing is deleted, never "enforced by Logweir", and says why.
test("fx20c_a_binding_refused_retention_policy_is_not_shown_as_enforced", async () => {
  const { renderEnforcement, retentionSentenceFor } = await import("../pages/schedules.js");
  const base = JSON.parse(readFileSync(
    new URL("./fixtures/d3/retention-enforce.json", import.meta.url), "utf8"));
  const shaped = (enforcement, ageExpiry, enforced) => {
    const policy = JSON.parse(JSON.stringify(base));
    policy.status.enforcement = enforcement;
    policy.status.guarantees.ageExpiry = ageExpiry;
    policy.status.conditions = policy.status.conditions
      .filter((c) => c.type !== "Enforced" && c.type !== "EnforcementDegraded")
      .concat([enforced]);
    return policy;
  };
  const refusal = {
    type: "Enforced", status: "False", reason: "CredentialBindingMismatch",
    message: "retention run r1 REFUSED a credential before building any handle " +
      "(CredentialBindingMismatch). Nothing was deleted.",
  };
  const refused = shaped("RecommendationOnly", "NotEnforced", refusal);
  const html = renderEnforcement({}, refused) + retentionSentenceFor({}, refused);
  assert.match(html, /<dt>age expiry<\/dt><dd>not enforced<\/dd>/);
  assert.match(html, /<dt>what is happening<\/dt><dd>RecommendationOnly<\/dd>/);
  assert.doesNotMatch(html, /An isolated Logweir retention worker deletes/);
  assert.match(html, /Logweir never deletes from your archive/);
  assert.match(html, /data-enforced-refusal="CredentialBindingMismatch"/);
  assert.match(html, /Not enforcing: Enforced=False CredentialBindingMismatch: retention run r1/);

  // CONTROL: the fields the first landing left behind read "enforced".
  const before = shaped("LogweirWorker", "LogweirEnforced", refusal);
  const old = renderEnforcement({}, before) + retentionSentenceFor({}, before);
  assert.match(old, /<dt>age expiry<\/dt><dd>enforced by Logweir<\/dd>/);
  assert.match(old, /An isolated Logweir retention worker deletes/);
  assert.doesNotMatch(old, /data-enforced-refusal/);
});
