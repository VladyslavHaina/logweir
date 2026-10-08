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

import {
  CREATE_GRANT_SOURCES,
  DESTINATION_DEFAULTS,
  EXISTING_ON_ROTATION,
  GRANT_SOURCES,
  renderDestinationForm,
  renderRotateForm,
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
