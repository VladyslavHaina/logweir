import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { renderScheduleForm, scheduleBody } from "../pages/schedules.js";
import * as wizard from "../pages/restore-wizard.js";
import { renderPlanBytes } from "../plan.js";

const fixture = (name) => JSON.parse(readFileSync(new URL("./fixtures/" + name, import.meta.url)));

/** The route identity for a fixture's newest recovery point -- what the wizard
 *  is entered with since PLAT-11.1: it no longer picks a Backup for itself. */
function newestPoint(list) {
  const point = wizard.recoveryPoints(list)[0];
  return { uid: point.metadata.uid, backup: point.metadata.name };
}

function scramState() {
  const clusters = fixture("wizard-clusters.json");
  const target = clusters.items.find((c) => c.spec.role === "target");
  target.spec.auth = {
    mode: "scramSha512", username: "restore-user", tls: true,
    secretRef: { name: "kafka-scram" }, password: "must-not-enter-plan",
  };
  const backups = fixture("wizard-backups.json");
  return wizard.initialState("incident", clusters, backups, newestPoint(backups));
}

test("schedule form defaults to the installed archive credential and preserves its reference", () => {
  assert.match(renderScheduleForm(), /name="archiveSecret" value="logweir-s3"/);
  const values = {
    name: "hourly", cron: "0 * * * *", source: "source", topics: "orders",
    archive: "s3://kafka-backups/incident", archiveSecret: "incident-archive",
  };
  assert.deepEqual(scheduleBody(values).spec.archive, {
    url: values.archive, secretRef: { name: "incident-archive" },
  });
  assert.deepEqual(scheduleBody({ ...values, archiveSecret: "" }).spec.archive, {
    url: values.archive,
  });
});

test("a selected SCRAM cluster supplies only public connection settings to the signed plan", async () => {
  const state = scramState();
  assert.deepEqual(state.fields.target.auth, {
    mode: "scramSha512", username: "restore-user", tls: true,
  });
  const prepared = await wizard.preparePlan(state);
  assert.match(prepared.bytes, /  auth:\n    mode: "scramSha512"\n    username: "restore-user"\n    tls: true\n/);
  assert.doesNotMatch(prepared.bytes, /must-not-enter-plan|kafka-scram|secretRef/);
  const requests = [];
  await wizard.submitRestore(state, { create: async (...args) => requests.push(args) });
  assert.equal(requests[0][2].spec.planBytes, prepared.bytes);
  assert.equal(prepared.hash, "sha256:" + createHash("sha256").update(prepared.bytes).digest("hex"));
});

test("changing target refreshes auth and plan hash, without carrying the previous identity", async () => {
  const state = scramState();
  const before = await wizard.preparePlan(state);
  const source = state.clusters.items.find((c) => c.spec.role === "source");
  source.spec.auth = { mode: "plaintext" };
  wizard.selectTarget(state, source.metadata.name);
  assert.equal(state.fields.target.auth, undefined);
  assert.deepEqual(state.fields.target.bootstrapServers, source.spec.bootstrapServers);
  const after = await wizard.preparePlan(state);
  assert.notEqual(after.hash, before.hash);
  assert.doesNotMatch(after.bytes, /  auth:|restore-user/);
  source.spec.auth = { mode: "scramSha512", username: "second-user", tls: false };
  wizard.selectTarget(state, source.metadata.name);
  const third = await wizard.preparePlan(state);
  assert.notEqual(third.hash, after.hash);
  assert.match(third.bytes, /username: "second-user"\n    tls: false/);
  assert.doesNotMatch(third.bytes, /restore-user/);
});

test("SCRAM over plain transport is explicit, and unsupported or incomplete auth is refused", () => {
  const fields = fixture("plan-fields.json");
  fields.target.auth = { mode: "scramSha512", username: "plain-user", tls: false };
  assert.match(renderPlanBytes(fields), /    tls: false\n/);
  fields.target.auth.mode = "unknown";
  assert.throws(() => renderPlanBytes(fields), /auth/);
  fields.target.auth = { mode: "scramSha512", username: "", tls: true };
  assert.throws(() => renderPlanBytes(fields), /username/);
});
