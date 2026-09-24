// legacy-point.spec.js -- the console half of the legacy-point-restore round:
// a recovery point written by a release before saved destinations (`v0.1.5`),
// restored after the upgrade.
//
// P5: such a point's plan used to name evidence bucket `logweir-evidence`
// whatever its archive was; the controller reads an inline-archive run's
// scorecard only through its own archive handle, in that handle's bucket, so
// the PoC restore finished with no verification and no completion. The
// default is now the archive's own bucket.
// P5's class sweep: a Restore's "check it yourself" commands fetched the
// scorecard from the SOURCE archive's bucket; they now fetch it from the
// bucket the approved plan wrote it to.
// P6: the Catalog page said a Full sync "walks the receipts and manifests";
// it reads catalog records only.
//
// Every row carries its negative control: it FAILS on the code before this
// round. Pure functions from JSON to strings; no DOM, no network, no clock.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  initialState,
  legacyEvidenceBucket,
  LEGACY_EVIDENCE_BUCKET_SENTENCE,
  preparePlan,
  readinessSourceSentence,
  recoveryPoints,
  renderStoreFields,
} from "../pages/restore-wizard.js";
import { renderRestoreDetail } from "../pages/history.js";
import { CATALOG_MODE_HELP, renderConnectForm } from "../pages/catalog.js";
import { planEvidenceBucket } from "../render.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));

function fixture(name) {
  return JSON.parse(readFileSync(FIXTURES + name, "utf8"));
}

/** The wizard over the legacy fixture's newest point: `s3://kafka-backups/drill-demo`,
 *  no `destinationRef`. */
function legacyState() {
  const backups = fixture("wizard-backups.json");
  const point = recoveryPoints(backups)[0];
  assert.equal(((point.spec || {}).destinationRef), undefined, "the premise: a legacy point");
  assert.equal(point.spec.archive.url, "s3://kafka-backups/drill-demo", "the premise: its archive");
  return initialState(
    "logweir-poc",
    fixture("wizard-clusters.json"),
    backups,
    { uid: point.metadata.uid, backup: point.metadata.name },
  );
}

/** The `evidence:` block's bucket line of a rendered plan. */
function evidenceBucketLine(bytes) {
  const block = bytes.slice(bytes.indexOf("\nevidence:\n"));
  return /\n {2}bucket: "([^"]*)"/.exec(block)[1];
}

test("a_legacy_points_evidence_starts_in_its_own_archives_bucket", async () => {
  const state = legacyState();
  assert.equal(state.fields.evidence.bucket, "kafka-backups",
    "P5: the evidence bucket is the archive's own, never the hard-coded logweir-evidence");
  assert.equal(state.evidenceBucket, "kafka-backups");
  assert.equal(state.fields.evidence.prefix, "logweir/", "Global Constraint 6 is unchanged");
  const plan = await preparePlan(state);
  assert.equal(evidenceBucketLine(plan.bytes), "kafka-backups",
    "the SIGNED plan writes the scorecard where the controller's handle reads it");
  assert.doesNotMatch(plan.bytes, /logweir-evidence/,
    "negative control: the old default is gone from the plan bytes");
});

test("an_archive_url_with_no_bucket_leaves_the_evidence_bucket_empty", () => {
  assert.equal(legacyEvidenceBucket("s3://kafka-backups/poc"), "kafka-backups");
  assert.equal(legacyEvidenceBucket("s3://kafka-backups"), "kafka-backups");
  for (const nothing of ["", undefined, null, "not-a-url", "s3://"]) {
    assert.equal(legacyEvidenceBucket(nothing), "",
      "no bucket, no guess: " + String(nothing) + " must not become a placeholder bucket");
  }
});

test("a_legacy_points_evidence_field_says_where_the_controller_verifies", () => {
  const out = renderStoreFields(legacyState());
  assert.match(out, /id="legacy-evidence-bucket"/);
  assert.ok(out.includes("LOGWEIR_ARCHIVE_URL"), "the handle is named on screen");
  assert.ok(out.includes("NotAttempted"), "and what another bucket costs");
  assert.ok(LEGACY_EVIDENCE_BUCKET_SENTENCE.length > 0);
});

test("a_legacy_points_readiness_sentence_names_the_restore_jobs_principal", () => {
  const point = recoveryPoints(fixture("wizard-backups.json"))[0];
  const sentence = readinessSourceSentence(point);
  assert.match(sentence, /with the Secret that Backup named/,
    "P3: the check reads as the restore Job will, and the page says so");
});

test("the_plan_evidence_bucket_is_read_from_the_plans_own_evidence_block", () => {
  const plan = fixture("restore-valid-pass.json").spec.planBytes;
  assert.equal(planEvidenceBucket(plan), "logweir-evidence",
    "the evidence block's bucket, not the source block's kafka-backups");
  assert.equal(planEvidenceBucket("source:\n  storage:\n    bucket: a\nevidence:\n  bucket: b\n"),
    "b");
  assert.equal(planEvidenceBucket("evidence:\n  backend: s3\n  bucket: 'c'\n"), "c");
  assert.equal(planEvidenceBucket("source:\n  storage:\n    bucket: a\n"), "",
    "no evidence block, no bucket");
  assert.equal(planEvidenceBucket(undefined), "");
});

test("a_restores_fetch_commands_read_the_bucket_its_plan_wrote_the_scorecard_to", () => {
  const object = fixture("restore-valid-pass.json");
  // The source archive is in ANOTHER bucket from the plan's evidence: the
  // shape of every legacy point restored with a separate evidence bucket, and
  // of every destination-backed restore (whose sourceArchive is a sentinel).
  object.spec.sourceArchive.url = "s3://kafka-backups/drill-demo";
  const out = renderRestoreDetail(object);
  const key = object.status.evidence.scorecardKey;
  assert.ok(out.includes("aws s3 cp s3://logweir-evidence/" + key),
    "the scorecard is fetched from the plan's evidence bucket");
  assert.ok(!out.includes("aws s3 cp s3://kafka-backups/" + key),
    "negative control: never from the source archive's bucket");

  const sentinel = fixture("restore-valid-pass.json");
  sentinel.spec.sourceArchive.url = "logweir-destination://primary";
  const destinationBacked = renderRestoreDetail(sentinel);
  assert.ok(!destinationBacked.includes("s3://primary/"),
    "a destination NAME is never rendered as a bucket");
});

test("the_catalog_page_says_a_full_sync_reads_records_and_how_old_points_appear", () => {
  const out = renderConnectForm({ ns: "logweir-poc", values: {}, state: {} });
  assert.ok(out.includes("logweir/catalog/v1/points/"), "what Full reads");
  assert.ok(out.includes("logweir catalog sync"), "the backfill a pre-catalog point needs");
  assert.ok(!/walks the receipts and manifests/.test(out),
    "P6's negative control: the false claim is gone");
  assert.ok(out.includes(CATALOG_MODE_HELP.slice(0, 40)));
});
