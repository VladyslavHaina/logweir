// pages/restore-wizard.js -- six steps: archive, backup set, point in time,
// target and naming, target-topic preflight, and the rendered plan with its
// hash and the two names the next two creates will use.
//
// THE BYTES ARE THE PRODUCT OF THIS PAGE. `../plan.js` renders the runner's
// own document, this page shows it, hashes it and submits it -- and between
// the `<pre>` a viewer reads and the string `create` sends there is no
// transformation of any kind. Not a parse, not a re-emit, not a normalise, not
// a trim. The sha256 on screen is the sha256 the controller recomputes only if
// those are the same bytes, and "nearly the same bytes" is a different
// document with a different hash that `logweir drill approve` would sign
// instead.
//
// TWO NAMES, MINTED BEFORE EITHER CREATE. `Restore.spec.approvalRef` and
// `Approval.spec.subjectRef` name each other and both specs are CEL-immutable
// (`self == oldSelf`, spec section 3.2), so neither reference can be filled in
// afterwards and two peer "create" buttons with no stated order cannot produce
// a working pair. So: mint both names from the plan bytes, create the
// `Restore` FIRST with `spec.approvalRef.name` set to an approval that does
// not exist yet, and let the reconciler requeue at 30 s on
// `ApprovalNotVerified` until it does (spec section 7 amendment 4; Task 20's
// requeue). The same suffix on both is what lets an operator read the pair off a
// `kubectl get` without a join.
//
// AND NO KEY IS EVER SEEN HERE. This page does not mint an approval, does not
// generate a keypair, does not upload or proxy one, and offers no "Approve"
// button that produces a signature (Global Constraint 28). It prints the exact
// command an approver runs on their own machine, and the approvals page takes
// the two documents that command wrote.
//
// EDITING IS CREATING. `Restore.spec` is immutable, so there is no in-place
// edit: "edit" prefills a NEW draft, whose bytes hash differently, and the
// page says so above the form.
//
// ONE GUIDED SUBMIT (PLAT-12.1). "Create the Restore" is the only action, and
// it does the whole journey in order: check that the plan about to be sent is
// the plan on screen, create the Restore idempotently -- its name is minted
// from those bytes, so the same plan submitted twice, or retried after a lost
// response, resolves to the Restore the first request made -- and then open
// what it needs next. Under today's approval semantics every Restore waits for
// a verified Approval: when one already authorises this exact Restore, that is
// the Restore's operation view; otherwise it is the Restore's approval page,
// Awaiting approval. There is no second button that navigates without
// creating, and no create that forgets where it was going.

import { create, get, list } from "../api.js";
import {
  active,
  cancelled,
  dropDraft,
  fieldErrors,
  formKey,
  invalidInput,
  keepDraft,
  listen,
  mutationFor,
  readDraft,
  readOptions,
  refusal,
  resolveExisting,
  watchMutation,
} from "../lifecycle.js";
import {
  CONVENIENCE_SENTENCE,
  COPY_CAVEAT,
  RESTORE_IMMUTABLE_SENTENCE,
  bucketOf,
  cell,
  copyBlock,
  epochMs,
  errorBox,
  esc,
  facts,
  fieldErrorLine,
  invalidAttributes,
  mutationStatus,
  prefixOf,
  preflightSentence,
  replace,
  rfc3339,
  table,
  windowMessage,
} from "../render.js";
import { defaultTopicPrefix, TARGET_MODES, mintNames, planHash, renderPlanBytes } from "../plan.js";
import { isObjectName, itemsOf } from "./clusters.js";
import { approvalAuthorizes, restoreOperationRoute } from "./approvals.js";

const PLURAL = "restores";
const CLUSTERS = "kafkaclusters";
const BACKUPS = "backups";
const APPROVALS = "approvals";

/** The wizard's identity in the draft and mutation registries. */
export const WIZARD_FORM = "restore-wizard";

/** The wizard fields a draft keeps: the values a person can change in steps
 *  1, 3 and 4. None is a credential -- the archive credential is a Secret's
 *  NAME -- and the plan bytes themselves are never kept: they are rendered
 *  again, and hashed again, from these. */
export const WIZARD_DRAFT_FIELDS = Object.freeze([
  "backupSetRef", "pointInTime", "mode", "topicPrefix", "targetCluster", "endpoint", "region",
  "pathStyle", "evidenceBucket", "archiveSecret",
]);

/** The API server's field paths, mapped to the wizard's inputs. `archive` and
 *  `backupSet` are not inputs: they were read from a Backup, and their
 *  messages are shown beside the submit button. */
export const WIZARD_FIELD_PATHS = Object.freeze([
  ["spec.pointInTime", "pointInTime"],
  ["spec.target.topicNaming", "topicPrefix"],
  ["spec.target.clusterRef", "targetCluster"],
  ["spec.target.mode", "mode"],
  ["spec.sourceArchive.secretRef", "archiveSecret"],
  ["spec.sourceArchive", "archive"],
  ["spec.backupSetRef", "backupSet"],
]);

/** The fields of `WIZARD_FIELD_PATHS` with no input of their own. */
const NOT_INPUTS = Object.freeze(["archive", "backupSet"]);

/** `Restore.spec` has no server default and no field that may change, so the
 *  comparison with an existing Restore is exact. */
const RESTORE_SPEC_RULES = Object.freeze({});

/** The evidence prefix, and it is NOT a field an operator sets. Global
 *  Constraint 6: `logweir` writes only under `logweir/`, and
 *  `Store::from_url` refuses an evidence prefix that is not exactly that --
 *  trailing slash included. A plan that named another one would be refused at
 *  phase 0 after the approver had already signed it. */
const EVIDENCE_PREFIX = "logweir/";

/** The default api surface. The mount half takes an override so the behaviour
 *  suite can hand in a stub that RECORDS every write and a stub that THROWS on
 *  one; there is no DOM and no network under `node --test`, and a page whose
 *  write half could only be exercised in a browser is a page whose write half
 *  is exercised nowhere. */
const API = { create: create, get: get, list: list };

// ---------------------------------------------------------------- the steps

/** Step 1 -- the archive. A `KafkaCluster` with `role: source`, and the
 *  archives this namespace's `Backup` objects name.
 *
 *  THE PAGE HOLDS NO BUCKET CREDENTIAL AND DOES NOT LIST OBJECT STORAGE. Every
 *  archive and every backup set on this page was read from a Kubernetes
 *  object, which is the only thing this page can read. */
export function renderArchiveStep(state) {
  const s = state || {};
  const sources = itemsOf(s.clusters).filter((c) => ((c.spec || {}).role) === "source");
  const rows = sources.map((cluster) => {
    const meta = cluster.metadata || {};
    const status = cluster.status || {};
    return [
      cell(meta.name),
      cell(((cluster.spec || {}).bootstrapServers || []).join(", ")),
      cell(status.clusterId),
      cell(archiveFor(s, meta.name)),
      cell(archiveSecretFor(s, meta.name)),
    ];
  });
  return (
    "<section class=\"step\" id=\"step-archive\" tabindex=\"-1\"><h3>1. Archive</h3>" +
    "<p class=\"blurb\">The source cluster this restore reads an archive of. The archives " +
    "below were read from this namespace's Backup objects: this page holds no bucket " +
    "credential and lists no object storage.</p>" +
    table(
      ["SOURCE CLUSTER", "BOOTSTRAP", "CLUSTER ID", "ARCHIVE", "ARCHIVE CREDENTIAL"],
      rows,
      "no KafkaCluster in this namespace carries role: source",
    ) +
    renderArchiveCredentialField(s) +
    renderStoreFields(s) +
    "</section>"
  );
}

/** The NAME of the Secret the runner reads the archive with -- never a value.
 *
 *  `ArchiveRef` is `{url, secretRef}` and both halves belong to the `Restore`
 *  this wizard creates: `spec.sourceArchive.url` says where the archive is and
 *  `spec.sourceArchive.secretRef.name` says what reaches it. The controller
 *  injects the object-store credential into the runner Job ONLY when that
 *  second half is present, so a `Restore` submitted without it is admitted by
 *  the API server -- the field is optional in the CRD -- and then fails at the
 *  ARCHIVE rather than at admission: the Job starts, reaches for the first
 *  object, and cannot read it.
 *
 *  Prefilled from the Backup object's own archive reference, which is where
 *  this page read the URL beside it. Blank when that archive names none, which
 *  is an archive reached anonymously or by an instance role -- a real
 *  configuration, and the reason this is an input and not a refusal. */
export function renderArchiveCredentialField(state) {
  const s = state || {};
  const name = typeof s.archiveSecretName === "string" ? s.archiveSecretName : "";
  const errors = errorsOf(s);
  return (
    "<h4>The credential that reaches that archive</h4>" +
    "<div class=\"field\">" +
    "<label for=\"archive-secret\">ARCHIVE CREDENTIAL (Secret name)</label>" +
    "<input id=\"archive-secret\" name=\"archiveSecret\" value=\"" + esc(name) + "\"" +
    invalidAttributes("archive-secret", errors.archiveSecret) + ">" +
    fieldErrorLine("archive-secret", errors.archiveSecret) +
    "<p class=\"note\">The runner reads the archive with this credential: weirkeeper mounts " +
    "the named Secret's keys into the runner Job as its object-store credential, and does so " +
    "only when spec.sourceArchive.secretRef is set. A Restore created without it is " +
    "ADMITTED and then fails at the archive, not at admission -- the field is optional in " +
    "the CRD, so nothing refuses it until the Job cannot read an object. Leave it blank only " +
    "for an archive reached anonymously or by an instance role. This page shows and sends " +
    "the NAME; it never reads the Secret.</p>" +
    "</div>"
  );
}

/** The three object-store settings NO Kubernetes object in this product
 *  records, and the evidence bucket.
 *
 *  `ArchiveRef` is `{url, secretRef}` -- a location and a credential name --
 *  so an endpoint, a region and the `path_style` flag are nowhere in the
 *  cluster's own view. The runner reads them out of THESE BYTES and from
 *  nowhere else, which is why they are inputs here rather than something an
 *  operator edits into the downloaded plan afterwards: a plan edited after it
 *  was hashed is a plan the approval no longer covers.
 *
 *  The evidence PREFIX is not among them. Global Constraint 6 fixes it, and a
 *  plan naming another one is refused at phase 0 after the signature. */
export function renderStoreFields(state) {
  const s = state || {};
  const store = ((s.fields || {}).source) || {};
  return (
    "<h4>Where that archive actually is</h4>" +
    "<p class=\"note\">Leave the endpoint blank for AWS S3. These three values are not on " +
    "any object in the cluster, and the runner reads them from the plan bytes.</p>" +
    "<div class=\"field-row\">" +
    "<div class=\"field\"><label for=\"store-endpoint\">endpoint</label>" +
    "<input id=\"store-endpoint\" name=\"endpoint\" value=\"" + esc(store.endpoint) + "\"></div>" +
    "<div class=\"field\"><label for=\"store-region\">region</label>" +
    "<input id=\"store-region\" name=\"region\" value=\"" + esc(store.region) + "\"></div>" +
    "</div>" +
    "<label class=\"inline\" for=\"store-pathStyle\">" +
    "<input type=\"checkbox\" id=\"store-pathStyle\" name=\"pathStyle\"" +
    (store.pathStyle === true ? " checked" : "") + "> path_style addressing</label>" +
    "<div class=\"field\"><label for=\"evidence-bucket\">evidence bucket</label>" +
    "<input id=\"evidence-bucket\" name=\"evidenceBucket\" value=\"" +
    esc(((s.fields || {}).evidence || {}).bucket) + "\"" +
    invalidAttributes("evidence-bucket", errorsOf(s).evidenceBucket) + ">" +
    fieldErrorLine("evidence-bucket", errorsOf(s).evidenceBucket) +
    "<p class=\"note\">The evidence prefix is fixed at " + esc(EVIDENCE_PREFIX) + " by Global " +
    "Constraint 6 and is not an input: a plan naming another one is refused at phase 0, " +
    "after the approver has already signed it.</p></div>"
  );
}

/** The sentence step 2 prints under the chosen row. A running schedule is the
 *  normal state of a backed-up cluster, and this page is a snapshot of one
 *  `list` -- Task 28 measured three `Backup` objects in six minutes against a
 *  two-minute schedule, with the plan bytes, the plan hash and both minted
 *  names moving under the operator on every reload. Saying which object was
 *  chosen is what makes the move visible; suspending the schedule is what
 *  stops it. */
export const RELOAD_SENTENCE =
  "a running schedule may complete a newer backup while you read this; reload to pick it " +
  "up, or suspend the schedule first.";

/** Printed instead of the chosen-row sentence when NO run in this archive has
 *  reached `Succeeded` -- the page still renders, off the last row, and says
 *  what it did. */
export const NO_SUCCEEDED_SENTENCE =
  "no run in this archive has reached phase Succeeded; the last listed Backup is shown, and " +
  "a set no completed run wrote is a set the runner will not find.";

/** Printed as the WHOLE page when no run in this namespace has completed with
 *  a backup set: the wizard builds a plan document from a completed run's
 *  set and covered window, and without one there is nothing to hash. Before
 *  Task 28a this case threw inside the plan renderer and the page was an
 *  error box (Task 28a's review, measured pre-existing at 352a4b8). */
export const NO_COMPLETED_BACKUP_SENTENCE =
  "no Backup in this namespace has completed with a backup set yet; the restore wizard " +
  "needs one to build a plan document. Wait for a scheduled run to reach phase Succeeded, " +
  "or create a Backup, then reload this page. The runs the namespace does hold are listed " +
  "below.";

/** The `Backup`s a plan can be built from: `phase: Succeeded` AND a non-empty
 *  `status.backupId`. A run still running, or one that completed before the
 *  controller wrote the id (Task 28a), is not one. Pure. */
export function completedBackups(backups) {
  return itemsOf(backups).filter((backup) => {
    const status = (backup || {}).status || {};
    return (
      status.phase === "Succeeded" &&
      typeof status.backupId === "string" &&
      status.backupId.length > 0
    );
  });
}

/** The page rendered instead of the six steps when [`completedBackups`] is
 *  empty: the heading, the sentence, and step 2's table so the reader sees
 *  what the namespace does hold. Pure; never throws on an empty or
 *  running-only list. */
export function renderNoCompletedBackup(ns, backups) {
  return (
    "<h2>Restore wizard</h2>" +
    "<div class=\"empty-state\"><p class=\"note\">" + NO_COMPLETED_BACKUP_SENTENCE + "</p></div>" +
    renderBackupSetStep({ ns: ns, backups: backups, fields: {} })
  );
}

/** Step 2 -- the backup set. Every `Backup` whose archive is the selected one,
 *  with its covered range CONVERTED TO RFC 3339 (interface I22: the field is
 *  two integers, and a viewer reading `1757253900000` learns nothing) -- and
 *  the one this wizard CHOSE, named. */
export function renderBackupSetStep(state) {
  const s = state || {};
  const chosen = chosenBackup(s);
  const chosenName = ((chosen || {}).metadata || {}).name;
  const rows = backupsOf(s).map((backup) => {
    const status = backup.status || {};
    const covered = status.windowCovered || {};
    const name = (backup.metadata || {}).name;
    return [
      cell(status.backupId),
      cell(rfc3339(covered.fromMs)),
      cell(rfc3339(covered.toMs)),
      cell(status.records),
      cell(status.phase),
      cell(name === chosenName ? name + " (chosen)" : name),
    ];
  });
  const succeeded = newestSucceeded(s) !== null;
  const chose =
    chosen === null
      ? "<p class=\"note\">this namespace holds no Backup for this archive.</p>"
      : "<p class=\"note\">chosen: " + esc(String(chosenName)) + ", backup set " +
        esc(String(((chosen.status || {}).backupId) || "(none -- this run wrote no set)")) +
        ". " + (succeeded ? RELOAD_SENTENCE : NO_SUCCEEDED_SENTENCE) + "</p>";
  return (
    "<section class=\"step\" id=\"step-backup-set\" tabindex=\"-1\"><h3>2. Backup set</h3>" +
    "<p class=\"blurb\">The sets this archive holds. The wizard restores from the run that " +
    "COMPLETED most recently -- the newest Succeeded row, not the newest row: the newest row " +
    "is usually still running and has no set to restore from. The covered range is " +
    "Backup.status.windowCovered, two epoch-millisecond integers, shown as RFC 3339.</p>" +
    table(
      ["BACKUP SET", "COVERED FROM", "COVERED TO", "RECORDS", "PHASE", "BACKUP"],
      rows,
      "no Backup names this archive in this namespace",
    ) +
    chose +
    "</section>"
  );
}

/** Step 3 -- the point in time, defaulted to the set's `toMs` and checked
 *  against `[fromMs, toMs]` BY CONVERTING THE INPUT BACK TO EPOCH
 *  MILLISECONDS.
 *
 *  THE CHECK IS A CONVENIENCE AND NEVER THE GATE, and the page says so. The
 *  controller recomputes the hash from the referent's own bytes and phase 0
 *  refuses a point the archive does not cover; this input exists so an
 *  operator finds out before signing rather than after. */
export function renderPointInTimeStep(state) {
  const s = state || {};
  const covered = coveredOf(s);
  const value =
    typeof (s.fields || {}).pointInTime === "string" && s.fields.pointInTime.length > 0
      ? s.fields.pointInTime
      : rfc3339(covered.toMs);
  const complaint = windowComplaint(value, covered);
  const errors = errorsOf(s);
  return (
    "<section class=\"step\" id=\"step-point-in-time\" tabindex=\"-1\"><h3>3. Point in time</h3>" +
    "<p class=\"blurb\">An RFC 3339 instant. The window is closed at both ends: a record " +
    "whose timestamp equals this exactly is restored. Its FLOOR is the archive's own " +
    "earliest covered timestamp, read from the manifest by the runner, and is never a " +
    "field of this plan.</p>" +
    "<div class=\"field\">" +
    "<label for=\"point-in-time\">point in time</label>" +
    "<input id=\"point-in-time\" name=\"pointInTime\" value=\"" + esc(value) + "\"" +
    invalidAttributes("point-in-time", errors.pointInTime) + ">" +
    fieldErrorLine("point-in-time", errors.pointInTime) +
    "<p class=\"window\">" + windowMessage(covered.fromMs, covered.toMs) + "</p>" +
    "</div>" +
    (complaint === null ? "" : "<p class=\"complaint\">" + complaint + "</p>") +
    "<p class=\"note\">" + CONVENIENCE_SENTENCE + "</p>" +
    "</section>"
  );
}

/** The sentence step 4 prints when no `KafkaCluster` in the namespace carries
 *  `role: target`, and the source cluster is preselected instead.
 *
 *  IT IS A LABEL AND NOT AN AUTHORISATION, AND THE CRD SAYS SO.
 *  `crates/weirkeeper/src/crds/kafka_cluster.rs` documents `role` as "a
 *  free-form string ... a label the adopter picks and the controller reports,
 *  and `allowedClusterIds` on the cluster-scoped `TrustRoster` is what
 *  actually authorises a target, never a role written next to the address it
 *  authorises". The runner agrees: `drill/phase0_admit.rs` branches on
 *  `spec.target.mode`, and all three cluster checks -- the allowlist, the
 *  target != source rule and the marker topic -- live in the `Scratch` arm
 *  alone. The `NewTopic` arm is EMPTY, with a comment saying the source
 *  cluster is exactly where a point-in-time recovery belongs. */
export const TARGET_ROLE_SENTENCE =
  "no cluster is labelled role: target; the source cluster is preselected. The role is a " +
  "label, not an authorisation: for mode newTopic the runner accepts any reachable target, " +
  "the source cluster included; mode scratch is refused by the runner unless the target is " +
  "among the approval's allowed cluster ids, differs from the source, and proves it is " +
  "scratch with its marker topic.";

/** The warning step 4 prints for `mode: scratch` against a cluster whose spec
 *  declares no `markerTopic`. A WARNING and not a refusal: the runner refuses,
 *  at phase 0, against the cluster it actually reaches -- and this page reads
 *  a spec field, which is a statement of intent rather than an observation. */
export const SCRATCH_MARKER_WARNING =
  "this cluster's spec declares no markerTopic, and mode scratch is refused at phase 0 " +
  "unless the target proves it is scratch by carrying one. The runner checks the broker; " +
  "this line only checks the object.";

/** Step 4 -- target and naming. EVERY `KafkaCluster` in the namespace with its
 *  role beside it, the two modes `TargetMode` accepts and nothing else, and
 *  the prefix PREFILLED with `default_topic_prefix`'s own output for the
 *  chosen instant.
 *
 *  EVERY CLUSTER, BECAUSE THE RUNNER'S GUARD IS THE GATE AND THIS IS NOT.
 *  Until Task 28a this select was built from `role === "target"` alone, so a
 *  namespace with one `role: source` cluster -- which is what Demo 1 is, and
 *  what `scripts/k8s-demo.sh` runs a `newTopic` restore against, green --
 *  rendered an EMPTY select, left `target.bootstrapServers` empty, and made
 *  the plan grammar throw. The page was refusing what the product supports.
 *  So: list them all, say what each is labelled, preselect the sensible one,
 *  and let phase 0 decide. */
export function renderTargetStep(state) {
  const s = state || {};
  const fields = s.fields || {};
  const target = fields.target || {};
  const clusters = itemsOf(s.clusters);
  const chosen = targetCluster(s);
  const chosenName = ((chosen || {}).metadata || {}).name;
  const labelled = clusters.some((c) => ((c.spec || {}).role) === "target");
  const options = TARGET_MODES.map(
    (mode) =>
      "<option value=\"" + esc(mode) + "\"" +
      (target.mode === mode ? " selected" : "") +
      ">" + esc(mode) + "</option>",
  ).join("");
  const prefix =
    typeof target.topicPrefix === "string" && target.topicPrefix.length > 0
      ? target.topicPrefix
      : prefixFor(fields.pointInTime);
  const clusterOptions = clusters
    .map((c) => {
      const name = (c.metadata || {}).name;
      const role = (c.spec || {}).role;
      return (
        "<option value=\"" + esc(name) + "\"" +
        (name === chosenName ? " selected" : "") + ">" +
        esc(name) + " (role: " + esc(typeof role === "string" && role.length > 0 ? role : "unset") +
        ")</option>"
      );
    })
    .join("");
  const markerWarning =
    target.mode === "scratch" && typeof ((chosen || {}).spec || {}).markerTopic !== "string"
      ? "<p class=\"complaint\">" + SCRATCH_MARKER_WARNING + "</p>"
      : "";
  const errors = errorsOf(s);
  return (
    "<section class=\"step\" id=\"step-target\" tabindex=\"-1\"><h3>4. Target and naming</h3>" +
    "<p class=\"blurb\">Where the restored records are written. Nothing that already " +
    "exists is written to: a Restore only ever creates topics that did not exist, and " +
    "refuses outright if a mapped target topic is already there.</p>" +
    "<div class=\"field-row\">" +
    "<div class=\"field\"><label for=\"target-cluster\">target cluster</label>" +
    "<select id=\"target-cluster\" name=\"targetCluster\"" +
    invalidAttributes("target-cluster", errors.targetCluster) + ">" + clusterOptions + "</select>" +
    fieldErrorLine("target-cluster", errors.targetCluster) + "</div>" +
    "<div class=\"field\"><label for=\"target-mode\">mode</label>" +
    "<select id=\"target-mode\" name=\"mode\"" + invalidAttributes("target-mode", errors.mode) + ">" +
    options + "</select>" +
    "<p class=\"help\">newTopic writes beside what is there; scratch needs a target that " +
    "proves it is scratch.</p>" + fieldErrorLine("target-mode", errors.mode) + "</div>" +
    "</div>" +
    (labelled ? "" : "<p class=\"note\">" + TARGET_ROLE_SENTENCE + "</p>") +
    markerWarning +
    "<div class=\"field\"><label for=\"topic-prefix\">topicNaming.prefix</label>" +
    "<input id=\"topic-prefix\" name=\"topicPrefix\" value=\"" + esc(prefix) + "\"" +
    invalidAttributes("topic-prefix", errors.topicPrefix) + ">" +
    fieldErrorLine("topic-prefix", errors.topicPrefix) +
    "<p class=\"note\">The prefix defaults to what logweir_core::spec::default_topic_prefix " +
    "produces for this instant, so a topic name says both what it is and what point it was " +
    "recovered to. It is editable.</p></div>" +
    "</section>"
  );
}

/** Step 5 -- the target-topic preflight. The TARGET cluster's own most recent
 *  status, and the sentence naming what the run will do to it before the
 *  engine starts.
 *
 *  It is the cluster's status and NOT `Restore.status.topicPreflight`: that
 *  field is written by the runner, after the run this wizard has not yet
 *  requested. */
export function renderPreflightStep(state) {
  const s = state || {};
  const cluster = targetCluster(s);
  const status = (cluster || {}).status || {};
  const topics = (s.fields || {}).topics || [];
  return (
    "<section class=\"step\" id=\"step-preflight\" tabindex=\"-1\"><h3>5. Target-topic preflight</h3>" +
    "<p class=\"blurb\">The target cluster's own most recent status, and what the run will " +
    "do to it before the engine starts.</p>" +
    facts([
      ["target cluster", cell((((cluster || {}).metadata) || {}).name)],
      ["reachable", cell(status.reachable)],
      ["cluster id", cell(status.clusterId)],
      ["observed at", cell(status.observedAt)],
      ["reason", cell(status.reason)],
    ]) +
    "<p class=\"preflight\">" + preflightSentence(topics.length) + "</p>" +
    "</section>"
  );
}

/** Step 6 -- the rendered plan, its hash, the two minted names, and the one
 *  guided submit.
 *
 *  `prepared.problem` is set instead of the bytes when the fields do not make
 *  a document the runner's grammar accepts; then there is no hash to show and
 *  nothing to submit, and the step says why. `state.submission` is the
 *  wizard's mutation record: while it is pending the button is disabled, and
 *  its outcome is shown beside the button in the words every form uses. */
export function renderPlanStep(prepared, state) {
  const p = prepared || {};
  const s = state || {};
  const submission = s.submission || {};
  const pending = submission.phase === "pending";
  const errors = errorsOf(s);
  const beside = (s.errorsUnmatched || []).concat(
    NOT_INPUTS.reduce((all, name) => all.concat(errors[name] || []), []),
  );
  const renderable = typeof p.problem !== "string";
  const plan = renderable
    ? "<pre class=\"plan-bytes\" id=\"plan-bytes\">" + esc(p.bytes) + "</pre>"
    : "<p class=\"complaint\" id=\"plan-problem\">The plan cannot be rendered from these values, " +
      "so there is no hash and nothing to submit: " + esc(p.problem) + "</p>";
  return (
    "<section class=\"step\" id=\"step-plan\" tabindex=\"-1\"><h3>6. Plan, hash and names</h3>" +
    "<p class=\"blurb\">The document an approver signs, exactly as it will be sent, with " +
    "its sha256 and the two names minted from it.</p>" +
    plan +
    facts([
      ["plan hash", renderable ? "<code id=\"plan-hash-value\">" + esc(p.hash) + "</code>" : cell(null)],
      ["Restore metadata.name", renderable ? "<code>" + esc(p.restoreName) + "</code>" : cell(null)],
      ["Approval metadata.name", renderable ? "<code>" + esc(p.approvalName) + "</code>" : cell(null)],
    ]) +
    "<p class=\"note\">Both names are minted from the plan bytes before either object " +
    "exists. The Restore is created first, naming an Approval that is not there yet; the " +
    "reconciler requeues every 30 s until it arrives. Neither name is ever edited, because " +
    "neither spec can be.</p>" +
    "<div class=\"actions\">" +
    "<button type=\"button\" id=\"copy-plan\"" + (renderable ? "" : " disabled") + ">Copy plan</button>" +
    "<button type=\"button\" id=\"download-plan\"" + (renderable ? "" : " disabled") + ">Download plan</button>" +
    "</div>" +
    "<p class=\"caveat\">" + esc(COPY_CAVEAT) + "</p>" +
    "<h4>Approve it out of band</h4>" +
    "<p class=\"note\">Run this on the machine that holds the approver's private key. This " +
    "page never sees it.</p>" +
    copyBlock([APPROVE_COMMAND]) +
    "<div class=\"actions actions-final\">" +
    "<button type=\"button\" id=\"create-restore\" class=\"primary\"" +
    (pending || !renderable ? " disabled" : "") + (pending ? " aria-busy=\"true\"" : "") +
    ">Create the Restore</button>" +
    "</div>" +
    "<p class=\"note\">" + GUIDED_SUBMIT_SENTENCE + "</p>" +
    "<div class=\"form-status\" id=\"restore-submit-status\" tabindex=\"-1\">" +
    submissionStatus(submission, p, beside, s.ns) +
    "</div>" +
    "</section>"
  );
}

/** What the one submit button does, said beside it. */
export const GUIDED_SUBMIT_SENTENCE =
  "Create the Restore sends exactly the plan above, then opens what the Restore needs next: its " +
  "approval page while it waits for a verified Approval, or its operation view once one " +
  "authorises it. Submitting this plan again -- a second click, a retry after a lost response, " +
  "or the same plan after a reload -- never creates a second Restore, because its name is minted " +
  "from these bytes.";

/** Whether an outcome is about a plan this page is no longer showing.
 *
 *  An attempt records the plan it sent (`about`, see `createMutation`). A
 *  field edited while that attempt was outstanding leaves the two apart: the
 *  page then shows one plan and holds an outcome about another, and every
 *  sentence that says "submit again" is false of it. An attempt that recorded
 *  nothing (there is none in this file, and a caller may still pass one) is
 *  treated as being about what is shown, which is the older behaviour. */
export function outcomeIsElsewhere(submission, prepared) {
  const about = ((submission || {}).about) || {};
  const hash = (prepared || {}).hash;
  if (typeof about.hash !== "string" || about.hash.length === 0) {
    return false;
  }
  return typeof hash !== "string" || about.hash !== hash;
}

/** The name the attempt these words are about actually sent, which is not the
 *  name on screen once a field has changed. */
function submittedName(submission, prepared) {
  const about = ((submission || {}).about) || {};
  return typeof about.restoreName === "string" && about.restoreName.length > 0
    ? about.restoreName
    : (prepared || {}).restoreName;
}

function submissionStatus(submission, prepared, beside, ns) {
  const s = submission || {};
  const settled = s.phase === "succeeded" || s.phase === "failed";
  // THE OUTCOME MAY BE ABOUT A PLAN THAT IS NO LONGER ON SCREEN. `about` is
  // absent only before the first attempt of this page's life, so an outcome
  // that carries none is treated as being about what is shown.
  const elsewhere = settled && outcomeIsElsewhere(s, prepared);
  const name = submittedName(s, prepared);
  const aside = elsewhere
    ? "<p class=\"note\" id=\"submitted-elsewhere\">These words are about Restore <code>" +
      esc(name) + "</code>, the plan that was submitted -- not the plan shown above, which the " +
      "fields have changed since. Submitting now creates <code>" +
      esc((prepared || {}).restoreName) + "</code> instead.</p>"
    : "";
  if (s.phase === "succeeded") {
    const result = s.result || {};
    const meta = ((result.object || {}).metadata) || {};
    const shown = typeof meta.name === "string" && meta.name.length > 0 ? meta.name : name;
    // THE DURABLE LINK IS SHOWN EVEN WHEN THE DRAFT MOVED ON. An outcome about
    // another plan is exactly the case where the object would otherwise never
    // be mentioned again, and the one page that says whether it exists and
    // where it stands is its own operation view -- not the next step the
    // submit had chosen for a plan this page is no longer showing.
    const route = elsewhere || typeof result.route !== "string" || result.route.length === 0
      ? restoreOperationRoute(ns, shown)
      : result.route;
    return (
      mutationStatus(s, { kind: "Restore", name: shown }) +
      "<p class=\"note\"><a href=\"" + esc(route) + "\">Open Restore " + esc(shown) + "</a></p>" +
      aside
    );
  }
  return (
    mutationStatus(s, { kind: "Restore", name: name, resubmits: !elsewhere }, beside) +
    (elsewhere
      ? aside + "<p class=\"note\"><a href=\"" + esc(restoreOperationRoute(ns, name)) +
        "\">Open Restore " + esc(name) + " to see whether it exists</a></p>"
      : "")
  );
}

/** The field messages the wizard state carries, by input. */
function errorsOf(state) {
  return (((state || {}).errors) || {});
}

/** The exact command an approver runs, verbatim.
 *
 *  `--out` is SHOWN rather than left implicit because it defaults to
 *  `approval.json` in the caller's own working directory: an approver who did
 *  not know that has written two files somewhere they did not expect, and the
 *  sidecar lands beside `--out` with the extension replaced. The command runs
 *  on the approver's own machine, against a copy of the plan they downloaded
 *  from here; nothing about it reaches this page. */
export const APPROVE_COMMAND =
  "logweir drill approve --spec <file> --key <privkey> --approver <id> " +
  "--ticket <id> --subject-kind Restore --out <file>";

/** The six steps as the stepper names them: id, number and title, in the
 *  order the sections render. The titles are the section headings' own
 *  words. */
export const STEPS = Object.freeze([
  { id: "step-archive", title: "Archive" },
  { id: "step-backup-set", title: "Backup set" },
  { id: "step-point-in-time", title: "Point in time" },
  { id: "step-target", title: "Target and naming" },
  { id: "step-preflight", title: "Target-topic preflight" },
  { id: "step-plan", title: "Plan, hash and names" },
]);

/** THE STEPPER'S STATE: which steps are done, which one you are on, what is
 *  next, and which one needs attention -- one entry per step, in order.
 *
 *  All six sections are on the page at once, so "the step you are on" is the
 *  FIRST one whose inputs are not yet whole: an archive with no URL, a chosen
 *  run with no backup set, a point outside the covered window, a target with
 *  no mode or no prefix, a target cluster whose recorded status is not
 *  reachable. When the five input steps are whole, the plan step is the
 *  current one and reads `ready`. A step that is not whole because the page
 *  has a complaint about it reads `attention`; one that merely waits its
 *  turn reads `todo`.
 *
 *  THIS DECIDES NOTHING THE RUNNER DECIDES. It reads the same state the six
 *  sections read and summarises it; the create button is never gated on it,
 *  because the client-side checks are a convenience and never the gate. Pure:
 *  no DOM, no network, no clock. */
export function stepStates(state) {
  const s = state || {};
  const fields = s.fields || {};
  const targetFields = fields.target || {};
  const chosen = chosenBackup(s);
  const covered = coveredOf(s);
  const value =
    typeof fields.pointInTime === "string" && fields.pointInTime.length > 0
      ? fields.pointInTime
      : rfc3339(covered.toMs);
  const target = targetCluster(s);
  const targetStatus = (target || {}).status || {};
  const targetSpec = (target || {}).spec || {};
  const setChosen =
    chosen !== null &&
    typeof ((chosen.status || {}).backupId) === "string" &&
    chosen.status.backupId.length > 0;
  const whole = [
    typeof s.archiveUrl === "string" && s.archiveUrl.length > 0,
    setChosen,
    windowComplaint(value, covered) === null,
    target !== null &&
      TARGET_MODES.indexOf(targetFields.mode) !== -1 &&
      typeof targetFields.topicPrefix === "string" &&
      targetFields.topicPrefix.length > 0,
    target !== null && targetStatus.reachable === true,
  ];
  const attention = [
    false,
    chosen !== null && !setChosen,
    !whole[2],
    target !== null &&
      targetFields.mode === "scratch" &&
      typeof targetSpec.markerTopic !== "string",
    target !== null && !whole[4],
  ];
  let firstOpen = 5;
  for (let i = 0; i < 5; i += 1) {
    if (!whole[i]) {
      firstOpen = i;
      break;
    }
  }
  return STEPS.map((step, i) => {
    let status;
    if (i === 5) {
      status = firstOpen === 5 ? "ready" : "todo";
    } else if (whole[i]) {
      status = attention[i] ? "attention" : "done";
    } else {
      status = attention[i] ? "attention" : "todo";
    }
    return {
      id: step.id,
      number: i + 1,
      title: step.title,
      status: status,
      current: i === firstOpen,
    };
  });
}

/** The word a stepper entry carries beside its title. Text, so the state is
 *  never colour alone. */
function stepWord(step) {
  if (step.status === "done") {
    return "done";
  }
  if (step.status === "attention") {
    return "needs attention";
  }
  if (step.status === "ready") {
    return "review and create";
  }
  return step.current ? "you are here" : "next";
}

/** The stepper: an ordered list of the six steps, each a button that scrolls
 *  to its section, carrying the step's number, its title and its state in
 *  words. The current one is marked `aria-current="step"`. */
export function renderStepper(state) {
  const items = stepStates(state)
    .map((step) => {
      const classes =
        "stepper-item is-" + step.status + (step.current ? " is-current" : "");
      return (
        "<li class=\"" + classes + "\">" +
        "<button type=\"button\" class=\"stepper-link\" data-target=\"" + step.id + "\"" +
        (step.current ? " aria-current=\"step\"" : "") + ">" +
        "<span class=\"stepper-num\">" + String(step.number) + "</span>" +
        "<span class=\"stepper-title\">" + esc(step.title) + "</span>" +
        "<span class=\"stepper-status\">" + stepWord(step) + "</span>" +
        "</button></li>"
      );
    })
    .join("");
  return "<ol class=\"stepper\" aria-label=\"The six steps\">" + items + "</ol>";
}

/** The whole wizard, all six steps, over one state. */
export async function renderRestoreWizard(state) {
  return renderPreparedWizard(state, await preparePlanOrProblem(state));
}

/** The same wizard over a plan already prepared -- so the mount half knows the
 *  exact hash it put on screen, and can refuse to submit any other. */
export function renderPreparedWizard(state, prepared) {
  const s = state || {};
  return (
    "<h2>Restore wizard</h2>" +
    "<p class=\"blurb\">Six steps, all on this page: the archive, the backup set, the " +
    "point in time, the target, the preflight, and the plan whose bytes the Restore " +
    "carries. Every value was read from this namespace's own objects or is editable " +
    "below.</p>" +
    (s.editing
      ? "<p class=\"immutable-note\">" + RESTORE_IMMUTABLE_SENTENCE + "</p>"
      : "") +
    (s.draftRestored === true
      ? "<div class=\"draft-note\"><p class=\"note\">" + DRAFT_RESTORED_SENTENCE + "</p>" +
        "<div class=\"actions\"><button type=\"button\" id=\"discard-draft\">Discard these edits</button></div></div>"
      : "") +
    renderStepper(state) +
    renderArchiveStep(state) +
    renderBackupSetStep(state) +
    renderPointInTimeStep(state) +
    renderTargetStep(state) +
    renderPreflightStep(state) +
    renderPlanStep(prepared, state)
  );
}

/** Said when the wizard reopens with edits made earlier in this page's life. */
export const DRAFT_RESTORED_SENTENCE =
  "Your unsubmitted edits to this plan, made earlier on this page for the same backup set, are " +
  "back. They lived in this page's memory only: a reload would have started the wizard afresh.";

/** [`preparePlan`], or `{problem}` naming the field the runner's grammar
 *  needs when the values do not make a document. Never throws for that. */
export async function preparePlanOrProblem(state) {
  try {
    return await preparePlan(state);
  } catch (error) {
    if (error instanceof TypeError || error instanceof RangeError) {
      return { problem: String(error.message) };
    }
    throw error;
  }
}

/** The page's own checks on the values a Restore is created from, by input.
 *  A point OUTSIDE the covered window is not here: that check is a convenience
 *  and never the gate (see `CONVENIENCE_SENTENCE`). A point that is not an
 *  instant at all is, because the API server refuses it (`format: date-time`). */
export function validateRestore(state) {
  const s = state || {};
  const fields = s.fields || {};
  const target = fields.target || {};
  const problems = Object.create(null);
  if (epochMs(fields.pointInTime) === null) {
    problems.pointInTime = "an RFC 3339 instant, such as 2026-09-07T14:05:00Z";
  }
  if (TARGET_MODES.indexOf(target.mode) === -1) {
    problems.mode = "one of " + TARGET_MODES.join(", ");
  }
  if (typeof target.topicPrefix !== "string" || target.topicPrefix.length === 0) {
    problems.topicPrefix = "the prefix every restored topic's name starts with";
  }
  if (targetCluster(s) === null) {
    problems.targetCluster = "choose the KafkaCluster the restore writes to";
  }
  if (typeof ((fields.evidence || {}).bucket) !== "string" || fields.evidence.bucket.length === 0) {
    problems.evidenceBucket = "the bucket the signed evidence is written to";
  }
  if (typeof s.archiveSecretName === "string" && s.archiveSecretName.length > 0 &&
    !isObjectName(s.archiveSecretName)) {
    problems.archiveSecret = "a Secret name is lowercase letters, digits, '-' and '.'";
  }
  if (typeof s.archiveUrl !== "string" || s.archiveUrl.length === 0) {
    problems.archive = "no archive URL was read from a Backup in this namespace";
  }
  if (typeof fields.backupSetRef !== "string" || fields.backupSetRef.length === 0) {
    problems.backupSet = "no completed backup set is chosen";
  }
  return problems;
}

/** The wizard's editable values, as a draft keeps them. */
export function wizardDraftValues(state) {
  const s = state || {};
  const f = s.fields || {};
  const target = f.target || {};
  const source = f.source || {};
  return {
    backupSetRef: f.backupSetRef,
    pointInTime: f.pointInTime,
    mode: target.mode,
    topicPrefix: target.topicPrefix,
    targetCluster: s.targetClusterName,
    endpoint: source.endpoint,
    region: source.region,
    pathStyle: source.pathStyle === true,
    evidenceBucket: (f.evidence || {}).bucket,
    archiveSecret: s.archiveSecretName,
  };
}

/** Puts a kept draft back into a freshly built state -- only when the draft
 *  was made for the backup set this state chose, because a point in time and
 *  a prefix chosen for one set are not edits to another. Returns whether it
 *  applied. */
export function applyWizardDraft(state, draft) {
  const d = draft || {};
  if (typeof d.backupSetRef !== "string" || d.backupSetRef !== ((state || {}).fields || {}).backupSetRef) {
    return false;
  }
  if (typeof d.pointInTime === "string") {
    state.fields.pointInTime = d.pointInTime;
  }
  if (typeof d.mode === "string") {
    state.fields.target.mode = d.mode;
  }
  if (typeof d.targetCluster === "string" &&
    itemsOf(state.clusters).some((c) => ((c || {}).metadata || {}).name === d.targetCluster)) {
    selectTarget(state, d.targetCluster);
  }
  if (typeof d.topicPrefix === "string") {
    state.fields.target.topicPrefix = d.topicPrefix;
  }
  for (const block of [state.fields.source, state.fields.evidence]) {
    if (typeof d.endpoint === "string") {
      block.endpoint = d.endpoint;
    }
    if (typeof d.region === "string") {
      block.region = d.region;
    }
    if (typeof d.pathStyle === "boolean") {
      block.pathStyle = d.pathStyle;
      block.allowHttp = d.pathStyle;
    }
  }
  if (typeof d.evidenceBucket === "string") {
    state.fields.evidence.bucket = d.evidenceBucket;
    state.evidenceBucket = d.evidenceBucket;
  }
  if (typeof d.archiveSecret === "string") {
    state.archiveSecretName = d.archiveSecret;
  }
  return true;
}

/** The prefill an "edit" produces: a NEW draft, never a patch.
 *
 *  `Restore.spec` is CEL-immutable, so the only thing an edit can mean is
 *  "start from these values". The fields the CRD carries are lifted across;
 *  the rest of the plan document -- the sample window, the evidence sink, the
 *  bootstrap servers -- is not on `Restore.spec` at all and stays as the
 *  wizard has it. */
export function draftFrom(object, fields) {
  const spec = ((object || {}).spec) || {};
  const target = spec.target || {};
  const base = fields || {};
  const nextTarget = Object.assign({}, base.target || {});
  if (typeof target.mode === "string") {
    nextTarget.mode = target.mode;
  }
  if (typeof (target.topicNaming || {}).prefix === "string") {
    nextTarget.topicPrefix = target.topicNaming.prefix;
  }
  const next = Object.assign({}, base, { target: nextTarget });
  if (typeof spec.pointInTime === "string") {
    next.pointInTime = spec.pointInTime;
  }
  if (typeof spec.backupSetRef === "string") {
    next.backupSetRef = spec.backupSetRef;
  }
  return next;
}

// -------------------------------------------------------------- the plan half

/** Step 6's one state transition: the plan bytes, hashed and named.
 *
 *  The bytes are rendered ONCE and read from `state.planBytes` by everything
 *  that follows. A page that rendered them again for the submit would be a
 *  page with two documents and one hash. */
export async function preparePlan(state) {
  const s = state || {};
  const bytes =
    typeof s.planBytes === "string" ? s.planBytes : renderPlanBytes(s.fields);
  const names = await mintNames(bytes);
  return {
    bytes: bytes,
    hash: await planHash(bytes),
    restoreName: names.restoreName,
    approvalName: names.approvalName,
  };
}

// EVERYTHING BETWEEN THE TWO MARKERS BELOW IS THE SUBMIT REGION, and
// `crates/logweir/tests/ui_lint.rs::the_wizard_never_reserialises_the_plan_bytes`
// asserts that six string-transforming tokens -- the two JSON entry points, the
// structured clone, the two whitespace/Unicode normalisers and the plan
// document's file extension -- appear nowhere inside it. The token list is
// spelled out in the Rust test and deliberately NOT here, because a comment
// naming the tokens would itself be a hit; this paragraph sits outside the
// region for the same reason.
//
// The behaviour arm asserts the same property the only way that can actually
// hold it -- by byte comparison over a fixture whose plan ends in two spaces
// and a newline and carries a non-ASCII name. JavaScript has no string
// identity operator, so an `===` arm would hold under a mutant that
// reserialised to an equal string and would fail to catch the one that did
// not.

// SUBMIT-REGION-BEGIN

/** The object `create` posts. `metadata.name` is the minted restore name and
 *  `spec.approvalRef.name` is the minted approval name, which DOES NOT EXIST
 *  YET -- the reconciler requeues at 30 s on `ApprovalNotVerified` until it
 *  does (interface I19). `spec.planBytes` is the string from `prepared`,
 *  unchanged.
 *
 *  `spec.sourceArchive` is BOTH HALVES of `ArchiveRef`: the URL, and the name
 *  of the Secret the runner reads the archive with. The second is what makes
 *  the first usable -- the controller injects the object-store credential into
 *  the runner Job only when `secretRef` is set -- and an object that omitted it
 *  is accepted by the API server and fails later, at the archive.
 *
 *  THE KEY IS ABSENT AND NEVER `null` WHEN THERE IS NO NAME. `secretRef` is an
 *  OBJECT in the CRD schema, and `status` and `spec` are both structural: a
 *  literal `null` is a type error the API server reports as a 422 over a field
 *  the operator deliberately left blank. */
export function restoreBody(state, prepared) {
  const s = state || {};
  const p = prepared || {};
  const fields = s.fields || {};
  const target = fields.target || {};
  const sourceArchive = { url: s.archiveUrl };
  if (typeof s.archiveSecretName === "string" && s.archiveSecretName.length > 0) {
    sourceArchive.secretRef = { name: s.archiveSecretName };
  }
  return {
    apiVersion: "logweir.dev/v1alpha1",
    kind: "Restore",
    metadata: { name: p.restoreName },
    spec: {
      planBytes: p.bytes,
      approvalRef: { name: p.approvalName },
      sourceArchive: sourceArchive,
      backupSetRef: fields.backupSetRef,
      pointInTime: fields.pointInTime,
      target: {
        clusterRef: { name: s.targetClusterName },
        mode: target.mode,
        topicNaming: { prefix: target.topicPrefix },
      },
      deadlineSeconds: typeof s.deadlineSeconds === "number" ? s.deadlineSeconds : 3600,
    },
  };
}

/** The route "Request approval" navigates to. `subjectRef` and `planHash` come
 *  from HERE and never from the approval documents: the approvals page is
 *  forbidden from parsing those, so the hash cannot be lifted out of them
 *  either. */
export function approvalRoute(state, prepared) {
  const s = state || {};
  const p = prepared || {};
  // There is no implicit namespace on a router that deliberately refuses to
  // guess one. In particular, `default` is a real selected namespace and
  // must cross this hand-off explicitly rather than being elided as a legacy
  // shorthand.
  const ns = typeof s.ns === "string" ? s.ns.trim() : "";
  return (
    "#/approvals?subject=" +
    encodeURIComponent(p.restoreName) +
    "&hash=" +
    encodeURIComponent(p.hash) +
    "&name=" +
    encodeURIComponent(p.approvalName) +
    (ns.length > 0 ? "&ns=" + encodeURIComponent(ns) : "")
  );
}

/** THE GUIDED SUBMIT: creates the `Restore` -- the FIRST of the two creates,
 *  with a dangling `approvalRef` -- and says where the journey goes next.
 *
 *  In order, and nothing is sent until the first three pass:
 *   1. the page's own checks on the values (`validateRestore`);
 *   2. the plan is prepared -- rendered, hashed and named -- and when a route
 *      token is given and has left meanwhile, nothing is sent (`null`);
 *   3. when `options.reviewedHash` is given, the prepared hash must be it: the
 *      bytes a click sends are the bytes that were on screen when it was
 *      clicked, and a field changed in between is a refusal, not a surprise;
 *   4. one create, WITHOUT a route signal. Its name is minted from the bytes,
 *      so `409 AlreadyExists` is this plan submitted before: an existing
 *      Restore with exactly this spec is this operation, and any other is a
 *      conflict (`resolveExisting`);
 *   5. the destination: the Restore's operation view when an Approval already
 *      authorises exactly this Restore, else its approval page.
 *
 *  Returns `{outcome, object, route, prepared}`, or `null` when step 2 found
 *  the route gone. */
export async function submitRestore(state, deps, lifecycle, options) {
  const api = deps || API;
  const s = state || {};
  const problems = validateRestore(s);
  if (Object.keys(problems).length > 0) {
    throw invalidInput(problems);
  }
  const prepared = await preparePlan(s);
  // Hashing and minting are client-side preparation, not a submitted durable
  // operation. Navigation while they run therefore disarms the pending
  // action; once `create` starts, deliberately pass no route signal so an
  // accepted server mutation can finish after navigation.
  if (!active(lifecycle)) {
    return null;
  }
  const reviewed = (options || {}).reviewedHash;
  if (typeof reviewed === "string" && reviewed !== prepared.hash) {
    throw refusal(
      "the plan changed after it was displayed (the page showed " + reviewed + ", the current " +
        "values hash to " + prepared.hash + "); review the plan shown now and submit again",
      { reviewedHash: reviewed, preparedHash: prepared.hash },
    );
  }
  const body = restoreBody(s, prepared);
  let created;
  try {
    created = { outcome: "created", object: await api.create(s.ns, PLURAL, body) };
  } catch (error) {
    created = await resolveExisting(api, s.ns, PLURAL, body, RESTORE_SPEC_RULES, error);
  }
  const route = await restoreDestination(api, s, prepared, created.object);
  return { outcome: created.outcome, object: created.object, route: route, prepared: prepared };
}

/** Where a submitted Restore goes next, under today's approval semantics:
 *  every Restore waits for a verified Approval. The operation view when the
 *  Approval its `spec.approvalRef` names already authorises exactly this
 *  Restore -- this name, this namespace, this UID, this plan -- and its
 *  approval page otherwise. A read that fails is not an authorisation, so it
 *  lands on the approval page, which reads the state again for itself. */
export async function restoreDestination(api, state, prepared, restore) {
  const s = state || {};
  const p = prepared || {};
  const meta = (restore || {}).metadata || {};
  const approvalName = (((restore || {}).spec || {}).approvalRef || {}).name || p.approvalName;
  let approval = null;
  try {
    approval = await api.get(s.ns, APPROVALS, approvalName);
  } catch (unread) {
    approval = null;
  }
  if (approval !== null && approvalAuthorizes(approval, restore, p.hash)) {
    return restoreOperationRoute(s.ns, typeof meta.name === "string" ? meta.name : p.restoreName);
  }
  return approvalRoute(s, p);
}

// SUBMIT-REGION-END

// --------------------------------------------------------------- private half

function archiveFor(state, clusterName) {
  for (const backup of itemsOf(state.backups)) {
    const spec = backup.spec || {};
    if (((spec.sourceRef || {}).name) === clusterName) {
      return (spec.archive || {}).url;
    }
  }
  return null;
}

/** The name beside the URL above, off the SAME archive reference. `null` when
 *  that archive names no Secret, which the table renders as an empty cell. */
function archiveSecretFor(state, clusterName) {
  for (const backup of itemsOf(state.backups)) {
    const spec = backup.spec || {};
    if (((spec.sourceRef || {}).name) === clusterName) {
      const secret = ((spec.archive || {}).secretRef) || {};
      return typeof secret.name === "string" ? secret.name : null;
    }
  }
  return null;
}

function backupsOf(state) {
  return itemsOf(state.backups).filter((backup) => {
    if (typeof state.archiveUrl !== "string" || state.archiveUrl.length === 0) {
      return true;
    }
    return (((backup.spec || {}).archive) || {}).url === state.archiveUrl;
  });
}

/** THE COVERED WINDOW, READ FROM `windowCovered.fromMs`/`toMs`.
 *
 *  Interface I22 names those two keys and they are integers. A page that read
 *  `covered.from`/`covered.to` would get `undefined` twice, default step 3 to
 *  nothing, and render the window message with `Invalid Date` on both bounds --
 *  which is what the behaviour suite's window row exists to catch. */
function coveredOf(state) {
  const chosen = chosenBackup(state);
  const covered = ((chosen || {}).status || {}).windowCovered || {};
  return { fromMs: covered.fromMs, toMs: covered.toMs };
}

/** The `Backup` whose run COMPLETED most recently, or `null`.
 *
 *  THE LAST ROW OF A LIST IS NOT THE NEWEST COMPLETED RUN, and Task 28
 *  measured the difference on a live cluster: a two-minute schedule produced
 *  three `Backup` objects in six minutes and the page read a different one on every
 *  reload -- the chosen set, its covered window, the plan bytes, the plan hash
 *  and BOTH minted names all changing under the operator between one render
 *  and the next. Worse, the newest object is usually the one still RUNNING,
 *  which has no `backupId` and no `windowCovered` at all.
 *
 *  So: only a `Succeeded` run is offered, and among those the one whose
 *  `Complete` condition transitioned latest (falling back to the creation
 *  timestamp, then to list order). A tie or a missing timestamp keeps the
 *  later-listed object, which is `kubectl`'s own order.
 *
 *  `completedAt` is read off the `Complete` condition rather than off
 *  `windowCovered.toMs`: the covered window is about the RECORDS, and two runs
 *  can cover windows that end in the other order from the order they ran. */
function succeededBackups(state) {
  return backupsOf(state).filter((b) => ((b.status || {}).phase) === "Succeeded");
}

function completedAt(backup) {
  const conditions = ((backup || {}).status || {}).conditions;
  if (Array.isArray(conditions)) {
    for (const condition of conditions) {
      if ((condition || {}).type === "Complete") {
        const at = epochMs(condition.lastTransitionTime);
        if (at !== null) {
          return at;
        }
      }
    }
  }
  return epochMs(((backup || {}).metadata || {}).creationTimestamp);
}

function newestSucceeded(state) {
  const candidates = succeededBackups(state);
  let best = null;
  for (const backup of candidates) {
    if (best === null) {
      best = backup;
      continue;
    }
    const left = completedAt(backup);
    const right = completedAt(best);
    if (left === null || right === null ? true : left >= right) {
      best = backup;
    }
  }
  return best;
}

function chosenBackup(state) {
  const wanted = (state.fields || {}).backupSetRef;
  const candidates = backupsOf(state);
  // THE STRING MUST BE A STRING. `wanted` is `undefined` before a set has been
  // chosen, and a `Backup` that is still RUNNING has no `backupId` either -- so
  // an equality test that did not check the type matched the running object
  // and handed the page a `Backup` with no covered window at all.
  if (typeof wanted === "string" && wanted.length > 0) {
    for (const backup of candidates) {
      if (((backup.status || {}).backupId) === wanted) {
        return backup;
      }
    }
  }
  const succeeded = newestSucceeded(state);
  if (succeeded !== null) {
    return succeeded;
  }
  return candidates.length > 0 ? candidates[candidates.length - 1] : null;
}

/** The chosen target: the one named in the state if it is still in the list,
 *  else [`firstTarget`]'s default. The two must agree, because step 4's
 *  `<select>` marks the same cluster `selected` and `bootstrapOf` reads this
 *  one into the plan. */
function targetCluster(state) {
  for (const cluster of itemsOf(state.clusters)) {
    if ((cluster.metadata || {}).name === state.targetClusterName) {
      return cluster;
    }
  }
  return firstTarget(state.clusters);
}

/** The default prefix for an instant, or the empty string when there is no
 *  instant to derive one from. */
function prefixFor(pointInTime) {
  // An input that is not (yet) an instant has no default prefix; it is the
  // point-in-time field's message that says so, not a thrown render.
  if (typeof pointInTime !== "string" || epochMs(pointInTime) === null) {
    return "";
  }
  return defaultTopicPrefix(pointInTime);
}

/** The client-side window complaint, or `null`. It compares MILLISECONDS on
 *  both sides. */
function windowComplaint(value, covered) {
  const at = epochMs(value);
  if (at === null) {
    return windowMessage(covered.fromMs, covered.toMs);
  }
  if (typeof covered.fromMs !== "number" || typeof covered.toMs !== "number") {
    return windowMessage(covered.fromMs, covered.toMs);
  }
  if (at < covered.fromMs || at > covered.toMs) {
    return windowMessage(covered.fromMs, covered.toMs);
  }
  return null;
}

// --------------------------------------------------------------- mount half

export async function mountRestoreWizard(node, ns, parse, deps, lifecycle) {
  const api = deps || API;
  try {
    const collections = await Promise.all([
      api.list(ns, CLUSTERS, readOptions(lifecycle)),
      api.list(ns, BACKUPS, readOptions(lifecycle)),
    ]);
    if (!active(lifecycle)) {
      return;
    }
    const clusters = collections[0];
    const backups = collections[1];
    if (completedBackups(backups).length === 0) {
      replace(node, parse(renderNoCompletedBackup(ns, backups)));
      return;
    }
    const state = initialState(ns, clusters, backups);
    const key = formKey(ns, WIZARD_FORM);
    const record = mutationFor(key);
    if (record.state.phase === "succeeded") {
      dropDraft(key);
    }
    const draft = readDraft(key);
    if (draft !== null) {
      if (applyWizardDraft(state, draft)) {
        state.draftRestored = true;
      } else {
        dropDraft(key);
      }
    }
    await renderAndWire(node, state, parse, api, lifecycle);
  } catch (error) {
    if (!cancelled(error, lifecycle) && active(lifecycle)) {
      replace(node, errorBox(error));
    }
  }
}

/** Renders the wizard over `state` -- with its mutation record and its field
 *  messages -- and wires what was rendered to the plan it shows. */
async function renderAndWire(node, state, parse, api, lifecycle) {
  const record = mutationFor(formKey(state.ns, WIZARD_FORM));
  state.submission = record.state;
  if (record.state.phase === "failed") {
    const found = fieldErrors(record.state.error, WIZARD_FIELD_PATHS);
    state.errors = found.fields;
    state.errorsUnmatched = found.unmatched;
  } else {
    state.errors = null;
    state.errorsUnmatched = [];
  }
  const prepared = await preparePlanOrProblem(state);
  if (!active(lifecycle)) {
    return false;
  }
  replace(node, parse(renderPreparedWizard(state, prepared)));
  wire(node, state, parse, api, lifecycle, prepared);
  return true;
}

/** The state the six steps read: the two collections, the chosen archive and
 *  target, and the WHOLE plan document's fields.
 *
 *  THE FIELDS OBJECT IS COMPLETE FROM THE FIRST RENDER, because a plan is
 *  hashed as a whole and an approver signs a whole document. A wizard that
 *  built half a document and left the rest to a later edit would be offering
 *  to invalidate its own approval. Every value below is either read from a
 *  Kubernetes object, fixed by a constraint, or editable in a step -- and the
 *  three object-store settings that no CRD records -- endpoint, region and the
 *  `path_style` flag -- are editable in step 1 rather than left out, because
 *  the runner reads them from these bytes and from nowhere else. */
export function initialState(ns, clusters, backups) {
  // THE NEWEST *COMPLETED* RUN, NOT THE LAST ROW. `chosenBackup` is asked with
  // no `backupSetRef` yet, so it answers with the newest `Succeeded` object --
  // the same one every later render will pick, and the only kind that carries
  // a `backupId` and a `windowCovered` to build a plan from. See
  // `newestSucceeded` for what Task 28 measured when this took the last row.
  const newest = chosenBackup({ backups: backups, fields: {} });
  const spec = (newest || {}).spec || {};
  const status = (newest || {}).status || {};
  const covered = status.windowCovered || {};
  const pointInTime = rfc3339(covered.toMs);
  const archive = spec.archive || {};
  const archiveUrl = archive.url;
  // BOTH HALVES OF THE ARCHIVE REFERENCE, FROM THE SAME OBJECT. A URL taken
  // from one Backup and a credential taken from another would be two archives
  // and one name for them.
  const archiveSecretName =
    typeof ((archive.secretRef || {}).name) === "string" ? archive.secretRef.name : "";
  const target = firstTarget(clusters);
  const store = { region: "", endpoint: "", pathStyle: false, allowHttp: false };
  return {
    ns: ns,
    clusters: clusters,
    backups: backups,
    archiveUrl: archiveUrl,
    archiveSecretName: archiveSecretName,
    evidenceBucket: "logweir-evidence",
    targetClusterName: ((target || {}).metadata || {}).name,
    editing: null,
    deadlineSeconds: 3600,
    fields: {
      backupSetRef: status.backupId,
      topics: Array.isArray(spec.topics) ? spec.topics : [],
      pointInTime: pointInTime,
      source: Object.assign(
        { bucket: bucketOf(archiveUrl), prefix: prefixOf(archiveUrl) },
        store,
      ),
      target: {
        bootstrapServers: ((target || {}).spec || {}).bootstrapServers || [],
        auth: targetAuth(target),
        mode: TARGET_MODES[1],
        topicPrefix: prefixFor(pointInTime),
        // UNREAD in `newTopic` mode and still required by the grammar (it has
        // no serde default, because an empty prefix maps every source topic
        // onto ITSELF). The same string, so switching the mode select is a
        // one-value change and never a document that will not parse.
        topicMappingPrefix: prefixFor(pointInTime),
        markerTopic: "logweir.scratch",
        replicationFactor: 1,
        teardown: "delete",
      },
      // THE SAMPLE WINDOW IS NOT THE RESTORE WINDOW. This one bounds the
      // per-record reconciliation and defaults to the set's own covered range,
      // which is the only window the cluster told this page about; the
      // RESTORE's floor is the archive's earliest covered timestamp, read from
      // the manifest by the runner, and is never a field here.
      sample: {
        windowStart: rfc3339(covered.fromMs),
        windowEnd: rfc3339(covered.toMs),
        recordsPerPartition: 25,
        anchor: "head",
      },
      objectives: {},
      evidence: Object.assign(
        { bucket: "logweir-evidence", prefix: EVIDENCE_PREFIX },
        store,
      ),
    },
  };
}

// Copy public settings only. The controller projects the selected cluster's
// Secret into the runner; its reference and contents do not belong in a plan.
function targetAuth(cluster) {
  const auth = ((cluster || {}).spec || {}).auth || {};
  if (!auth.mode || auth.mode === "plaintext") {
    return undefined;
  }
  return { mode: auth.mode, username: auth.username, tls: auth.tls === true };
}
export function selectTarget(state, name) {
  state.targetClusterName = name;
  const cluster = targetCluster(state);
  state.fields.target.bootstrapServers = ((cluster || {}).spec || {}).bootstrapServers || [];
  state.fields.target.auth = targetAuth(cluster);
}

/** The cluster step 4 preselects: one labelled `role: target` if the namespace
 *  has one, else the SOURCE cluster, else the first cluster there is.
 *
 *  NEVER `null` WHEN THE NAMESPACE HAS A CLUSTER. The old version returned
 *  `null` for a namespace whose only cluster is `role: source`, which left
 *  `target.bootstrapServers` empty and made `renderPlanBytes` throw -- on the
 *  one-cluster `newTopic` walk that IS Demo 1. The runner has no such rule
 *  (`drill/phase0_admit.rs`'s `TargetMode::NewTopic` arm is empty), so the
 *  page had invented a requirement the product does not have. */
function firstTarget(clusters) {
  const all = itemsOf(clusters);
  for (const cluster of all) {
    if (((cluster.spec || {}).role) === "target") {
      return cluster;
    }
  }
  for (const cluster of all) {
    if (((cluster.spec || {}).role) === "source") {
      return cluster;
    }
  }
  return all.length > 0 ? all[0] : null;
}

function wire(node, state, parse, api, lifecycle, prepared) {
  const key = formKey(state.ns, WIZARD_FORM);
  const record = mutationFor(key);
  const point = node.querySelector("#point-in-time");
  const prefix = node.querySelector("#topic-prefix");
  const mode = node.querySelector("#target-mode");
  const cluster = node.querySelector("#target-cluster");
  const endpoint = node.querySelector("#store-endpoint");
  const region = node.querySelector("#store-region");
  const pathStyle = node.querySelector("#store-pathStyle");
  const evidenceBucket = node.querySelector("#evidence-bucket");
  const archiveSecret = node.querySelector("#archive-secret");
  const refresh = async () => {
    if (!active(lifecycle)) {
      return;
    }
    if (point !== null) {
      state.fields.pointInTime = valueOf(point);
    }
    if (mode !== null) {
      state.fields.target.mode = valueOf(mode);
    }
    if (prefix !== null) {
      // An emptied prefix is the default prefix again: the field SHOWS the
      // default when the value is empty, and the plan must be what it shows.
      state.fields.target.topicPrefix = valueOf(prefix) || prefixFor(state.fields.pointInTime);
    }
    if (cluster !== null) {
      selectTarget(state, valueOf(cluster));
    }
    for (const block of [state.fields.source, state.fields.evidence]) {
      if (endpoint !== null) {
        block.endpoint = valueOf(endpoint);
      }
      if (region !== null) {
        block.region = valueOf(region);
      }
      if (pathStyle !== null) {
        block.pathStyle = pathStyle.checked === true;
        // An endpoint that is not AWS S3 is reached over whatever transport the
        // adopter gave it; the flag travels with the addressing style because
        // the two are set together for every on-premises object store this
        // product has been run against.
        block.allowHttp = pathStyle.checked === true;
      }
    }
    if (evidenceBucket !== null) {
      state.fields.evidence.bucket = valueOf(evidenceBucket);
      state.evidenceBucket = state.fields.evidence.bucket;
    }
    // THE NAME ONLY, AND IT IS NOT A PLAN FIELD. It goes on the `Restore`'s
    // own spec, beside the archive URL, and never into the bytes an approver
    // signs: the runner takes the credential from its environment, which the
    // controller fills from the named Secret.
    if (archiveSecret !== null) {
      state.archiveSecretName = valueOf(archiveSecret);
    }
    if (!active(lifecycle)) {
      return;
    }
    // THE EDIT IS KEPT BEFORE ANYTHING ELSE, and a settled outcome about the
    // previous plan is cleared: it described bytes that are no longer these.
    //
    // WHICH OUTCOMES AN EDIT CLEARS. An outcome about these BYTES -- a refusal,
    // a 422, a conflict -- described bytes that are no longer these, so it
    // goes. An outcome about whether an OBJECT EXISTS does not: editing a
    // field does not un-create a Restore, and does not settle one whose fate
    // is unknown.
    //
    // That is the defect this rule replaces. A create that timed out was NOT
    // cancelled and may still be accepted; clearing the record on the next
    // edit took its attempt number with it, so the late 201 for that Restore
    // found no answerable attempt and was dropped -- leaving an object nobody
    // was ever told about. Now the record is kept, and `submissionStatus` says
    // which plan it is about and links to it by name.
    keepDraft(key, wizardDraftValues(state), WIZARD_DRAFT_FIELDS);
    if (record.state.phase === "failed" && record.state.kind !== "unknown") {
      record.clear();
    }
    await renderAndWire(node, state, parse, api, lifecycle);
  };
  for (const field of [
    point,
    prefix,
    mode,
    cluster,
    endpoint,
    region,
    pathStyle,
    evidenceBucket,
    archiveSecret,
  ]) {
    if (field !== null) {
      listen(field, "change", refresh, lifecycle);
    }
  }

  // The stepper: each entry scrolls its section into view and hands it focus,
  // so a keyboard reader lands where a pointer reader looks. Motion follows
  // the reader's own preference.
  for (const link of node.querySelectorAll(".stepper-link")) {
    listen(link, "click", () => {
      if (!active(lifecycle)) {
        return;
      }
      const section = node.querySelector("#" + link.getAttribute("data-target"));
      if (section === null) {
        return;
      }
      const still =
        typeof window.matchMedia === "function" &&
        window.matchMedia("(prefers-reduced-motion: reduce)").matches;
      section.scrollIntoView({ behavior: still ? "auto" : "smooth", block: "start" });
      section.focus({ preventScroll: true });
    }, lifecycle);
  }

  const copy = node.querySelector("#copy-plan");
  if (copy !== null) {
    listen(copy, "click", async () => {
      if (!active(lifecycle)) {
        return;
      }
      const pre = node.querySelector("#plan-bytes");
      if (pre !== null && navigator.clipboard) {
        await navigator.clipboard.writeText(pre.textContent);
      }
    }, lifecycle);
  }

  const download = node.querySelector("#download-plan");
  if (download !== null) {
    listen(download, "click", async () => {
      if (!active(lifecycle)) {
        return;
      }
      const prepared = await preparePlan(state);
      if (active(lifecycle)) {
        downloadPlan(prepared);
      }
    }, lifecycle);
  }

  const discard = node.querySelector("#discard-draft");
  if (discard !== null) {
    listen(discard, "click", () => {
      if (!active(lifecycle) || record.pending()) {
        return;
      }
      dropDraft(key);
      record.clear();
      mountRestoreWizard(node, state.ns, parse, api, lifecycle);
    }, lifecycle);
  }

  // THE ONE RECORD FOR THIS NAMESPACE'S WIZARD. Pending disables the button in
  // place; a failure re-renders the wizard with its messages and keeps every
  // value; success opens the destination the submit chose -- while this route
  // is still the current one. A route left in between keeps the outcome in the
  // record, and the next mount of the wizard shows it with a link.
  watchMutation(node, key, record, (settled) => {
    // A SETTLEMENT ABOUT THE PLAN ON SCREEN OWNS THE PAGE; one about a plan the
    // fields have moved on from does not. A late answer to a timed-out attempt
    // arrives after the operator has started editing: navigating away from
    // those edits, or dropping them as "consumed", would answer one problem by
    // causing another. Instead the wizard re-renders in place and
    // `submissionStatus` shows the durable link to the Restore that settled.
    if (settled.phase === "succeeded" && !outcomeIsElsewhere(settled, prepared)) {
      dropDraft(key);
      const route = ((settled.result || {}).route);
      if (typeof route === "string" && route.length > 0 && typeof window !== "undefined") {
        window.location.hash = route;
        return;
      }
    }
    if (settled.phase === "pending") {
      const button = node.querySelector("#create-restore");
      if (button !== null) {
        button.disabled = true;
        button.setAttribute("aria-busy", "true");
      }
      const status = node.querySelector("#restore-submit-status");
      if (status !== null) {
        replace(status, parse(submissionStatus(settled, prepared, [])));
      }
      return;
    }
    renderAndWire(node, state, parse, api, lifecycle).then((rendered) => {
      if (rendered && settled.phase === "failed") {
        const target = node.querySelector("[aria-invalid=\"true\"]") ||
          node.querySelector("#restore-submit-status");
        if (target !== null && typeof target.focus === "function") {
          target.focus();
        }
      }
    });
  }, lifecycle);

  const submit = node.querySelector("#create-restore");
  if (submit !== null) {
    listen(submit, "click", () => {
      if (!active(lifecycle) || record.pending()) {
        return;
      }
      const reviewed = typeof (prepared || {}).hash === "string" ? prepared.hash : undefined;
      // The record turns pending BEFORE the first await inside it, so a second
      // click in the same instant finds it pending and sends nothing. The
      // attempt carries WHAT IT IS ABOUT -- the reviewed plan's hash and the
      // name minted from it -- so an outcome that arrives after the fields have
      // changed can still be told, and named, for what it is.
      record.run(async () => {
        const result = await submitRestore(state, api, lifecycle, { reviewedHash: reviewed });
        return result === null ? { outcome: "abandoned" } : result;
      }, { about: { hash: reviewed, restoreName: (prepared || {}).restoreName } });
    }, lifecycle);
  }
}

/** A form control's value. Kept here rather than inline so the submit region
 *  above stays free of every string-transforming token: what a viewer typed is
 *  read and trimmed HERE, and what is submitted is the document rendered from
 *  it, untouched. */
function valueOf(field) {
  return String(field.value).trim();
}

// DOWNLOAD-BEGIN
//
// THE DOWNLOAD IS CLIENT-SIDE AND WRITES NOTHING TO THE CLUSTER. A `Blob` over
// exactly the bytes in the `<pre>`, named after the minted restore, handed to
// the browser's own save flow. No request is issued, no object is created, and
// no identifier outside this origin is named -- `ui_lint.rs`'s
// `the_plan_download_writes_no_scheme_and_no_cluster_write` asserts all three.
// This region is also the only place in this file the plan document's file
// extension appears, which is what lets the submit-region scan forbid that token
// without forbidding the download.
function downloadPlan(prepared) {
  const blob = new Blob([prepared.bytes], { type: "text/yaml" });
  const handle = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.setAttribute("href", handle);
  anchor.setAttribute("download", prepared.restoreName + ".yaml");
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  URL.revokeObjectURL(handle);
}
// DOWNLOAD-END
