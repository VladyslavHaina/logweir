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

import { create, list } from "../api.js";
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
  prefixOf,
  preflightSentence,
  replace,
  rfc3339,
  table,
  windowMessage,
} from "../render.js";
import { defaultTopicPrefix, TARGET_MODES, mintNames, planHash, renderPlanBytes } from "../plan.js";
import { itemsOf } from "./clusters.js";

const PLURAL = "restores";
const CLUSTERS = "kafkaclusters";
const BACKUPS = "backups";

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
const API = { create: create, list: list };

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
    "<section class=\"step\" id=\"step-archive\"><h3>1. Archive</h3>" +
    "<p class=\"blurb\">The source cluster this restore reads an archive of. The archives " +
    "below were read from this namespace's Backup objects: this page holds no bucket " +
    "credential and lists no object storage.</p>" +
    table(
      ["SOURCE CLUSTER", "BOOTSTRAP", "CLUSTER ID", "ARCHIVE", "ARCHIVE CREDENTIAL"],
      rows,
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
  return (
    "<h4>The credential that reaches that archive</h4>" +
    "<label for=\"archive-secret\">ARCHIVE CREDENTIAL (Secret name)</label>" +
    "<input id=\"archive-secret\" name=\"archiveSecret\" value=\"" + esc(name) + "\">" +
    "<p class=\"note\">The runner reads the archive with this credential: weirkeeper mounts " +
    "the named Secret's keys into the runner Job as its object-store credential, and does so " +
    "only when spec.sourceArchive.secretRef is set. A Restore created without it is " +
    "ADMITTED and then fails at the archive, not at admission -- the field is optional in " +
    "the CRD, so nothing refuses it until the Job cannot read an object. Leave it blank only " +
    "for an archive reached anonymously or by an instance role. This page shows and sends " +
    "the NAME; it never reads the Secret.</p>"
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
    "<label for=\"store-endpoint\">endpoint</label>" +
    "<input id=\"store-endpoint\" name=\"endpoint\" value=\"" + esc(store.endpoint) + "\">" +
    "<label for=\"store-region\">region</label>" +
    "<input id=\"store-region\" name=\"region\" value=\"" + esc(store.region) + "\">" +
    "<label for=\"store-pathStyle\">path_style addressing</label>" +
    "<input type=\"checkbox\" id=\"store-pathStyle\" name=\"pathStyle\"" +
    (store.pathStyle === true ? " checked" : "") + ">" +
    "<label for=\"evidence-bucket\">evidence bucket</label>" +
    "<input id=\"evidence-bucket\" name=\"evidenceBucket\" value=\"" +
    esc(((s.fields || {}).evidence || {}).bucket) + "\">" +
    "<p class=\"note\">The evidence prefix is fixed at " + esc(EVIDENCE_PREFIX) + " by Global " +
    "Constraint 6 and is not an input: a plan naming another one is refused at phase 0, " +
    "after the approver has already signed it.</p>"
  );
}

/** Step 2 -- the backup set. Every `Backup` whose archive is the selected one,
 *  with its covered range CONVERTED TO RFC 3339 (interface I22: the field is
 *  two integers, and a viewer reading `1757253900000` learns nothing). */
export function renderBackupSetStep(state) {
  const s = state || {};
  const rows = backupsOf(s).map((backup) => {
    const status = backup.status || {};
    const covered = status.windowCovered || {};
    return [
      cell(status.backupId),
      cell(rfc3339(covered.fromMs)),
      cell(rfc3339(covered.toMs)),
      cell(status.records),
      cell((backup.metadata || {}).name),
    ];
  });
  return (
    "<section class=\"step\" id=\"step-backup-set\"><h3>2. Backup set</h3>" +
    "<p class=\"blurb\">The sets this archive holds, newest last. The covered range is " +
    "Backup.status.windowCovered, two epoch-millisecond integers, shown as RFC 3339.</p>" +
    table(["BACKUP SET", "COVERED FROM", "COVERED TO", "RECORDS", "BACKUP"], rows) +
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
  return (
    "<section class=\"step\" id=\"step-point-in-time\"><h3>3. Point in time</h3>" +
    "<p class=\"blurb\">An RFC 3339 instant. The window is closed at both ends: a record " +
    "whose timestamp equals this exactly is restored. Its FLOOR is the archive's own " +
    "earliest covered timestamp, read from the manifest by the runner, and is never a " +
    "field of this plan.</p>" +
    "<label for=\"point-in-time\">point in time</label>" +
    "<input id=\"point-in-time\" name=\"pointInTime\" value=\"" + esc(value) + "\">" +
    "<p class=\"window\">" + windowMessage(covered.fromMs, covered.toMs) + "</p>" +
    (complaint === null ? "" : "<p class=\"complaint\">" + complaint + "</p>") +
    "<p class=\"note\">" + CONVENIENCE_SENTENCE + "</p>" +
    "</section>"
  );
}

/** Step 4 -- target and naming. A `KafkaCluster` with `role: target`, the two
 *  modes `TargetMode` accepts and nothing else, and the prefix PREFILLED with
 *  `default_topic_prefix`'s own output for the chosen instant. */
export function renderTargetStep(state) {
  const s = state || {};
  const fields = s.fields || {};
  const target = fields.target || {};
  const targets = itemsOf(s.clusters).filter((c) => ((c.spec || {}).role) === "target");
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
  const clusterOptions = targets
    .map((c) => {
      const name = (c.metadata || {}).name;
      return "<option value=\"" + esc(name) + "\">" + esc(name) + "</option>";
    })
    .join("");
  return (
    "<section class=\"step\" id=\"step-target\"><h3>4. Target and naming</h3>" +
    "<p class=\"blurb\">Where the restored records are written. Nothing that already " +
    "exists is written to: a Restore only ever creates topics that did not exist, and " +
    "refuses outright if a mapped target topic is already there.</p>" +
    "<label for=\"target-cluster\">target cluster</label>" +
    "<select id=\"target-cluster\" name=\"targetCluster\">" + clusterOptions + "</select>" +
    "<label for=\"target-mode\">mode</label>" +
    "<select id=\"target-mode\" name=\"mode\">" + options + "</select>" +
    "<label for=\"topic-prefix\">topicNaming.prefix</label>" +
    "<input id=\"topic-prefix\" name=\"topicPrefix\" value=\"" + esc(prefix) + "\">" +
    "<p class=\"note\">The prefix defaults to what logweir_core::spec::default_topic_prefix " +
    "produces for this instant, so a topic name says both what it is and what point it was " +
    "recovered to. It is editable.</p>" +
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
    "<section class=\"step\" id=\"step-preflight\"><h3>5. Target-topic preflight</h3>" +
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

/** Step 6 -- the rendered plan, its hash, and the two minted names. */
export function renderPlanStep(prepared, state) {
  const p = prepared || {};
  const s = state || {};
  return (
    "<section class=\"step\" id=\"step-plan\"><h3>6. Plan, hash and names</h3>" +
    "<pre class=\"plan-bytes\" id=\"plan-bytes\">" + esc(p.bytes) + "</pre>" +
    facts([
      ["plan hash", "<code>" + esc(p.hash) + "</code>"],
      ["Restore metadata.name", "<code>" + esc(p.restoreName) + "</code>"],
      ["Approval metadata.name", "<code>" + esc(p.approvalName) + "</code>"],
    ]) +
    "<p class=\"note\">Both names are minted from the plan bytes before either object " +
    "exists. The Restore is created first, naming an Approval that is not there yet; the " +
    "reconciler requeues every 30 s until it arrives. Neither name is ever edited, because " +
    "neither spec can be.</p>" +
    "<button type=\"button\" id=\"copy-plan\">Copy plan</button>" +
    "<button type=\"button\" id=\"download-plan\">Download plan</button>" +
    "<p class=\"caveat\">" + esc(COPY_CAVEAT) + "</p>" +
    "<h4>Approve it out of band</h4>" +
    "<p class=\"note\">Run this on the machine that holds the approver's private key. This " +
    "page never sees it.</p>" +
    copyBlock([APPROVE_COMMAND]) +
    "<button type=\"button\" id=\"create-restore\">Create the Restore</button>" +
    "<button type=\"button\" id=\"request-approval\">Request approval</button>" +
    "</section>"
  );
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

/** The whole wizard, all six steps, over one state. */
export async function renderRestoreWizard(state) {
  const prepared = await preparePlan(state);
  return (
    "<h2>Restore wizard</h2>" +
    ((state || {}).editing
      ? "<p class=\"immutable-note\">" + RESTORE_IMMUTABLE_SENTENCE + "</p>"
      : "") +
    renderArchiveStep(state) +
    renderBackupSetStep(state) +
    renderPointInTimeStep(state) +
    renderTargetStep(state) +
    renderPreflightStep(state) +
    renderPlanStep(prepared, state)
  );
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
  const ns = typeof s.ns === "string" && s.ns.length > 0 ? s.ns : "default";
  return (
    "#/approvals?subject=" +
    encodeURIComponent(p.restoreName) +
    "&hash=" +
    encodeURIComponent(p.hash) +
    "&name=" +
    encodeURIComponent(p.approvalName) +
    (ns === "default" ? "" : "&ns=" + encodeURIComponent(ns))
  );
}

/** Creates the `Restore` -- the FIRST of the two creates, with a dangling
 *  `approvalRef` -- and returns the approvals route the next action navigates
 *  to. Both names are minted before this function issues anything. */
export async function submitRestore(state, deps) {
  const api = deps || API;
  const prepared = await preparePlan(state);
  const body = restoreBody(state, prepared);
  await api.create((state || {}).ns, PLURAL, body);
  return approvalRoute(state, prepared);
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

function chosenBackup(state) {
  const wanted = (state.fields || {}).backupSetRef;
  const candidates = backupsOf(state);
  for (const backup of candidates) {
    if (((backup.status || {}).backupId) === wanted) {
      return backup;
    }
  }
  return candidates.length > 0 ? candidates[candidates.length - 1] : null;
}

function targetCluster(state) {
  for (const cluster of itemsOf(state.clusters)) {
    if ((cluster.metadata || {}).name === state.targetClusterName) {
      return cluster;
    }
  }
  for (const cluster of itemsOf(state.clusters)) {
    if (((cluster.spec || {}).role) === "target") {
      return cluster;
    }
  }
  return null;
}

/** The default prefix for an instant, or the empty string when there is no
 *  instant to derive one from. */
function prefixFor(pointInTime) {
  if (typeof pointInTime !== "string" || pointInTime.length === 0) {
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

export async function mountRestoreWizard(node, ns, parse, deps) {
  const api = deps || API;
  try {
    const clusters = await api.list(ns, CLUSTERS);
    const backups = await api.list(ns, BACKUPS);
    const state = initialState(ns, clusters, backups);
    replace(node, parse(await renderRestoreWizard(state)));
    wire(node, state, parse, api);
  } catch (error) {
    replace(node, errorBox(error));
  }
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
  const objects = itemsOf(backups);
  const newest = objects.length > 0 ? objects[objects.length - 1] : null;
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

function bootstrapOf(state) {
  const cluster = targetCluster(state);
  return ((cluster || {}).spec || {}).bootstrapServers || [];
}

function firstTarget(clusters) {
  for (const cluster of itemsOf(clusters)) {
    if (((cluster.spec || {}).role) === "target") {
      return cluster;
    }
  }
  return null;
}

function wire(node, state, parse, api) {
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
    if (point !== null) {
      state.fields.pointInTime = valueOf(point);
    }
    if (mode !== null) {
      state.fields.target.mode = valueOf(mode);
    }
    if (prefix !== null) {
      state.fields.target.topicPrefix = valueOf(prefix);
    }
    if (cluster !== null) {
      state.targetClusterName = valueOf(cluster);
      state.fields.target.bootstrapServers = bootstrapOf(state);
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
    replace(node, parse(await renderRestoreWizard(state)));
    wire(node, state, parse, api);
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
      field.addEventListener("change", refresh);
    }
  }

  const copy = node.querySelector("#copy-plan");
  if (copy !== null) {
    copy.addEventListener("click", async () => {
      const pre = node.querySelector("#plan-bytes");
      if (pre !== null && navigator.clipboard) {
        await navigator.clipboard.writeText(pre.textContent);
      }
    });
  }

  const download = node.querySelector("#download-plan");
  if (download !== null) {
    download.addEventListener("click", async () => {
      const prepared = await preparePlan(state);
      downloadPlan(prepared);
    });
  }

  const submit = node.querySelector("#create-restore");
  if (submit !== null) {
    submit.addEventListener("click", async () => {
      try {
        await submitRestore(state, api);
      } catch (error) {
        replace(node, errorBox(error));
      }
    });
  }

  const request = node.querySelector("#request-approval");
  if (request !== null) {
    request.addEventListener("click", async () => {
      const prepared = await preparePlan(state);
      window.location.hash = approvalRoute(state, prepared);
    });
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
