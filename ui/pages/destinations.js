// pages/destinations.js -- saved destinations: the list, the create form, the
// access rotation, the access test and what uses one.
//
// A destination is an archive location written down ONCE, with an identity,
// so schedules and restores reference it by name instead of each spelling its
// own URL, endpoint, region and Secret (and disagreeing silently).
//
// FOUR RULES THIS PAGE KEEPS.
//
// 1. THE CREDENTIAL IS NEVER IN A DRAFT. `ui/lifecycle.js`'s `keepDraft` takes
//    an ALLOWLIST of field names, and [`DESTINATION_DRAFT_FIELDS`] names no
//    credential field, so a typed credential survives exactly as long as the
//    form element does: not a refusal, a route change or a re-render after a
//    409. An allowlist, because with a denylist the default for the NEXT
//    credential field somebody adds would be "kept". The twelve credential
//    field names are DERIVED from the four roles, so a fifth role cannot add
//    three the disjointness assertion does not cover.
//    `ui/tests/credentials.spec.js` plants a password in a draft and fails.
//
// 2. ADDRESSING IS NOT TRANSPORT. `addressing` says how the bucket appears in
//    the URL; `transport.security` alone says whether the connection is
//    encrypted. They are two independent controls in two fieldsets and
//    neither derives the other: deriving `allowHttp` from a pathStyle
//    checkbox once turned TLS off for anyone who ticked it.
//
// 3. ABSENT IS NOT FALSE. `status.valid` is OPTIONAL on the wire and `null`
//    means the controller has not reached a verdict (a controller older than
//    the kind never writes one). Rendering that as "invalid" would report a
//    missing controller as a broken destination, and "valid" would be worse.
//
// 4. A TEST'S VERDICT COMES FROM THE CHECK, NOT FROM THIS PAGE. "Test access"
//    creates a `Preflight` and renders that object's own recorded state: it
//    dials nothing, never infers a pass from the absence of a failure, and an
//    unreconciled `Preflight` reads `pending`. The create answer is the check
//    before it ran, projected without recomputing staleness, so the page
//    follows the started check with bounded GETs until a read answers
//    terminal.

import { apiClient, mayOperate } from "../client.js";
import {
  active,
  cancelled,
  dropDraft,
  fieldErrors,
  formKey,
  invalidInput,
  keepDraft,
  listen,
  followCheck,
  mutationFor,
  PREFLIGHT_FOLLOW_MS,
  readDraft,
  readOptions,
  watchMutation,
  wireCheckRetry,
} from "../lifecycle.js";
import {
  ABSENT,
  DESTINATION_TEST_SENTENCE,
  NOT_JUDGED_SENTENCE,
  applicabilityLine,
  badge,
  cell,
  checkStoppedBlock,
  checkTable,
  destinationVerdict,
  detailLink,
  errorBox,
  esc,
  executionOnlyBlock,
  facts,
  fieldErrorLine,
  invalidAttributes,
  messageText,
  mutationStatus,
  preflightVerdict,
  readinessHeadline,
  replace,
  replaceInPlace,
  table,
  when,
} from "../render.js";
import {
  connectionNonce,
  focusFirstProblem,
  isDataKey,
  isObjectName,
  readFormValues,
} from "./clusters.js";

const API = apiClient();

// THE TWO SCHEMES, BUILT AND NEVER SPELLED (the rule `ui/api.js`'s
// `SCHEME_SEPARATOR` follows and `scripts/check-ui-offline.sh` enforces over
// every line of this tree): grepping `ui/` for `http` and a colon must come
// back empty. These are pieces of prose and of a validator, not addresses
// this page fetches, but the gate reads lines and is right not to guess.
const HTTPS = "https" + ":" + "//";
const HTTP = "http" + ":" + "//";

const CREATE_FORM = "destination-create";
const ROTATE_FORM = "destination-rotate";
const TEST_FORM = "destination-test";

/** The four grants a destination carries, in the order the form shows them and
 *  the order the detail table lists them. `archiveWrite` is first because it
 *  is the only REQUIRED one: the other three have a defined meaning when they
 *  are absent, and the form says what that meaning is beside each. */
export const GRANT_ROLES = Object.freeze([
  "archiveWrite", "archiveRead", "evidenceWrite", "evidenceRead",
]);

/** What an ABSENT grant means, by role. These are not this page's opinions:
 *  they are `AccessRequest`'s own documented absences, and the response spells
 *  each of them back as a mode (`inheritsArchiveWrite`, `notConfigured`). */
export const ABSENT_GRANT_MEANING = Object.freeze({
  archiveWrite: "required; there is no absent case",
  archiveRead: "absent means reads use the archiveWrite credential",
  evidenceWrite: "absent means evidence is written with the archiveWrite credential",
  evidenceRead: "absent means verification is NOT attempted, and says so rather than leaving a blank",
});

/** The grant sources the form offers, by the value its selector carries.
 *  `absent` is a real answer and the default for the three optional roles. */
export const GRANT_SOURCES = Object.freeze([
  "absent", "existing", "new", "workloadIdentity", "controllerIdentity", "archiveReadGrant",
]);

/** The sources the CREATE form offers -- every one but `existing`. A new
 *  destination never names an existing Secret: none can carry the binding of
 *  a destination that does not exist yet, and the product API refuses one
 *  (`existing_credential_refused`). The credential is entered once (`new`)
 *  and becomes a Secret owned by and bound to the destination. The ROTATION
 *  form still offers `existing`, for a Secret this destination already
 *  names. */
export const CREATE_GRANT_SOURCES = Object.freeze(
  GRANT_SOURCES.filter((s) => s !== "existing"),
);

/** What `existing` means on the rotation form. */
export const EXISTING_ON_ROTATION =
  "existing: only a Secret this destination already names. To change a credential, enter the " +
  "new value: it is written to a NEW Secret bound to this destination, and the old one is left " +
  "for you to delete.";

/** The sources the ROTATION form offers: `keep` first, and the default for
 *  every role. A rotation sends the complete access block and the API
 *  removes a grant it omits, so a form that started a grant at `absent`
 *  would remove every grant the reader did not touch. `keep` sends the grant
 *  the destination has now, as [`keptGrant`] spells it. */
export const ROTATE_GRANT_SOURCES = Object.freeze(["keep"].concat(GRANT_SOURCES));

/** What choosing `absent` on the rotation form turns off, by role, said beside
 *  the control while it is chosen for a grant the destination has. The same
 *  absences as [`ABSENT_GRANT_MEANING`], as what changes. */
export const ABSENT_REMOVES = Object.freeze({
  archiveRead: "absent REMOVES this destination's archiveRead grant: restores then read the " +
    "archive with the archiveWrite credential.",
  evidenceWrite: "absent REMOVES this destination's evidenceWrite grant: evidence is then " +
    "written with the archiveWrite credential.",
  evidenceRead: "absent REMOVES this destination's evidenceRead grant: evidence verification " +
    "stops, and is recorded as NotAttempted, until a grant is set again.",
});

/** The response-only modes that say a grant is absent. */
const ABSENT_MODES = Object.freeze(["inheritsArchiveWrite", "notConfigured"]);

/** Whether the destination holds `view` as a grant of its own. */
export function holdsGrant(view) {
  const mode = (view || {}).mode;
  return typeof mode === "string" && ABSENT_MODES.indexOf(mode) === -1;
}

/** The data keys a `secret.existing` without key names stands for. */
const DEFAULT_SECRET_KEYS = Object.freeze(["access-key-id", "secret-access-key"]);

/** THE GRANT A DESTINATION HAS NOW, AS A REQUEST: what `keep` sends.
 *
 *  `null` for an absent grant (kept absent). `undefined` when this page
 *  cannot spell it: a Secret whose two credential keys are not the API's
 *  defaults is published as a sorted list that does not say which key holds
 *  the key id, and guessing would point the grant at the wrong value. Beside
 *  the two defaults, the one key left is `sessionTokenKey`. Naming a Secret
 *  the destination already names is what `:update-access` accepts for
 *  "keeping a grant as it is". */
export function keptGrant(view) {
  const g = view || {};
  if (!holdsGrant(g)) {
    return g.mode === undefined ? undefined : null;
  }
  if (g.mode === "controllerIdentity" || g.mode === "archiveReadGrant") {
    return { mode: g.mode };
  }
  if (g.mode === "workloadIdentity") {
    return typeof g.serviceAccountName === "string" && g.serviceAccountName.length > 0
      ? { mode: "workloadIdentity", workloadIdentity: { serviceAccountName: g.serviceAccountName } }
      : { mode: "workloadIdentity" };
  }
  if (g.mode !== "secretKeys" || typeof g.secretName !== "string" || g.secretName.length === 0) {
    return undefined;
  }
  const keys = Array.isArray(g.keys) ? g.keys : [];
  const others = keys.filter((k) => DEFAULT_SECRET_KEYS.indexOf(k) === -1);
  if (others.length > 1 || DEFAULT_SECRET_KEYS.some((k) => keys.indexOf(k) === -1)) {
    return undefined;
  }
  const existing = { name: g.secretName };
  if (others.length === 1) {
    existing.sessionTokenKey = others[0];
  }
  return { mode: "secretKeys", secret: { existing: existing } };
}

/** The `keep` option's words: what the grant is now. */
function keepLabel(view) {
  const g = view || {};
  const what = [g.mode, g.secretName, g.serviceAccountName]
    .filter((x) => typeof x === "string" && x.length > 0).join(" ");
  return "keep (as it is" + (what.length > 0 ? ": " + what : "") + ")";
}

/** The two `evidenceRead`-only modes, which the form offers for that role and
 *  refuses for the other three -- as the product API does. */
export const EVIDENCE_READ_ONLY_SOURCES = Object.freeze([
  "controllerIdentity", "archiveReadGrant",
]);

/** What the form starts from. `addressing` has NO default on the wire, so the
 *  form picks the one that works against every S3-compatible endpoint and
 *  discloses it; `security` defaults to TLS, which is the only default a
 *  transport control may have. */
export const DESTINATION_DEFAULTS = Object.freeze({
  name: "", description: "", bucket: "", prefix: "", region: "", endpoint: "",
  addressing: "pathStyle", security: "tls", caName: "", caKey: "",
  writeProbe: "createOnlyMarker", isDefault: false,
  archiveWriteSource: "new", archiveWriteSecret: "", archiveWriteServiceAccount: "",
  archiveReadSource: "absent", archiveReadSecret: "", archiveReadServiceAccount: "",
  evidenceWriteSource: "absent", evidenceWriteSecret: "", evidenceWriteServiceAccount: "",
  evidenceReadSource: "absent", evidenceReadSecret: "", evidenceReadServiceAccount: "",
});

/** What the ROTATION form starts from: every grant kept as it is. */
export const ROTATE_DEFAULTS = Object.freeze(Object.assign({}, DESTINATION_DEFAULTS, {
  archiveWriteSource: "keep", archiveReadSource: "keep",
  evidenceWriteSource: "keep", evidenceReadSource: "keep",
}));

/** THE DRAFT ALLOWLIST (rule 1 at the top of this file). Every field here is
 *  a name, a flag or an enum member; every name in [`CREDENTIAL_INPUTS`] is
 *  deliberately absent, so `keepDraft` cannot keep one even if a caller hands
 *  it the whole form. */
export const DESTINATION_DRAFT_FIELDS = Object.freeze([
  "name", "description", "bucket", "prefix", "region", "endpoint",
  "addressing", "security", "caName", "caKey", "writeProbe", "isDefault",
  "archiveWriteSource", "archiveWriteSecret", "archiveWriteServiceAccount",
  "archiveReadSource", "archiveReadSecret", "archiveReadServiceAccount",
  "evidenceWriteSource", "evidenceWriteSecret", "evidenceWriteServiceAccount",
  "evidenceReadSource", "evidenceReadSecret", "evidenceReadServiceAccount",
]);

/** EVERY FIELD NAME THAT CARRIES A CREDENTIAL VALUE, derived from the four
 *  roles so a fifth role cannot add three names this list does not have.
 *
 *  Exported so the suite can assert mechanically that NONE of them is in the
 *  allowlist. A field forgotten here is still not kept (the allowlist is the
 *  gate); this list is what makes the forgetting VISIBLE. `<role>SessionToken`
 *  is in it although no input renders one yet, because `grantBody` already
 *  reads that field into `secret.new`. */
export const CREDENTIAL_INPUTS = Object.freeze(GRANT_ROLES.reduce(
  (all, role) => all.concat([
    role + "AccessKeyId", role + "SecretAccessKey", role + "SessionToken",
  ]),
  [],
));

/** The sentence beside the credential inputs. */
export const WRITE_ONLY_SENTENCE =
  "Typed once and written once. The value goes into a Secret this namespace owns and is never " +
  "read back: no response, no status, no log line and no audit record carries it, and this page " +
  "does not keep it either -- a refusal, a reload or a visit to another view loses it, and that " +
  "is on purpose. What comes back is the Secret's NAME.";

/** What the default-destination control means, and why a second one is a
 *  conflict rather than a takeover. */
export const DEFAULT_DESTINATION_SENTENCE =
  "At most one destination per namespace is the default, and new schedules start from it. " +
  "Naming a second one is refused with a 409 rather than silently moving the flag: a namespace " +
  "with two defaults has none. Clear it on the destination that holds it first.";

/** The residual the product API documents, restated here because it is the one
 *  case the 409 above cannot see. */
export const DEFAULT_ANNOTATION_RESIDUAL =
  "A default set by hand with only the `logweir.dev/default-destination` annotation carries no " +
  "label, so the conflict check cannot see it -- it still reads as the default everywhere else.";

/** What a write probe does, disclosed because the API's default and the CRD's
 *  default differ on purpose. */
export const WRITE_PROBE_SENTENCE =
  "createOnlyMarker lets an access test create ONE marker object under logweir/readiness/. It is " +
  "never deleted and nothing else is ever written by a test. A destination created with kubectl " +
  "and no opinion gets `disabled` instead: a form that discloses the choice may default to the " +
  "useful answer, and an object created without one may not.";

/** The destinations table. TRANSPORT AND ADDRESSING ARE TWO COLUMNS, not one
 *  "mode" column, for the same reason they are two controls on the form: a
 *  reader who sees them merged learns that one implies the other. */
export function renderDestinationList(page, ns) {
  const items = Array.isArray((page || {}).items) ? page.items : [];
  const defaults = items.filter((d) => d.default === true);
  const rows = items.map((d) => [
    detailLink("destinations", ns, d.name),
    d.default === true ? badge("green", "default") : ABSENT,
    "<code>" + cell(d.canonicalUrl) + "</code>",
    transportCell(d.transport),
    cell(d.addressing),
    destinationVerdict(d.status),
    cell(d.generation),
  ]);
  return (
    "<h2>Destinations</h2>" +
    "<p class=\"blurb\">A saved destination is one archive location, written down once, with " +
    "credentials referenced by name. Schedules, backups and restores name it instead of each " +
    "spelling a URL, an endpoint and a Secret of their own.</p>" +
    table(
      ["NAME", "DEFAULT", "LOCATION", "TRANSPORT", "ADDRESSING", "VERDICT", "GEN"],
      rows,
      "No destination in this namespace yet. Create one below, or adopt the location an " +
        "existing schedule already writes to.",
      undefined,
      { id: "destinations", label: "destinations" },
    ) +
    (defaults.length === 0
      ? "<p class=\"note\">No destination in this namespace is marked default, so a new " +
        "schedule starts with none chosen. " + messageText(DEFAULT_ANNOTATION_RESIDUAL) + "</p>"
      : "") +
    (items.some((d) => d.status === null || d.status === undefined ||
      d.status.valid === null || d.status.valid === undefined)
      ? "<p class=\"note\" id=\"destinations-unjudged\">" + esc(NOT_JUDGED_SENTENCE) + "</p>"
      : "")
  );
}

/** A transport cell. `insecureHttp` is a warning BADGE saying what it is,
 *  plaintext over the network: a legal, explicitly configured choice for a
 *  lab endpoint, which the page does not refuse but will not let look like
 *  TLS. */
export function transportCell(security) {
  if (security === "insecureHttp") {
    // The warning style: a neutral grey pill read as "fine".
    return badge("warn", "insecureHttp (plaintext)");
  }
  if (security === "tls") {
    return badge("green", "tls");
  }
  return ABSENT;
}

/** The four grants, as a table of REFERENCES. There is no column for a value,
 *  because the response carries none: a grant is a mode, a Secret name and the
 *  KEY NAMES inside it, all three of which are public. */
export function renderAccessTable(access) {
  const a = access || {};
  const rows = GRANT_ROLES.map((role) => {
    const grant = a[role] || {};
    return [
      "<code>" + esc(role) + "</code>",
      cell(grant.mode),
      cell(grant.secretName) + (grant.serviceAccountName ? " " + cell(grant.serviceAccountName) : ""),
      Array.isArray(grant.keys) && grant.keys.length > 0 ? esc(grant.keys.join(", ")) : ABSENT,
      esc(ABSENT_GRANT_MEANING[role]),
    ];
  });
  return table(
    ["ROLE", "MODE", "REFERENCE", "KEY NAMES", "WHAT ABSENT MEANS"],
    rows,
    "no grant recorded",
  );
}

/** One destination, in full. */
export function renderDestinationDetail(item, view) {
  const d = item || {};
  const v = view || {};
  const storage = d.storage || {};
  const transport = d.transport || {};
  const ca = transport.caBundle || {};
  const unjudged = d.status === null || d.status === undefined ||
    d.status.valid === null || d.status.valid === undefined;
  return (
    "<h2>Destination " + cell(d.name) + "</h2>" +
    destinationVerdict(d.status) +
    (d.default === true ? " " + badge("green", "namespace default") : "") +
    (unjudged ? "<p class=\"note\" id=\"destination-unjudged\">" + esc(NOT_JUDGED_SENTENCE) +
      "</p>" : "") +
    facts([
      ["description", cell(d.description)],
      ["archive root", "<code>" + cell(d.canonicalUrl) + "</code>"],
      ["provider", cell(storage.provider)],
      ["bucket", cell(storage.bucket)],
      ["prefix", typeof storage.prefix === "string" && storage.prefix.length === 0
        ? "(the bucket root)" : cell(storage.prefix)],
      ["region", cell(storage.region)],
      ["endpoint", cell(storage.endpoint) === ABSENT ? "- (AWS S3)" : cell(storage.endpoint)],
      ["addressing", cell(storage.addressing)],
      ["transport", transportCell(transport.security)],
      ["private CA", ca.configMapName
        ? cell(ca.configMapName) + " / " + cell(ca.key) + " " + cell(ca.sha256)
        : "- (the runner image's own trust store)"],
      ["write probe", cell(d.writeProbe)],
      ["location digest", cell(d.locationDigest)],
      ["generation", cell(d.generation)],
      ["uid", "<code id=\"destination-uid\">" + cell(d.uid) + "</code>"],
      ["controller observed generation", cell((d.status || {}).observedGeneration)],
      ["controller message", cell((d.status || {}).message)],
    ]) +
    "<section class=\"access\"><h3>Access</h3>" + renderAccessTable(d.access) + "</section>" +
    // The summary stands down once this page holds a test of its own: it is
    // what the detail read found BEFORE the test started, and "No access test
    // has been recorded" above the test just started would contradict it.
    "<div class=\"test-slot\" id=\"destination-test-slot\">" + renderTestSlot(d, v) + "</div>" +
    renderUsage(v.usage, v.usageError) +
    renderRotateForm(d, v)
  );
}

/** The test half of the detail: the recorded summary (until this page holds a
 *  test of its own) and the Test access panel. Its own slot, so a followed
 *  test repaints this alone and the rotation form below -- whose credential
 *  inputs no draft may keep -- is never re-rendered under a reader typing
 *  into it. */
export function renderTestSlot(item, view) {
  const d = item || {};
  const v = view || {};
  return (v.test ? "" : renderLastTest(d.lastTest)) + renderTestPanel(d, v);
}

/** The recorded `lastTest`, which is a POINTER and not a verdict of its own.
 *  `truncated` is rendered: the search for the newest test hit its page
 *  bound, so this may not be the newest, and the reader is told so rather
 *  than reassured. */
export function renderLastTest(lastTest) {
  if (lastTest === null || lastTest === undefined) {
    return "<p class=\"note\" id=\"destination-no-test\">No access test has been recorded for " +
      "this destination. Nothing below claims it works.</p>";
  }
  return (
    "<div class=\"last-test\" id=\"destination-last-test\">" +
    preflightVerdict(lastTest.state) +
    (lastTest.stale === true ? " " + badge("unverified", "stale: not health") : "") +
    "<p class=\"note\">Recorded by <code>" + cell(lastTest.preflightId) + "</code>, observed " +
    when(lastTest.observedAt) + ".</p>" +
    (lastTest.truncated === true
      ? "<p class=\"note\">The search for the newest test hit its page bound, so this may not " +
        "be the newest one.</p>"
      : "") +
    "</div>"
  );
}

/** The "Test access" control and whatever the started `Preflight` says. The
 *  verdict is the object's: `view.test` is rendered field for field, and an
 *  unreconciled check is `pending`, which is what appears. */
export function renderTestPanel(item, view) {
  const v = view || {};
  const test = v.test || null;
  const may = v.mayOperate !== false;
  // The status is the form's own, as every check form's is: a click disables
  // the fieldset and focus moves to the status of the form it was in
  // (`render.js`'s `restoreFocus`); a status drawn after `</form>` is not found
  // and focus would fall to the body.
  const status = "<div class=\"form-status\" id=\"destination-test-status\" tabindex=\"-1\">" +
    mutationStatus(v.testState || {}, { kind: "Preflight", name: (test || {}).id || "" }, null) +
    "</div>";
  return (
    "<section class=\"destination-test\" id=\"destination-test\"><h3>Test access</h3>" +
    "<p class=\"note\">" + esc(DESTINATION_TEST_SENTENCE) + "</p>" +
    (may
      ? "<form id=\"destination-test-form\" novalidate>" +
        "<fieldset class=\"form-body\"" + (v.testPending ? " disabled" : "") + ">" +
        "<div class=\"field\"><label for=\"test-roles\">roles to exercise</label>" +
        "<select id=\"test-roles\" name=\"roles\" multiple size=\"4\">" +
        GRANT_ROLES.map((r) => "<option value=\"" + esc(r) + "\"" +
          ((Array.isArray(v.testRoles) ? v.testRoles : []).indexOf(r) !== -1 ? " selected" : "") +
          ">" + esc(r) + "</option>").join("") +
        "</select>" +
        "<p class=\"help\">Choose none to exercise every configured role.</p></div>" +
        "<div class=\"actions\"><button type=\"submit\">Test access</button></div>" +
        "</fieldset>" + status + "</form>"
      : "<p class=\"note\">This login may read destinations and not test them; the roles it " +
        "holds here do not include operator or administrator.</p>" + status) +
    (test === null ? "" : renderPreflight(test)) +
    "</section>"
  );
}

// The attempt token, as the connection check mints it: this load's nonce and
// a per-click ordinal, so a DELIBERATE second test is a new `Preflight`
// rather than a replay of the first one's verdict.
let testAttempts = 0;

/** The token one accepted Test access click carries. */
export function nextDestinationTestAttempt(ns, name) {
  const mint = connectionNonce();
  testAttempts += 1;
  return String(ns) + "." + String(name) + ".test." + mint + "-" + String(testAttempts);
}

// The started test, remembered per destination for the life of the loaded
// page (the connection panel's `checkViews`, for the same reason): every
// other control repaints the whole detail, and without this the first such
// repaint would wipe the result just asked for. It also names the ONE test
// being followed, so an older follower stops painting once a newer test
// replaces it.
const testViews = new Map();

/** A `Preflight`, rendered from its own recorded fields and from nothing else.
 *  Shared by the destination test, the schedules readiness panel and the
 *  restore wizard's step 5, so a readiness verdict reads the same in all
 *  three. */
export function renderPreflight(preflight) {
  const p = preflight || {};
  const binding = p.binding || {};
  return (
    "<div class=\"preflight-result\" id=\"preflight-" + esc(String(p.id || "")) + "\">" +
    "<p class=\"preflight-head\">" + readinessHeadline(p) +
    " <code>" + cell(p.id) + "</code> " + cell(p.operation) + "</p>" +
    applicabilityLine(p) +
    checkStoppedBlock(p) +
    facts([
      ["observed at", when(p.observedAt)],
      ["expires at", when(p.expiresAt)],
      ["reason", cell(p.reason)],
      ["plan hash", cell(binding.planHash)],
      ["inputs digest", cell(binding.inputsDigest)],
      ["referents", Array.isArray(binding.referents) && binding.referents.length > 0
        ? esc(binding.referents.map((r) => r.kind + "/" + r.name).join(", "))
        : ABSENT],
      ["details document", p.detailsAvailable === true ? "available" : "none recorded"],
    ]) +
    "<h4>Blocking checks</h4>" +
    checkTable(p.checks, "No blocking check has recorded a verdict. That is not a pass: it is " +
      "an empty result, and the aggregate above says what this check amounts to.") +
    (Array.isArray(p.warnings) && p.warnings.length > 0
      ? "<h4>Advisory</h4>" + checkTable(p.warnings, "none")
      : "") +
    executionOnlyBlock(p.executionOnly) +
    "</div>"
  );
}

/** What names this destination, with the BASIS on which the lists were built.
 *
 *  The basis is rendered beside an empty list, not instead of it: the product
 *  API matches `spec.destinationRef` over a bounded read and says how much it
 *  read, and an unexplained empty list would read as "nothing uses this" --
 *  the sentence an operator deletes on. */
export function renderUsage(usage, error) {
  if (error !== null && error !== undefined) {
    return "<section class=\"usage\" id=\"destination-usage\"><h3>What uses this</h3>" +
      "<p class=\"note\">The usage list could not be read: " + cell(error.message) + "</p></section>";
  }
  if (usage === null || usage === undefined) {
    return "";
  }
  const rows = []
    .concat((usage.schedules || []).map((u) => [cell(u.kind), cell(u.name), when(u.createdAt)]))
    .concat((usage.backups || []).map((u) => [cell(u.kind), cell(u.name), when(u.createdAt)]));
  return (
    "<section class=\"usage\" id=\"destination-usage\"><h3>What uses this</h3>" +
    table(["KIND", "NAME", "CREATED"], rows,
      "No schedule or Backup the product API read names it.") +
    (usage.truncated === true
      ? "<p class=\"note\">One of the lists was cut at its bound; there may be more.</p>"
      : "") +
    "<p class=\"note\" id=\"usage-basis\">" + cell(usage.basis) + "</p>" +
    "</section>"
  );
}

/** The page's own checks over the create form's values. A convenience: the
 *  product API's `destination_invalid` and the CRD's CEL rules are the gate,
 *  and every message names the rule rather than restating the value. */
export function validateDestination(values) {
  const v = values || {};
  const problems = Object.create(null);
  if (!isObjectName(String(v.name || ""))) {
    problems.name = "a destination name is a Kubernetes object name: lowercase letters, digits, " +
      "'-' and '.', starting and ending with a letter or digit";
  }
  const bucket = String(v.bucket || "");
  if (!/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(bucket)) {
    problems.bucket = "an S3 bucket is 3 to 63 characters of lowercase letters, digits, '.' and " +
      "'-', starting and ending with a letter or digit";
  }
  const prefix = String(v.prefix || "").trim();
  if (prefix.length > 0) {
    if (prefix.charAt(0) === "/" || prefix.indexOf("..") !== -1) {
      problems.prefix = "a prefix is relative and contains no parent-directory hop";
    } else if (prefix === "logweir" || prefix.indexOf("logweir/") === 0) {
      problems.prefix = "`logweir` and everything under it is reserved for evidence; a " +
        "destination whose archive prefix was there would write segments into the evidence tree";
    }
  }
  const region = String(v.region || "").trim();
  if (region.length > 0 && !/^[a-z0-9-]{1,32}$/.test(region)) {
    problems.region = "a region is lowercase letters, digits and '-'";
  }
  // The one rule about both controls at once, a CONSISTENCY rule and never a
  // derivation: the scheme typed must agree with the transport chosen, and
  // neither field is changed to suit the other.
  const endpoint = String(v.endpoint || "").trim();
  const security = v.security === "insecureHttp" ? "insecureHttp" : "tls";
  if (endpoint.length > 0) {
    if (endpoint.indexOf(HTTPS) === 0) {
      if (security === "insecureHttp") {
        problems.endpoint = "the endpoint is https and the transport says insecureHttp. Choose " +
          "TLS, or type an " + HTTP + " endpoint -- this page will not change one to suit the " +
          "other";
      }
    } else if (endpoint.indexOf(HTTP) === 0) {
      if (security === "tls") {
        problems.endpoint = "the endpoint is plaintext http and the transport says TLS. " +
          "Plaintext is allowed, and only when it is chosen explicitly: select insecureHttp";
      }
    } else {
      problems.endpoint = "an endpoint is an origin: scheme, host and optional port, with no " +
        "a route, a query or credentials";
    }
    if (/[?#]/.test(endpoint) || endpoint.replace(/^https?:\/\//, "").indexOf("/") !== -1 ||
      endpoint.indexOf("@") !== -1) {
      problems.endpoint = "an endpoint is an origin: scheme, host and optional port, with no " +
        "a route, a query or credentials";
    }
  } else if (security === "insecureHttp") {
    problems.endpoint = "insecureHttp requires an explicit " + HTTP + " endpoint. There is no " +
      "way to reach AWS S3 in the clear and this page will not pretend there is";
  }
  // A CUSTOM ENDPOINT NEEDS pathStyle ADDRESSING ON THIS ENGINE: the pinned
  // engine addresses a custom endpoint pathStyle whatever it is told, so a
  // virtual-hosted destination would describe a location its runs never use.
  // The product API refuses it (`addressing_unsupported_by_engine`), as does
  // the controller (`AddressingUnsupportedByEngine`); this says so beside the
  // control and changes neither control.
  if (endpoint.length > 0 && v.addressing === "virtualHosted" && problems.endpoint === undefined) {
    problems.addressing = "virtualHosted addressing with a custom endpoint is refused: this " +
      "engine addresses a custom endpoint as pathStyle, so the location would not be the one its " +
      "runs use. Choose pathStyle, or clear the endpoint for AWS S3";
  }
  const caName = String(v.caName || "").trim();
  if (caName.length > 0) {
    if (security !== "tls") {
      problems.caName = "a CA bundle verifies a TLS transport; a plaintext destination has " +
        "nothing to verify";
    } else if (!isObjectName(caName)) {
      problems.caName = "the name of a ConfigMap in this namespace holding the PEM bundle";
    }
    const caKey = String(v.caKey || "").trim();
    if (caKey.length > 0 && !isDataKey(caKey)) {
      problems.caKey = "a ConfigMap data key is letters, digits, '-', '_' and '.'";
    }
  }
  Object.assign(problems, validateGrants(v));
  // A NEW destination names no existing Secret (the API refuses it as
  // `existing_credential_refused`); this says so beside the control.
  for (const role of GRANT_ROLES) {
    if (String(v[role + "Source"] || "absent") === "existing") {
      problems[role + "Source"] = "a new destination does not name an existing Secret: no " +
        "Secret can be bound to a destination that does not exist yet. Enter the credential " +
        "(new) -- it becomes a Secret owned by and bound to this destination";
    }
  }
  return problems;
}

/** The four grants' own checks, for the create form and the rotation alike.
 *
 *  Without them a rotation that chose `new` and typed nothing would send
 *  `secret.new.accessKeyId: ""`; the API refuses that, but on paths that do
 *  not match this form's input names, so it would land as an unplaced banner
 *  on the one form whose subject is a credential. This is the rotation's
 *  only check: the location and transport security are immutable, so there
 *  is nothing else on that form a rule could be about. `current` is the
 *  destination's access as read, which a `keep` repeats. */
export function validateGrants(values, current) {
  const v = values || {};
  const problems = Object.create(null);
  for (const role of GRANT_ROLES) {
    const source = String(v[role + "Source"] || "absent");
    if (source === "keep" && keptGrant((current || {})[role]) === undefined) {
      problems[role + "Source"] = "keep cannot be sent for this grant: " +
        keepRefusal((current || {})[role]) + " Enter a new credential, or rotate it with kubectl";
    }
    if (role === "archiveWrite" && source === "absent") {
      problems.archiveWriteSource = "archiveWrite is required: it is the credential every " +
        "backup Job writes the archive with";
    }
    if (EVIDENCE_READ_ONLY_SOURCES.indexOf(source) !== -1 && role !== "evidenceRead") {
      problems[role + "Source"] = source + " is an evidenceRead answer only";
    }
    if (source === "existing") {
      const name = String(v[role + "Secret"] || "").trim();
      if (!isObjectName(name)) {
        problems[role + "Secret"] = "the NAME of an existing Secret in this namespace";
      }
    }
    if (source === "new") {
      const id = String(v[role + "AccessKeyId"] || "");
      const key = String(v[role + "SecretAccessKey"] || "");
      if (id.length === 0 || key.length === 0) {
        problems[role + "AccessKeyId"] = "a new credential needs both an access key id and a " +
          "secret access key. They are cleared on every render, so a retry after a refusal " +
          "needs them typed again";
      }
    }
    if (source === "workloadIdentity") {
      const sa = String(v[role + "ServiceAccount"] || "").trim();
      if (sa.length > 0 && !isObjectName(sa)) {
        problems[role + "ServiceAccount"] = "a ServiceAccount name, or blank for logweir-runner";
      }
    }
  }
  return problems;
}

/** Why [`keptGrant`] cannot spell `view`. */
function keepRefusal(view) {
  const g = view || {};
  if (g.mode === undefined) {
    return "this page read no current grant for it.";
  }
  return "the product API publishes its Secret's data keys (" +
    (Array.isArray(g.keys) ? g.keys.join(", ") : "") + ") without saying which holds the key " +
    "id, so this page cannot name them again.";
}

/** One grant, as the product API's `AccessGrantRequest`, or `null` for absent.
 *
 *  ABSENT IS `null` AND NEVER `{mode: "inheritsArchiveWrite"}`. Those two
 *  spellings would be one state with two names, which is why the API refuses
 *  the second on a request and uses it only in a RESPONSE. `keep` is the
 *  grant in `current`, the destination's access as read. */
export function grantBody(values, role, current) {
  const v = values || {};
  const source = String(v[role + "Source"] || "absent");
  if (source === "absent") {
    return null;
  }
  if (source === "keep") {
    const kept = keptGrant((current || {})[role]);
    return kept === undefined ? null : kept;
  }
  if (source === "workloadIdentity") {
    const sa = String(v[role + "ServiceAccount"] || "").trim();
    const grant = { mode: "workloadIdentity" };
    if (sa.length > 0) {
      grant.workloadIdentity = { serviceAccountName: sa };
    }
    return grant;
  }
  if (source === "controllerIdentity" || source === "archiveReadGrant") {
    return { mode: source };
  }
  if (source === "existing") {
    return {
      mode: "secretKeys",
      secret: { existing: { name: String(v[role + "Secret"] || "").trim() } },
    };
  }
  // `new`: the write-only entry. THE ONLY PLACE IN THIS TREE WHERE A CREDENTIAL
  // VALUE IS PUT INTO AN OBJECT, and the object goes straight to the
  // product-API create and is never kept, logged or drafted.
  const fresh = {
    accessKeyId: String(v[role + "AccessKeyId"] || ""),
    secretAccessKey: String(v[role + "SecretAccessKey"] || ""),
  };
  const token = String(v[role + "SessionToken"] || "");
  if (token.length > 0) {
    fresh.sessionToken = token;
  }
  return { mode: "secretKeys", secret: { new: fresh } };
}

/** The complete four-grant object. Used by BOTH the create and the rotation,
 *  because `:update-access` takes a COMPLETE access block and a grant omitted
 *  there is REMOVED -- so a rotation form that sent only the changed role
 *  would silently delete the other three. */
export function accessBody(values, current) {
  const access = {};
  for (const role of GRANT_ROLES) {
    const grant = grantBody(values, role, current);
    if (grant !== null) {
      access[role] = grant;
    }
  }
  return access;
}

/** The `CreateDestinationRequest` a filled-in form produces. Pure. */
export function destinationBody(values) {
  const v = values || {};
  const storage = {
    provider: "s3",
    bucket: String(v.bucket || "").trim(),
    addressing: v.addressing === "virtualHosted" ? "virtualHosted" : "pathStyle",
  };
  const prefix = String(v.prefix || "").trim();
  if (prefix.length > 0) {
    storage.prefix = prefix;
  }
  const region = String(v.region || "").trim();
  if (region.length > 0) {
    storage.region = region;
  }
  const endpoint = String(v.endpoint || "").trim();
  if (endpoint.length > 0) {
    storage.endpoint = endpoint;
  }
  // TWO INDEPENDENT READS (rule 2): `security` comes from the transport radio
  // alone and `addressing` above from the addressing radio alone.
  const transport = { security: v.security === "insecureHttp" ? "insecureHttp" : "tls" };
  const caName = String(v.caName || "").trim();
  if (caName.length > 0 && transport.security === "tls") {
    transport.caBundle = { configMapName: caName };
    const caKey = String(v.caKey || "").trim();
    if (caKey.length > 0) {
      transport.caBundle.key = caKey;
    }
  }
  const body = {
    name: String(v.name || "").trim(),
    storage: storage,
    transport: transport,
    access: accessBody(v),
  };
  const description = String(v.description || "").trim();
  if (description.length > 0) {
    body.description = description;
  }
  if (v.writeProbe === "disabled" || v.writeProbe === "createOnlyMarker") {
    body.readiness = { writeProbe: v.writeProbe };
  }
  if (v.isDefault === true) {
    body.default = true;
  }
  return body;
}

/** The `UpdateDestinationAccessRequest` a rotation produces. The generation is
 *  the one this page LAST READ: a stale value is a 412, which is the right
 *  answer to "the object moved under your feet". */
export function rotationBody(values, generation, current) {
  const body = {
    expectedGeneration: typeof generation === "number" ? generation : 0,
    access: accessBody(values, current),
  };
  const caName = String((values || {}).caName || "").trim();
  if (values && values.caChange === "set" && caName.length > 0) {
    body.transport = { caBundle: { configMapName: caName } };
    const caKey = String(values.caKey || "").trim();
    if (caKey.length > 0) {
      body.transport.caBundle.key = caKey;
    }
  } else if (values && values.caChange === "clear") {
    body.transport = {};
  }
  return body;
}

/** Which inputs each credential source uses. A source shows its own inputs
 *  and the others are `hidden` -- still in the form, so a draft and a
 *  re-render keep what was typed, and `grantBody` reads only the ones its
 *  source names. */
export const SOURCE_INPUTS = Object.freeze({
  keep: Object.freeze([]),
  // The rotation form's warning that `absent` removes a grant the
  // destination has; the create form renders no such group.
  absent: Object.freeze(["removes"]),
  existing: Object.freeze(["secret"]),
  new: Object.freeze(["keys"]),
  workloadIdentity: Object.freeze(["sa"]),
  controllerIdentity: Object.freeze([]),
  archiveReadGrant: Object.freeze([]),
});

/** Whether the input group `group` is shown for `source`. */
export function sourceShows(source, group) {
  const shown = SOURCE_INPUTS[source];
  return Array.isArray(shown) && shown.indexOf(group) !== -1;
}

function grantFieldset(role, d, field, line, prefixId, sources, current) {
  const source = String(d[role + "Source"] || "absent");
  const held = current === undefined ? undefined : current[role];
  const hide = (group) => (sourceShows(source, group) ? "" : " hidden");
  const offered = sources || GRANT_SOURCES;
  const options = offered.filter(
    (s) => EVIDENCE_READ_ONLY_SOURCES.indexOf(s) === -1 || role === "evidenceRead",
  ).filter((s) => !(role === "archiveWrite" && s === "absent"));
  const id = (suffix) => prefixId + "-" + role + "-" + suffix;
  return (
    "<fieldset class=\"grant\" id=\"" + esc(prefixId + "-" + role) + "\">" +
    "<legend>" + esc(role) + "</legend>" +
    "<p class=\"help\">" + esc(ABSENT_GRANT_MEANING[role]) + "</p>" +
    "<div class=\"field\"><label for=\"" + esc(id("source")) + "\">credential source</label>" +
    "<select id=\"" + esc(id("source")) + "\" name=\"" + esc(role + "Source") + "\"" +
    field(id("source"), role + "Source") + ">" +
    options.map((s) =>
      "<option value=\"" + esc(s) + "\"" + (source === s ? " selected" : "") + ">" +
      esc(s === "keep" ? keepLabel(held) : s) + "</option>").join("") +
    "</select>" + line(id("source"), role + "Source") + "</div>" +
    // CHOOSING `absent` FOR A GRANT THE DESTINATION HAS SAYS WHAT TURNS OFF,
    // beside the control, while it is chosen.
    (ABSENT_REMOVES[role] !== undefined && holdsGrant(held)
      ? "<p class=\"caveat\" data-grant-inputs=\"removes\" id=\"" + esc(id("removes")) + "\"" +
        hide("removes") + ">" + esc(ABSENT_REMOVES[role]) + "</p>"
      : "") +
    "<div class=\"field\" data-grant-inputs=\"secret\"" + hide("secret") + "><label for=\"" +
    esc(id("secret")) + "\">existing Secret name</label>" +
    "<input id=\"" + esc(id("secret")) + "\" name=\"" + esc(role + "Secret") + "\" value=\"" +
    esc(String(d[role + "Secret"] || "")) + "\"" + field(id("secret"), role + "Secret") + ">" +
    "<p class=\"help\">The NAME only. This page never reads a Secret's contents." +
    (offered.indexOf("existing") !== -1 ? " " + esc(EXISTING_ON_ROTATION) : "") + "</p>" +
    line(id("secret"), role + "Secret") + "</div>" +
    "<div class=\"field\" data-grant-inputs=\"sa\"" + hide("sa") + "><label for=\"" + esc(id("sa")) +
    "\">ServiceAccount (workloadIdentity)</label>" +
    "<input id=\"" + esc(id("sa")) + "\" name=\"" + esc(role + "ServiceAccount") + "\" value=\"" +
    esc(String(d[role + "ServiceAccount"] || "")) + "\"" +
    field(id("sa"), role + "ServiceAccount") + ">" +
    line(id("sa"), role + "ServiceAccount") + "</div>" +
    // THE WRITE-ONLY INPUTS. `autocomplete="off"` and `type="password"` for the
    // secret half, and never a `value` attribute: a re-render would have to carry
    // it, which is exactly what rule 1 forbids.
    "<div class=\"field-row\" data-grant-inputs=\"keys\"" + hide("keys") + ">" +
    "<div class=\"field\"><label for=\"" + esc(id("akid")) + "\">new access key id</label>" +
    "<input id=\"" + esc(id("akid")) + "\" name=\"" + esc(role + "AccessKeyId") +
    "\" autocomplete=\"off\" spellcheck=\"false\"" + field(id("akid"), role + "AccessKeyId") + ">" +
    line(id("akid"), role + "AccessKeyId") + "</div>" +
    "<div class=\"field\"><label for=\"" + esc(id("sak")) + "\">new secret access key</label>" +
    "<input id=\"" + esc(id("sak")) + "\" name=\"" + esc(role + "SecretAccessKey") +
    "\" type=\"password\" autocomplete=\"new-password\"" +
    field(id("sak"), role + "SecretAccessKey") + ">" +
    line(id("sak"), role + "SecretAccessKey") + "</div>" +
    "</div>" +
    "</fieldset>"
  );
}

/** The create form. */
export function renderDestinationForm(view) {
  const v = view || {};
  const d = Object.assign({}, DESTINATION_DEFAULTS, v.draft || {});
  const errors = ((v.errors || {}).fields) || {};
  const state = v.state || {};
  const pending = state.phase === "pending";
  const field = (id, name) => invalidAttributes(id, errors[name]);
  const line = (id, name) => fieldErrorLine(id, errors[name]);
  // One button, not a 4,300 px page: the form opens from "Create destination",
  // and is open already when something is in flight there (a draft, a refusal,
  // a pending create), so no outcome is hidden.
  const busy = pending || state.phase === "failed" || Object.keys(errors).length > 0 ||
    Object.keys(v.draft || {}).some((k) => String((v.draft || {})[k] || "") !== "" &&
      String((v.draft || {})[k]) !== String(DESTINATION_DEFAULTS[k]));
  return (
    "<details class=\"create-disclosure\" id=\"destination-create-disclosure\"" +
    (busy ? " open" : "") + "><summary class=\"button primary\">Create destination</summary>" +
    "<section class=\"create\" id=\"destination-create\"><h3>Create a destination</h3>" +
    "<form id=\"destination-form\" novalidate" + (pending ? " aria-busy=\"true\"" : "") + ">" +
    "<fieldset class=\"form-body\"" + (pending ? " disabled" : "") + ">" +
    "<div class=\"field\"><label for=\"destination-name\">name</label>" +
    "<input id=\"destination-name\" name=\"name\" required value=\"" + esc(d.name) + "\"" +
    field("destination-name", "name") + ">" +
    "<p class=\"help\">Everything references this destination by this name, so pick one an " +
    "operator will recognise a year from now. It is immutable.</p>" +
    line("destination-name", "name") + "</div>" +
    "<div class=\"field\"><label for=\"destination-description\">description</label>" +
    "<input id=\"destination-description\" name=\"description\" value=\"" + esc(d.description) +
    "\"" + field("destination-description", "description") + ">" +
    line("destination-description", "description") + "</div>" +
    "<fieldset class=\"storage\"><legend>location (immutable once created)</legend>" +
    "<div class=\"field-row\">" +
    "<div class=\"field\"><label for=\"destination-bucket\">bucket</label>" +
    "<input id=\"destination-bucket\" name=\"bucket\" required value=\"" + esc(d.bucket) + "\"" +
    field("destination-bucket", "bucket") + ">" + line("destination-bucket", "bucket") + "</div>" +
    "<div class=\"field\"><label for=\"destination-prefix\">prefix</label>" +
    "<input id=\"destination-prefix\" name=\"prefix\" value=\"" + esc(d.prefix) + "\"" +
    field("destination-prefix", "prefix") + ">" +
    "<p class=\"help\">Blank is the bucket root. <code>logweir</code> and everything under it is reserved " +
    "for evidence.</p>" + line("destination-prefix", "prefix") + "</div>" +
    "</div><div class=\"field-row\">" +
    "<div class=\"field\"><label for=\"destination-region\">region</label>" +
    "<input id=\"destination-region\" name=\"region\" value=\"" + esc(d.region) + "\"" +
    field("destination-region", "region") + ">" + line("destination-region", "region") + "</div>" +
    "<div class=\"field\"><label for=\"destination-endpoint\">endpoint</label>" +
    "<input id=\"destination-endpoint\" name=\"endpoint\" value=\"" + esc(d.endpoint) + "\"" +
    field("destination-endpoint", "endpoint") + ">" +
    "<p class=\"help\">An origin: scheme, host and optional port. Blank means AWS S3.</p>" +
    line("destination-endpoint", "endpoint") + "</div>" +
    "</div>" +
    "<fieldset class=\"addressing\"><legend>addressing</legend>" +
    "<p class=\"help\">How a request NAMES the bucket, and nothing else. It does not decide " +
    "whether the connection is encrypted; the transport control below does, and only that.</p>" +
    "<label class=\"inline\" for=\"destination-addressing-pathstyle\">" +
    "<input type=\"radio\" id=\"destination-addressing-pathstyle\" name=\"addressing\" " +
    "value=\"pathStyle\"" + (d.addressing !== "virtualHosted" ? " checked" : "") +
    "> pathStyle (" + HTTPS + "endpoint/bucket/key)</label>" +
    "<label class=\"inline\" for=\"destination-addressing-virtual\">" +
    "<input type=\"radio\" id=\"destination-addressing-virtual\" name=\"addressing\" " +
    "value=\"virtualHosted\"" + (d.addressing === "virtualHosted" ? " checked" : "") +
    "> virtualHosted (" + HTTPS + "bucket.endpoint/key)</label>" +
    line("destination-addressing-pathstyle", "addressing") + "</fieldset>" +
    "</fieldset>" +
    "<fieldset class=\"transport\"><legend>transport security (immutable once created)</legend>" +
    "<p class=\"help\">The only control that decides whether this connection is encrypted. It " +
    "is independent of the addressing choice above, in both directions.</p>" +
    "<label class=\"inline\" for=\"destination-security-tls\">" +
    "<input type=\"radio\" id=\"destination-security-tls\" name=\"security\" value=\"tls\"" +
    (d.security !== "insecureHttp" ? " checked" : "") + "> TLS</label>" +
    "<label class=\"inline\" for=\"destination-security-http\">" +
    "<input type=\"radio\" id=\"destination-security-http\" name=\"security\" " +
    "value=\"insecureHttp\"" + (d.security === "insecureHttp" ? " checked" : "") +
    "> insecureHttp (plaintext; requires an explicit " + HTTP + " endpoint)</label>" +
    line("destination-security-tls", "security") +
    "<div class=\"field-row\">" +
    "<div class=\"field\"><label for=\"destination-ca-name\">private CA ConfigMap</label>" +
    "<input id=\"destination-ca-name\" name=\"caName\" value=\"" + esc(d.caName) + "\"" +
    field("destination-ca-name", "caName") + ">" +
    "<p class=\"help\">A ConfigMap and never a Secret: a CA certificate is public material.</p>" +
    line("destination-ca-name", "caName") + "</div>" +
    "<div class=\"field\"><label for=\"destination-ca-key\">CA data key</label>" +
    "<input id=\"destination-ca-key\" name=\"caKey\" value=\"" + esc(d.caKey) + "\"" +
    field("destination-ca-key", "caKey") + ">" +
    "<p class=\"help\">Blank means ca.crt.</p>" +
    line("destination-ca-key", "caKey") + "</div>" +
    "</div></fieldset>" +
    "<fieldset class=\"access\"><legend>access</legend>" +
    "<p class=\"help\">" + esc(WRITE_ONLY_SENTENCE) + "</p>" +
    GRANT_ROLES.map((role) =>
      grantFieldset(role, d, field, line, "destination", CREATE_GRANT_SOURCES)).join("") +
    "</fieldset>" +
    "<div class=\"field\"><label for=\"destination-write-probe\">write probe</label>" +
    "<select id=\"destination-write-probe\" name=\"writeProbe\">" +
    "<option value=\"createOnlyMarker\"" +
    (d.writeProbe !== "disabled" ? " selected" : "") + ">createOnlyMarker</option>" +
    "<option value=\"disabled\"" + (d.writeProbe === "disabled" ? " selected" : "") +
    ">disabled</option></select>" +
    "<p class=\"help\">" + messageText(WRITE_PROBE_SENTENCE) + "</p></div>" +
    "<label class=\"inline\" for=\"destination-default\">" +
    "<input type=\"checkbox\" id=\"destination-default\" name=\"isDefault\"" +
    (d.isDefault === true ? " checked" : "") + "> make this the namespace default</label>" +
    // The default-destination paragraph is said once, beside its checkbox.
    "<p class=\"help\">" + esc(DEFAULT_DESTINATION_SENTENCE) + "</p>" +
    "<div class=\"actions\"><button type=\"submit\" class=\"primary\">Create</button></div>" +
    "</fieldset>" +
    "<div class=\"form-status\" id=\"destination-form-status\" tabindex=\"-1\">" +
    mutationStatus(
      state,
      { kind: "BackupDestination", name: d.name, clearsCredentials: true },
      ((v.errors || {}).unmatched),
    ) +
    "</div></form></section></details>"
  );
}

/** The rotation form: the COMPLETE access block, plus the CA. */
export function renderRotateForm(item, view) {
  const d = item || {};
  const v = view || {};
  const rotate = v.rotate || {};
  const errors = ((rotate.errors || {}).fields) || {};
  const state = rotate.state || {};
  const pending = state.phase === "pending";
  const field = (id, name) => invalidAttributes(id, errors[name]);
  const line = (id, name) => fieldErrorLine(id, errors[name]);
  if (v.mayOperate === false) {
    return "<section class=\"rotate\" id=\"destination-rotate\"><h3>Rotate access</h3>" +
      "<p class=\"note\">This login may read destinations and not manage them.</p></section>";
  }
  const draft = Object.assign({}, ROTATE_DEFAULTS, rotate.draft || {});
  return (
    "<section class=\"rotate\" id=\"destination-rotate\"><h3>Rotate access</h3>" +
    "<p class=\"note\">This sends the COMPLETE four-grant block. Each grant starts on " +
    "<code>keep</code>, which sends it as it is; a grant set to <code>absent</code> here is " +
    "REMOVED from the destination. It carries the generation this " +
    "page read (<code id=\"rotate-generation\">" + cell(d.generation) + "</code>); if the object " +
    "moved since, the answer is a 412 and nothing is written.</p>" +
    "<p class=\"note\">The location and the transport security are immutable. Only the " +
    "credentials and the CA reference can change here.</p>" +
    "<form id=\"destination-rotate-form\" novalidate" + (pending ? " aria-busy=\"true\"" : "") + ">" +
    "<fieldset class=\"form-body\"" + (pending ? " disabled" : "") + ">" +
    "<input type=\"hidden\" name=\"expectedGeneration\" id=\"rotate-expected-generation\" value=\"" +
    esc(String(d.generation === undefined || d.generation === null ? "" : d.generation)) + "\">" +
    GRANT_ROLES.map((role) =>
      grantFieldset(role, draft, field, line, "rotate", ROTATE_GRANT_SOURCES, d.access || {}))
      .join("") +
    "<div class=\"field\"><label for=\"rotate-ca-change\">private CA</label>" +
    "<select id=\"rotate-ca-change\" name=\"caChange\">" +
    "<option value=\"keep\"" + (draft.caChange === "keep" || draft.caChange === undefined
      ? " selected" : "") + ">leave it alone</option>" +
    "<option value=\"set\"" + (draft.caChange === "set" ? " selected" : "") + ">point it at a " +
    "ConfigMap</option>" +
    "<option value=\"clear\"" + (draft.caChange === "clear" ? " selected" : "") + ">clear it" +
    "</option></select></div>" +
    "<div class=\"field-row\">" +
    "<div class=\"field\"><label for=\"rotate-ca-name\">CA ConfigMap</label>" +
    "<input id=\"rotate-ca-name\" name=\"caName\" value=\"" + esc(draft.caName) + "\"" +
    field("rotate-ca-name", "caName") + ">" + line("rotate-ca-name", "caName") + "</div>" +
    "<div class=\"field\"><label for=\"rotate-ca-key\">CA data key</label>" +
    "<input id=\"rotate-ca-key\" name=\"caKey\" value=\"" + esc(draft.caKey) + "\"" +
    field("rotate-ca-key", "caKey") + ">" + line("rotate-ca-key", "caKey") + "</div>" +
    "</div>" +
    "<div class=\"actions\"><button type=\"submit\">Rotate access</button></div>" +
    "</fieldset>" +
    "<div class=\"form-status\" id=\"destination-rotate-status\" tabindex=\"-1\">" +
    mutationStatus(
      state,
      { kind: "BackupDestination", name: d.name, clearsCredentials: true },
      ((rotate.errors || {}).unmatched),
    ) +
    "</div></form></section>"
  );
}

/** Reads the create form's values. The credential inputs ARE read here --
 *  the request carries them -- and the object goes to `destinationBody` and
 *  to `keepDraft`, whose allowlist drops them. */
export function readDestinationValues(form) {
  return readFormValues(form);
}

function viewFor(ns, form) {
  const key = formKey(ns, form);
  const state = mutationFor(key).state;
  if (state.phase === "succeeded") {
    dropDraft(key);
  }
  return {
    draft: readDraft(key),
    state: state,
    errors: state.phase === "failed" ? fieldErrors(state.error, DESTINATION_FIELD_PATHS) : null,
  };
}

/** The product API's own field paths, mapped onto this form's input names, so
 *  a 422 lands beside the control it is about. */
export const DESTINATION_FIELD_PATHS = Object.freeze({
  name: "name",
  description: "description",
  "storage.bucket": "bucket",
  "storage.prefix": "prefix",
  "storage.region": "region",
  "storage.endpoint": "endpoint",
  "storage.addressing": "addressing",
  "transport.security": "security",
  "transport.caBundle.configMapName": "caName",
  "transport.caBundle.key": "caKey",
  "access.archiveWrite": "archiveWriteSource",
  "access.archiveWrite.mode": "archiveWriteSource",
  "access.archiveWrite.secret.existing.name": "archiveWriteSecret",
  // `existing_credential_refused` names the `existing` object itself.
  "access.archiveWrite.secret.existing": "archiveWriteSecret",
  "access.archiveRead.mode": "archiveReadSource",
  "access.archiveRead.secret.existing.name": "archiveReadSecret",
  "access.archiveRead.secret.existing": "archiveReadSecret",
  "access.evidenceWrite.mode": "evidenceWriteSource",
  "access.evidenceWrite.secret.existing.name": "evidenceWriteSecret",
  "access.evidenceWrite.secret.existing": "evidenceWriteSecret",
  "access.evidenceRead.mode": "evidenceReadSource",
  "access.evidenceRead.secret.existing.name": "evidenceReadSecret",
  "access.evidenceRead.secret.existing": "evidenceReadSecret",
  "readiness.writeProbe": "writeProbe",
  default: "isDefault",
});

/** Checks the values, then creates the destination. */
export async function submitDestination(ns, values, deps) {
  const problems = validateDestination(values);
  if (Object.keys(problems).length > 0) {
    throw invalidInput(problems);
  }
  return (deps || API).createDestination(ns, destinationBody(values));
}

/** Checks the grants, then rotates. Nothing is sent when a grant does not make
 *  a credential the API could use -- see [`validateGrants`]. */
export async function submitRotation(ns, name, values, generation, deps, current) {
  const problems = validateGrants(values, current);
  if (Object.keys(problems).length > 0) {
    throw invalidInput(problems);
  }
  return (deps || API).updateDestinationAccess(ns, name,
    rotationBody(values, generation, current));
}

export async function mountDestinations(node, ns, parse, lifecycle, deps) {
  const api = deps || API;
  try {
    const page = await api.destinations(ns, readOptions(lifecycle));
    if (!active(lifecycle)) {
      return;
    }
    const may = mayOperate(ns);
    replace(
      node,
      parse(
        renderDestinationList(page, ns) +
          (may
            ? "<div class=\"form-slot\" id=\"destination-form-slot\">" +
              renderDestinationForm(viewFor(ns, CREATE_FORM)) +
              "</div>"
            : "<p class=\"note\" id=\"destinations-read-only\">This login may read " +
              "destinations in this namespace and not create them.</p>"),
      ),
    );
    if (may) {
      wireCreate(node, ns, parse, lifecycle, api);
    }
  } catch (error) {
    if (!cancelled(error, lifecycle) && active(lifecycle)) {
      replace(node, errorBox(error));
    }
  }
}

export async function mountDestinationDetail(node, ns, name, parse, lifecycle, deps) {
  const api = deps || API;
  try {
    const read = await api.destination(ns, name, readOptions(lifecycle));
    if (!active(lifecycle)) {
      return;
    }
    // The usage read is separate and its failure is not the page's: a
    // destination whose `/usage` answers 403 still renders, and the lost section
    // says so.
    let usage = null;
    let usageError = null;
    try {
      usage = await api.destinationUsage(ns, name, readOptions(lifecycle));
    } catch (failed) {
      if (cancelled(failed, lifecycle)) {
        return;
      }
      usageError = failed;
    }
    if (!active(lifecycle)) {
      return;
    }
    // A FRESH DETAIL READ STARTS FROM THE SERVER'S ANSWER: its `lastTest` is
    // the newest recorded test, newer than anything a previous visit to this
    // view remembered, so nothing remembered is painted over it.
    testViews.delete(formKey(ns, TEST_FORM, name));
    paintDetail(node, ns, name, parse, lifecycle, api, read.item, {
      usage: usage, usageError: usageError, test: null,
    }, true);
  } catch (error) {
    if (!cancelled(error, lifecycle) && active(lifecycle)) {
      replace(node, errorBox(error));
    }
  }
}

/** The detail's view: the page's own records (the test it started, the
 *  rotation's draft and mutation) under what `extra` carries. */
function detailView(ns, name, extra) {
  const testKey = formKey(ns, TEST_FORM, name);
  const rotateKey = formKey(ns, ROTATE_FORM, name);
  const given = extra || {};
  if (given.test !== undefined && given.test !== null) {
    testViews.set(testKey, { test: given.test });
  }
  const remembered = testViews.get(testKey) || {};
  return Object.assign(
    {
      mayOperate: mayOperate(ns),
      testState: mutationFor(testKey).state,
      testPending: mutationFor(testKey).state.phase === "pending",
      rotate: {
        draft: readDraft(rotateKey),
        state: mutationFor(rotateKey).state,
        errors: mutationFor(rotateKey).state.phase === "failed"
          ? fieldErrors(mutationFor(rotateKey).state.error, DESTINATION_FIELD_PATHS)
          : null,
      },
    },
    given,
    { test: given.test || remembered.test || null },
  );
}

function paintDetail(node, ns, name, parse, lifecycle, api, item, extra, first) {
  const view = detailView(ns, name, extra);
  // The mount's first paint is new content; a later one repaints the detail
  // the reader is on and keeps their place there.
  (first === true ? replace : replaceInPlace)(node, parse(renderDestinationDetail(item, view)));
  wireTest(node, ns, name, parse, lifecycle, api, item, view);
  wireRotate(node, ns, name, parse, lifecycle, api, item, view);
}

/** The roles selected on the Test access form, read off the live select. */
export function readTestRoles(node) {
  const select = node.querySelector("#test-roles");
  if (select === null || select.options === undefined || select.options === null) {
    return [];
  }
  return Array.prototype.filter.call(select.options, (o) => o.selected === true)
    .map((o) => String(o.value));
}

/** Repaints the test half alone -- the answer a followed test waits for is
 *  about that half and nothing else -- or the whole detail when the slot is
 *  not on screen. */
function paintTest(node, ns, name, parse, lifecycle, api, item, extra) {
  const slot = node.querySelector("#destination-test-slot");
  if (slot === null) {
    paintDetail(node, ns, name, parse, lifecycle, api, item, extra);
    return;
  }
  // THE ROLES CHOSEN FOR THE NEXT TEST survive the repaint of this one.
  const view = Object.assign(detailView(ns, name, extra), { testRoles: readTestRoles(node) });
  replaceInPlace(slot, parse(renderTestSlot(item, view)));
  wireTest(node, ns, name, parse, lifecycle, api, item, view);
}

/** A source selector shows its own inputs and hides the rest as the reader
 *  changes it, in every grant fieldset on the page, create and rotate alike. */
function wireGrantSources(node, lifecycle) {
  for (const set of node.querySelectorAll("fieldset.grant")) {
    const select = set.querySelector("select");
    if (select === null) {
      continue;
    }
    const apply = () => {
      for (const group of set.querySelectorAll("[data-grant-inputs]")) {
        group.hidden = !sourceShows(String(select.value), group.getAttribute("data-grant-inputs"));
      }
    };
    listen(select, "change", apply, lifecycle);
  }
}

function wireCreate(node, ns, parse, lifecycle, api) {
  const form = node.querySelector("#destination-form");
  if (form === null) {
    return;
  }
  const key = formKey(ns, CREATE_FORM);
  const mutation = mutationFor(key);
  const remember = () => {
    if (!active(lifecycle)) {
      return;
    }
    // THE ALLOWLIST IS WHAT DROPS THE CREDENTIAL, applied here on the values the
    // form just produced, which do carry one.
    keepDraft(key, readDestinationValues(form), DESTINATION_DRAFT_FIELDS);
    if (mutation.state.phase === "succeeded") {
      mutation.clear();
    }
  };
  listen(form, "input", remember, lifecycle);
  listen(form, "change", remember, lifecycle);
  wireGrantSources(node, lifecycle);
  watchMutation(node, key, mutation, (state) => {
    if (state.phase === "succeeded") {
      dropDraft(key);
      mountDestinations(node, ns, parse, lifecycle, api);
      return;
    }
    const slot = node.querySelector("#destination-form-slot");
    if (slot === null) {
      return;
    }
    replace(slot, parse(renderDestinationForm(viewFor(ns, CREATE_FORM))));
    wireCreate(node, ns, parse, lifecycle, api);
    if (state.phase === "failed") {
      focusFirstProblem(node, "#destination-form-status");
    }
  }, lifecycle);
  listen(form, "submit", (event) => {
    event.preventDefault();
    if (!active(lifecycle) || mutation.pending()) {
      return;
    }
    const values = readDestinationValues(form);
    keepDraft(key, values, DESTINATION_DRAFT_FIELDS);
    mutation.run(() => submitDestination(ns, values, api));
  }, lifecycle);
}

function wireTest(node, ns, name, parse, lifecycle, api, item, view) {
  const form = node.querySelector("#destination-test-form");
  if (form === null) {
    return;
  }
  const key = formKey(ns, TEST_FORM, name);
  const mutation = mutationFor(key);
  watchMutation(node, key, mutation, (state) => {
    if (!active(lifecycle)) {
      return;
    }
    if (state.phase === "succeeded") {
      const made = (state.result || {}).item || null;
      const extra = { usage: view.usage, usageError: view.usageError, test: made };
      paintTest(node, ns, name, parse, lifecycle, api, item, extra);
      followDestinationTest(node, ns, name, parse, lifecycle, api, item, view, made);
      return;
    }
    paintTest(node, ns, name, parse, lifecycle, api, item, {
      usage: view.usage, usageError: view.usageError,
    });
  }, lifecycle);
  const start = () => {
    if (!active(lifecycle) || mutation.pending()) {
      return;
    }
    const values = readDestinationValues(form);
    const roles = Array.isArray(values.roles) ? values.roles : [];
    // The token is minted inside the executor, as the connection check's is, so
    // a platform with no random source is a refusal in this form's status region
    // rather than an exception out of an event handler.
    mutation.run(() => api.testDestination(ns, name, roles.length > 0 ? { roles: roles } : {}, {
      attempt: nextDestinationTestAttempt(ns, name),
    }));
  };
  listen(form, "submit", (event) => {
    event.preventDefault();
    start();
  }, lifecycle);
  // "Run the check again" is Test access again, with the roles on the form: a
  // per-click token, so a new test.
  wireCheckRetry(node, view.test, start, lifecycle);
}

/** Re-reads the test this page started until a READ answers terminal, or the
 *  longest time a check may take has passed (`lifecycle.js`'s `followCheck`).
 *  The create answer is not a read (`owesRead`); a failed read is not a
 *  verdict (the rows stay what the check last recorded); an older follower
 *  stops once a newer test is on screen; and a follow that ends without a
 *  result says so with "Run the check again". */
function followDestinationTest(node, ns, name, parse, lifecycle, api, item, view, first) {
  const key = formKey(ns, TEST_FORM, name);
  const mine = () => ((testViews.get(key) || {}).test || {}).id === (first || {}).id;
  return followCheck({
    first: first,
    budgetMs: PREFLIGHT_FOLLOW_MS,
    wait: api.wait,
    keep: () => active(lifecycle) && mine(),
    cancelled: (error) => cancelled(error, lifecycle),
    signal: (readOptions(lifecycle) || {}).signal,
    read: async (current, options) =>
      (((await api.preflight(ns, current.id, options)) || {}).item) || current,
    show: (current) => {
      paintTest(node, ns, name, parse, lifecycle, api, item, {
        usage: view.usage, usageError: view.usageError, test: current,
      });
    },
  });
}

function wireRotate(node, ns, name, parse, lifecycle, api, item, view) {
  wireGrantSources(node, lifecycle);
  const form = node.querySelector("#destination-rotate-form");
  if (form === null) {
    return;
  }
  const key = formKey(ns, ROTATE_FORM, name);
  const mutation = mutationFor(key);
  listen(form, "input", () => {
    if (active(lifecycle)) {
      keepDraft(key, readDestinationValues(form),
        DESTINATION_DRAFT_FIELDS.concat(["caChange"]));
    }
  }, lifecycle);
  listen(form, "change", () => {
    if (active(lifecycle)) {
      keepDraft(key, readDestinationValues(form),
        DESTINATION_DRAFT_FIELDS.concat(["caChange"]));
    }
  }, lifecycle);
  watchMutation(node, key, mutation, (state) => {
    if (!active(lifecycle)) {
      return;
    }
    if (state.phase === "succeeded") {
      dropDraft(key);
      mountDestinationDetail(node, ns, name, parse, lifecycle, api);
      return;
    }
    // THE NEWEST TEST, not the one this wire was handed: a followed test
    // repaints its own slot and does not re-wire this form, so `view.test`
    // may be older than the test on screen.
    paintDetail(node, ns, name, parse, lifecycle, api, item, {
      usage: view.usage, usageError: view.usageError,
    });
    if (state.phase === "failed") {
      focusFirstProblem(node, "#destination-rotate-status");
    }
  }, lifecycle);
  listen(form, "submit", (event) => {
    event.preventDefault();
    if (!active(lifecycle) || mutation.pending()) {
      return;
    }
    const values = readDestinationValues(form);
    keepDraft(key, values, DESTINATION_DRAFT_FIELDS.concat(["caChange"]));
    mutation.run(() => submitRotation(ns, name, values, item.generation, api, item.access));
  }, lifecycle);
}
