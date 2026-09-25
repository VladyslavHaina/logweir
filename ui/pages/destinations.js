// pages/destinations.js -- saved destinations (PLAT-08.1, PLAT-08.2): the
// list, the create form, the access rotation, the access test, what uses one,
// and adopting a legacy inline archive.
//
// WHY THIS PAGE EXISTS. Before it, every schedule and every restore spelled
// its own archive URL, its own endpoint, its own region and its own Secret
// name, in a form field beside a checkbox called "path_style". Two schedules
// meant to write to one bucket could disagree about any of those and nothing
// would say so until a run failed, or -- worse -- until it succeeded against
// the wrong place. A destination is that answer written down ONCE, with an
// identity, so everything else references it by name.
//
// FOUR RULES THIS PAGE KEEPS, AND WHAT EACH ONE IS FOR.
//
// 1. THE CREDENTIAL IS NEVER IN A DRAFT. `ui/lifecycle.js`'s `keepDraft` takes
//    an ALLOWLIST of field names, and [`DESTINATION_DRAFT_FIELDS`] does not
//    name `archiveWriteAccessKeyId`, `archiveWriteSecretAccessKey` or any of
//    their siblings. So a typed credential survives exactly as long as the
//    form element does: through nothing. Not a refusal, not a route change,
//    not a re-render after a 409. That is deliberate and it is the whole
//    reason the allowlist is an allowlist -- a denylist would have meant the
//    safe default for the NEXT credential field somebody adds is "kept".
//    The twelve credential field names are DERIVED from the four roles, so a
//    fifth role cannot add three the disjointness assertion does not cover.
//    `ui/tests/credentials.spec.js` plants a password in a draft and fails.
//
// 2. ADDRESSING IS NOT TRANSPORT (defect G5). `addressing` says how the bucket
//    appears in the URL; `transport.security` alone says whether the
//    connection is encrypted. They are two independent controls, sitting in
//    two different fieldsets, and neither derives the other. The wizard once
//    derived `allowHttp` from a pathStyle checkbox, which meant an operator
//    ticking "path_style addressing" silently turned TLS off; W13a deleted
//    that derivation and this page was written so it could not come back.
//
// 3. ABSENT IS NOT FALSE. `status.valid` is OPTIONAL on the wire and `null`
//    means the controller has not reached a verdict. On this laboratory
//    cluster NO controller reconciles a `BackupDestination` at all -- the
//    images predate the kind -- so every destination reads "not judged yet".
//    Rendering that as "invalid" would report a missing controller as a broken
//    destination, and rendering it as "valid" would be worse.
//
// 4. A TEST'S VERDICT COMES FROM THE CHECK, NOT FROM THIS PAGE. "Test access"
//    creates a `Preflight` and then renders that object's own recorded state.
//    It does not dial anything, it does not infer a pass from the absence of a
//    failure, and a `Preflight` that has not been reconciled reads `pending`
//    -- which is what it is. AND IT READS THE OBJECT BACK (defect P8): the
//    create answer is the check before it ran, projected without recomputing
//    staleness, so the page follows the started check with bounded GETs until
//    a read answers terminal. Before that it rendered the create answer for
//    ever -- `pending / does not apply / compared: nothing` four minutes after
//    the check had recorded `ready`.

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
  mutationFor,
  owesRead,
  readDraft,
  readOptions,
  watchMutation,
} from "../lifecycle.js";
import {
  ABSENT,
  DESTINATION_TEST_SENTENCE,
  NOT_JUDGED_SENTENCE,
  applicabilityLine,
  badge,
  cell,
  checkTable,
  destinationVerdict,
  detailLink,
  errorBox,
  esc,
  executionOnlyBlock,
  facts,
  fieldErrorLine,
  invalidAttributes,
  mutationStatus,
  preflightVerdict,
  replace,
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
// `SCHEME_SEPARATOR` follows, and `scripts/check-ui-offline.sh` enforces over
// every line of this tree). A reviewer grepping `ui/` for `http` followed by a
// colon must get an empty result and learn something true: this page names no
// resource outside the directory it was served from. These are pieces of
// PROSE and of a VALIDATOR -- the scheme an operator typed into a text field
// -- and not identifiers this page would fetch, but the gate cannot tell those
// apart from a line and it is right not to try.
const HTTPS = "https" + ":" + "//";
const HTTP = "http" + ":" + "//";

const CREATE_FORM = "destination-create";
const ROTATE_FORM = "destination-rotate";
const LEGACY_FORM = "destination-from-legacy";
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
  archiveWriteSource: "existing", archiveWriteSecret: "", archiveWriteServiceAccount: "",
  archiveReadSource: "absent", archiveReadSecret: "", archiveReadServiceAccount: "",
  evidenceWriteSource: "absent", evidenceWriteSecret: "", evidenceWriteServiceAccount: "",
  evidenceReadSource: "absent", evidenceReadSecret: "", evidenceReadServiceAccount: "",
});

/** THE DRAFT ALLOWLIST. Read rule 1 at the top of this file before adding to
 *  it. Every field here is a NAME, a FLAG or an ENUM MEMBER -- a bucket, a
 *  Secret's name, a radio's value. Not one of them is a credential, and every
 *  name in [`CREDENTIAL_INPUTS`] is deliberately absent, so `keepDraft` cannot
 *  keep one even if a future caller hands it the whole form. */
export const DESTINATION_DRAFT_FIELDS = Object.freeze([
  "name", "description", "bucket", "prefix", "region", "endpoint",
  "addressing", "security", "caName", "caKey", "writeProbe", "isDefault",
  "archiveWriteSource", "archiveWriteSecret", "archiveWriteServiceAccount",
  "archiveReadSource", "archiveReadSecret", "archiveReadServiceAccount",
  "evidenceWriteSource", "evidenceWriteSecret", "evidenceWriteServiceAccount",
  "evidenceReadSource", "evidenceReadSecret", "evidenceReadServiceAccount",
]);

/** EVERY FIELD NAME THAT CARRIES A CREDENTIAL VALUE, derived from the four
 *  roles rather than written out, so a fifth role cannot add three names this
 *  list does not have.
 *
 *  Exported so the suite can assert -- mechanically, not by reading the
 *  allowlist above -- that NONE of them is in it. A field added to the form and
 *  forgotten here is still not kept, because the allowlist is the gate; this
 *  list is what makes the forgetting VISIBLE.
 *
 *  `<role>SessionToken` IS IN IT even though no input renders one yet (review
 *  F4). `grantBody` already reads that field and puts it in `secret.new`, so
 *  the day the input lands the disjointness assertion covers it -- rather than
 *  the day after, when somebody notices. */
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

/** What `:from-legacy` does, and what it refuses to do. */
export const FROM_LEGACY_SENTENCE =
  "Derives the location from FACTS about an existing schedule or backup -- the frozen execution " +
  "inputs of its newest succeeded run, or its own spec -- and never from a guess. When neither " +
  "can be read the answer is a refusal, not a destination pointed at a bucket nobody named.";

// ---------------------------------------------------------------- the reads

/** The destinations table. NAME, DEFAULT, LOCATION, TRANSPORT, ADDRESSING,
 *  VERDICT, GENERATION.
 *
 *  TRANSPORT AND ADDRESSING ARE TWO COLUMNS, not one "mode" column, for the
 *  same reason they are two controls on the form: they are two facts, and a
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
        "schedule starts with none chosen. " + esc(DEFAULT_ANNOTATION_RESIDUAL) + "</p>"
      : "") +
    (items.some((d) => d.status === null || d.status === undefined ||
      d.status.valid === null || d.status.valid === undefined)
      ? "<p class=\"note\" id=\"destinations-unjudged\">" + esc(NOT_JUDGED_SENTENCE) + "</p>"
      : "")
  );
}

/** A transport cell. `insecureHttp` is a BADGE and not a word in a cell, and
 *  the badge says what it is: plaintext over the network. It is a legal,
 *  explicitly configured choice for a lab endpoint and the page does not
 *  refuse it -- it refuses to let it look like the other one. */
export function transportCell(security) {
  if (security === "insecureHttp") {
    // THE WARNING STYLE (MCP-12): a neutral grey pill read as "fine".
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
    // THE SUMMARY STANDS DOWN ONCE THIS PAGE HOLDS A TEST OF ITS OWN (the
    // rule the connection panel keeps): the summary is what the detail read
    // found BEFORE the test was started, and "No access test has been
    // recorded" printed above the test this page just started contradicted it.
    "<div class=\"test-slot\" id=\"destination-test-slot\">" + renderTestSlot(d, v) + "</div>" +
    renderUsage(v.usage, v.usageError) +
    renderRotateForm(d, v)
  );
}

/** The test half of the detail: the recorded summary (until this page holds a
 *  test of its own) and the Test access panel. ITS OWN SLOT (P13's class): a
 *  followed test repaints this and nothing else, so the rotation form below
 *  -- whose credential inputs no draft may keep and no re-render may carry --
 *  is never re-rendered under a reader typing into it. */
export function renderTestSlot(item, view) {
  const d = item || {};
  const v = view || {};
  return (v.test ? "" : renderLastTest(d.lastTest)) + renderTestPanel(d, v);
}

/** The recorded `lastTest`, which is a POINTER and not a verdict of its own.
 *
 *  `truncated` IS RENDERED. It means the search for the newest test hit its
 *  page bound, so this may not actually be the newest one -- and a "last test"
 *  that might not be last is worse than none, so the reader is told rather
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

/** The "Test access" control and whatever the started `Preflight` says.
 *
 *  THE VERDICT IS THE OBJECT'S. `view.test` is the `Preflight` this page last
 *  read, rendered field for field: its aggregate, its applicability, its
 *  blocking rows, its advisory rows and its execution-only notes. When the
 *  check has not been reconciled -- which is every check on a cluster whose
 *  controller predates the kind -- that is `pending`, and `pending` is what
 *  appears. */
export function renderTestPanel(item, view) {
  const v = view || {};
  const test = v.test || null;
  const may = v.mayOperate !== false;
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
        "</fieldset></form>"
      : "<p class=\"note\">This login may read destinations and not test them; the roles it " +
        "holds here do not include operator or administrator.</p>") +
    "<div class=\"form-status\" id=\"destination-test-status\" tabindex=\"-1\">" +
    mutationStatus(v.testState || {}, { kind: "Preflight", name: (test || {}).id || "" }, null) +
    "</div>" +
    (test === null ? "" : renderPreflight(test)) +
    (test !== null && v.testStopped === true
      ? "<p class=\"note\" id=\"destination-test-stopped\">" +
        esc(DESTINATION_TEST_STOPPED_SENTENCE) + "</p>"
      : "") +
    "</section>"
  );
}

/** What the follower says when it stops reading. */
export const DESTINATION_TEST_STOPPED_SENTENCE =
  "This page stopped following the access test after its read budget, or after a read failed. " +
  "The test itself was not cancelled and is still the controller's; reload this page to read " +
  "the newest recorded test, or press Test access again for a new one.";

/** How many times the panel re-reads the test it started, and the gap between
 *  reads. BOUNDED, as the connection check's follower is: a panel is not a
 *  watcher. Thirty reads two seconds apart is a minute, which covers one check
 *  pod scheduled on a busy node (the PoC's took three seconds). */
export const DESTINATION_TEST_POLLS = 30;

/** The gap between those reads, in milliseconds. */
export const DESTINATION_TEST_INTERVAL_MS = 2000;

// THE ATTEMPT TOKEN, as the connection check mints it (review F1): this load's
// nonce and a per-click ordinal, so a DELIBERATE second test is a new
// `Preflight` rather than a replay of the first one's verdict.
let testAttempts = 0;

/** The token one accepted Test access click carries. */
export function nextDestinationTestAttempt(ns, name) {
  const mint = connectionNonce();
  testAttempts += 1;
  return String(ns) + "." + String(name) + ".test." + mint + "-" + String(testAttempts);
}

// THE STARTED TEST, REMEMBERED PER DESTINATION FOR THE LIFE OF THE LOADED PAGE
// -- the connection panel's `checkViews`, for the same reason: every other
// control on this view repaints the whole detail, and without this the first
// such repaint would wipe the result the operator had just asked for. It also
// names the ONE test being followed, so an older follower stops painting the
// moment a newer test replaces it.
const testViews = new Map();

/** A `Preflight`, rendered from its own recorded fields and from nothing else.
 *
 *  SHARED BY THREE CALLERS -- the destination test here, the schedules
 *  readiness panel and the restore wizard's step 5 -- because a readiness
 *  verdict must read the same in all three. A page that rendered its own
 *  version would be a page that could render a different one. */
export function renderPreflight(preflight) {
  const p = preflight || {};
  const binding = p.binding || {};
  return (
    "<div class=\"preflight-result\" id=\"preflight-" + esc(String(p.id || "")) + "\">" +
    "<p class=\"preflight-head\">" + preflightVerdict(p.state) +
    " <code>" + cell(p.id) + "</code> " + cell(p.operation) + "</p>" +
    applicabilityLine(p) +
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
 *  THE BASIS IS RENDERED BESIDE AN EMPTY LIST, not instead of it. The product
 *  API builds these from a label IT sets on the objects IT creates, so a
 *  schedule made with kubectl is invisible here -- and an empty list that did
 *  not say so would read as "nothing uses this", which is the sentence an
 *  operator would delete a destination on. */
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
    table(["KIND", "NAME", "CREATED"], rows, "Nothing labelled by this service names it.") +
    (usage.truncated === true
      ? "<p class=\"note\">One of the lists was cut at its bound; there may be more.</p>"
      : "") +
    "<p class=\"note\" id=\"usage-basis\">" + cell(usage.basis) + "</p>" +
    "</section>"
  );
}

// ------------------------------------------------------------- the two forms

/** The page's own checks over the create form's values. A CONVENIENCE: the
 *  product API's `destination_invalid` and the CRD's CEL rules are the gate,
 *  and every message here names the rule rather than restating the value. */
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
  // THE ONE RULE THAT IS ABOUT BOTH CONTROLS AT ONCE, and it is a CONSISTENCY
  // rule and never a derivation: the scheme the operator typed must AGREE with
  // the transport they chose. Neither field is changed to suit the other.
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
  // A CUSTOM ENDPOINT NEEDS pathStyle ADDRESSING ON THIS ENGINE (D2 G4,
  // ENGINE-PATHSTYLE): the pinned engine addresses a custom endpoint pathStyle
  // whatever it is told, so a virtual-hosted destination there would describe a
  // location its runs never use. The product API refuses it as
  // `addressing_unsupported_by_engine` and the controller as
  // `AddressingUnsupportedByEngine`; this says so beside the control, and it
  // changes neither control to suit the other.
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
  return problems;
}

/** The four grants' own checks, for the CREATE form and the ROTATION alike.
 *
 *  FACTORED OUT BECAUSE THE ROTATION HAD NONE (review F3). `wireRotate` called
 *  `rotationBody` straight from the form, so an operator who chose `new` and
 *  typed nothing sent `secret.new.accessKeyId: ""`. That fails closed at the
 *  API -- `check_credential` answers `required`, "the value is empty" -- but
 *  the 422 comes back on paths (`archiveWrite.secret.new.accessKeyId`) that do
 *  not match this form's input names, so it landed as an unplaced banner on the
 *  one form whose entire subject is a credential. The message here is the same
 *  one the create form has always shown, beside the field it is about.
 *
 *  IT IS THE ROTATION'S ONLY CHECK, and that is correct: the location and the
 *  transport security are immutable, so there are no bucket, endpoint or
 *  scheme rules to run -- there is nothing on that form they could be about. */
export function validateGrants(values) {
  const v = values || {};
  const problems = Object.create(null);
  for (const role of GRANT_ROLES) {
    const source = String(v[role + "Source"] || "absent");
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

/** One grant, as the product API's `AccessGrantRequest`, or `null` for absent.
 *
 *  ABSENT IS `null` AND NEVER `{mode: "inheritsArchiveWrite"}`. Those two
 *  spellings would be one state with two names, which is why the API refuses
 *  the second on a request and uses it only in a RESPONSE. */
export function grantBody(values, role) {
  const v = values || {};
  const source = String(v[role + "Source"] || "absent");
  if (source === "absent") {
    return null;
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
  // `new`: the write-only entry. THE ONLY PLACE IN THIS TREE WHERE A
  // CREDENTIAL VALUE IS PUT INTO AN OBJECT, and the object goes straight to
  // the product-API create and is never kept, logged or drafted.
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
export function accessBody(values) {
  const access = {};
  for (const role of GRANT_ROLES) {
    const grant = grantBody(values, role);
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
  // TWO INDEPENDENT READS. `security` comes from the transport radio and from
  // nothing else; `addressing` above comes from the addressing radio and from
  // nothing else. Neither expression mentions the other field (defect G5).
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
export function rotationBody(values, generation) {
  const body = {
    expectedGeneration: typeof generation === "number" ? generation : 0,
    access: accessBody(values),
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

/** WHICH INPUTS EACH CREDENTIAL SOURCE USES (MCP-10). The form used to show
 *  the Secret name, the ServiceAccount and both new-key inputs under every
 *  role whatever its source, `absent` included; now a source shows its own
 *  inputs and the others are `hidden` -- still in the form, so a draft and a
 *  re-render keep what was typed, and `grantBody` reads only the ones its
 *  source names, as it always did. */
export const SOURCE_INPUTS = Object.freeze({
  absent: Object.freeze([]),
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

function grantFieldset(role, d, field, line, prefixId) {
  const source = String(d[role + "Source"] || "absent");
  const hide = (group) => (sourceShows(source, group) ? "" : " hidden");
  const options = GRANT_SOURCES.filter(
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
      esc(s) + "</option>").join("") +
    "</select>" + line(id("source"), role + "Source") + "</div>" +
    "<div class=\"field\" data-grant-inputs=\"secret\"" + hide("secret") + "><label for=\"" +
    esc(id("secret")) + "\">existing Secret name</label>" +
    "<input id=\"" + esc(id("secret")) + "\" name=\"" + esc(role + "Secret") + "\" value=\"" +
    esc(String(d[role + "Secret"] || "")) + "\"" + field(id("secret"), role + "Secret") + ">" +
    "<p class=\"help\">The NAME only. This page never reads a Secret's contents.</p>" +
    line(id("secret"), role + "Secret") + "</div>" +
    "<div class=\"field\" data-grant-inputs=\"sa\"" + hide("sa") + "><label for=\"" + esc(id("sa")) +
    "\">ServiceAccount (workloadIdentity)</label>" +
    "<input id=\"" + esc(id("sa")) + "\" name=\"" + esc(role + "ServiceAccount") + "\" value=\"" +
    esc(String(d[role + "ServiceAccount"] || "")) + "\"" +
    field(id("sa"), role + "ServiceAccount") + ">" +
    line(id("sa"), role + "ServiceAccount") + "</div>" +
    // THE WRITE-ONLY INPUTS. `autocomplete="off"` and `type="password"` for the
    // secret half; no `value` attribute on either, ever, because a value
    // attribute is what a re-render would have to carry -- and a re-render
    // that carried a credential is exactly the failure rule 1 forbids.
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
  // ONE BUTTON, NOT A 4,300 PX PAGE (MCP-10). The form opens from "Create
  // destination"; it is open already when the reader has something in flight
  // there -- a draft, a refusal, a pending create -- so no outcome is hidden.
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
    "<p class=\"help\">Blank is the bucket root. `logweir` and everything under it is reserved " +
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
    GRANT_ROLES.map((role) => grantFieldset(role, d, field, line, "destination")).join("") +
    "</fieldset>" +
    "<div class=\"field\"><label for=\"destination-write-probe\">write probe</label>" +
    "<select id=\"destination-write-probe\" name=\"writeProbe\">" +
    "<option value=\"createOnlyMarker\"" +
    (d.writeProbe !== "disabled" ? " selected" : "") + ">createOnlyMarker</option>" +
    "<option value=\"disabled\"" + (d.writeProbe === "disabled" ? " selected" : "") +
    ">disabled</option></select>" +
    "<p class=\"help\">" + esc(WRITE_PROBE_SENTENCE) + "</p></div>" +
    "<label class=\"inline\" for=\"destination-default\">" +
    "<input type=\"checkbox\" id=\"destination-default\" name=\"isDefault\"" +
    (d.isDefault === true ? " checked" : "") + "> make this the namespace default</label>" +
    // MCP-11: the default-destination paragraph is said once, beside the
    // checkbox it is about, and no longer a second time under the list.
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
  const draft = Object.assign({}, DESTINATION_DEFAULTS, rotate.draft || {});
  return (
    "<section class=\"rotate\" id=\"destination-rotate\"><h3>Rotate access</h3>" +
    "<p class=\"note\">This sends the COMPLETE four-grant block: a grant left at " +
    "<code>absent</code> here is REMOVED from the destination. It carries the generation this " +
    "page read (<code id=\"rotate-generation\">" + cell(d.generation) + "</code>); if the object " +
    "moved since, the answer is a 412 and nothing is written.</p>" +
    "<p class=\"note\">The location and the transport security are immutable. Only the " +
    "credentials and the CA reference can change here.</p>" +
    "<form id=\"destination-rotate-form\" novalidate" + (pending ? " aria-busy=\"true\"" : "") + ">" +
    "<fieldset class=\"form-body\"" + (pending ? " disabled" : "") + ">" +
    "<input type=\"hidden\" name=\"expectedGeneration\" id=\"rotate-expected-generation\" value=\"" +
    esc(String(d.generation === undefined || d.generation === null ? "" : d.generation)) + "\">" +
    GRANT_ROLES.map((role) => grantFieldset(role, draft, field, line, "rotate")).join("") +
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

/** The "adopt a legacy archive" form. */
export function renderLegacyForm(view) {
  const v = view || {};
  const d = v.draft || {};
  const state = v.state || {};
  const pending = state.phase === "pending";
  return (
    "<section class=\"from-legacy\" id=\"destination-from-legacy\">" +
    "<h3>Adopt an existing archive location</h3>" +
    "<p class=\"note\">" + esc(FROM_LEGACY_SENTENCE) + "</p>" +
    "<form id=\"destination-legacy-form\" novalidate" + (pending ? " aria-busy=\"true\"" : "") + ">" +
    "<fieldset class=\"form-body\"" + (pending ? " disabled" : "") + ">" +
    "<div class=\"field-row\">" +
    "<div class=\"field\"><label for=\"legacy-name\">new destination name</label>" +
    "<input id=\"legacy-name\" name=\"name\" required value=\"" + esc(String(d.name || "")) +
    "\"></div>" +
    "<div class=\"field\"><label for=\"legacy-schedule\">derive from BackupSchedule</label>" +
    "<input id=\"legacy-schedule\" name=\"sourceSchedule\" value=\"" +
    esc(String(d.sourceSchedule || "")) + "\"></div>" +
    "<div class=\"field\"><label for=\"legacy-backup\">or from Backup</label>" +
    "<input id=\"legacy-backup\" name=\"sourceBackup\" value=\"" +
    esc(String(d.sourceBackup || "")) + "\"></div>" +
    "</div>" +
    "<div class=\"field\"><label for=\"legacy-secret\">archiveWrite: existing Secret name</label>" +
    "<input id=\"legacy-secret\" name=\"archiveWriteSecret\" value=\"" +
    esc(String(d.archiveWriteSecret || "")) + "\">" +
    "<p class=\"help\">Adoption derives the LOCATION from facts; the credential is still named " +
    "here, because a legacy object references a Secret and this service cannot read it to " +
    "confirm which keys it holds.</p></div>" +
    "<div class=\"actions\"><button type=\"submit\">Adopt</button></div>" +
    "</fieldset>" +
    "<div class=\"form-status\" id=\"destination-legacy-status\" tabindex=\"-1\">" +
    (state.phase === "failed" ? renderLegacyRefusal(state.error) : "") +
    mutationStatus(state, { kind: "BackupDestination", name: String(d.name || "") }, null) +
    "</div></form></section>"
  );
}

/** A `:from-legacy` refusal, rendered as WHAT IT IS.
 *
 *  `legacy_location_unknown` is a 404, and a 404 rendered as "not found" would
 *  read as "that schedule does not exist" -- which is a different fact and
 *  usually a false one. The schedule exists; what could not be read is a
 *  LOCATION for it, and the service refuses to guess one. So the code is named
 *  and the server's own sentence is rendered verbatim beneath it. */
export function renderLegacyRefusal(error) {
  const e = error || {};
  if (e.code !== "legacy_location_unknown") {
    return "";
  }
  return (
    "<div class=\"refusal\" id=\"legacy-location-unknown\" role=\"status\">" +
    badge("unverified", "legacy_location_unknown") +
    "<p class=\"note\">The object was found. What could not be read is an archive LOCATION for " +
    "it -- no frozen execution input from a succeeded run, and no usable archive URL on the " +
    "spec. This service refuses to derive a destination from a guess, so nothing was created. " +
    "Create the destination explicitly with the form above.</p>" +
    "<p class=\"detail\">" + cell(e.message) + "</p>" +
    "</div>"
  );
}

// ---------------------------------------------------------------- the DOM half

/** Reads the create form's values. The credential inputs ARE read here --
 *  they have to be, they are what the request carries -- and the returned
 *  object goes to `destinationBody` and to `keepDraft`, whose allowlist drops
 *  them. Two callers, one of which forgets. */
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
  "access.archiveRead.mode": "archiveReadSource",
  "access.archiveRead.secret.existing.name": "archiveReadSecret",
  "access.evidenceWrite.mode": "evidenceWriteSource",
  "access.evidenceWrite.secret.existing.name": "evidenceWriteSecret",
  "access.evidenceRead.mode": "evidenceReadSource",
  "access.evidenceRead.secret.existing.name": "evidenceReadSecret",
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
export async function submitRotation(ns, name, values, generation, deps) {
  const problems = validateGrants(values);
  if (Object.keys(problems).length > 0) {
    throw invalidInput(problems);
  }
  return (deps || API).updateDestinationAccess(ns, name, rotationBody(values, generation));
}

// --------------------------------------------------------------- mount half

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
              renderLegacyForm(viewFor(ns, LEGACY_FORM)) +
              "</div>"
            : "<p class=\"note\" id=\"destinations-read-only\">This login may read " +
              "destinations in this namespace and not create them.</p>"),
      ),
    );
    if (may) {
      wireCreate(node, ns, parse, lifecycle, api);
      wireLegacy(node, ns, parse, lifecycle, api);
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
    // THE USAGE READ IS SEPARATE AND ITS FAILURE IS NOT THE PAGE'S. A
    // destination whose `/usage` route answers 403 still renders; what the
    // reader loses is one section, and that section says so.
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
    });
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
    testViews.set(testKey, { test: given.test, testStopped: given.testStopped === true });
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
    {
      test: given.test || remembered.test || null,
      testStopped: given.test ? given.testStopped === true : remembered.testStopped === true,
    },
  );
}

function paintDetail(node, ns, name, parse, lifecycle, api, item, extra) {
  const view = detailView(ns, name, extra);
  replace(node, parse(renderDestinationDetail(item, view)));
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
  replace(slot, parse(renderTestSlot(item, view)));
  wireTest(node, ns, name, parse, lifecycle, api, item, view);
}

/** A source selector shows its own inputs and hides the rest, as the reader
 *  changes it (MCP-10). Every grant fieldset on the page, create and rotate
 *  alike: each `<fieldset class="grant">` holds one selector and its groups. */
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
    // THE ALLOWLIST IS WHAT DROPS THE CREDENTIAL, and it is applied HERE, on
    // the values the form just produced -- which do carry one. Nothing between
    // `readDestinationValues` and this call holds them.
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
    replace(slot, parse(
      renderDestinationForm(viewFor(ns, CREATE_FORM)) + renderLegacyForm(viewFor(ns, LEGACY_FORM)),
    ));
    wireCreate(node, ns, parse, lifecycle, api);
    wireLegacy(node, ns, parse, lifecycle, api);
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

function wireLegacy(node, ns, parse, lifecycle, api) {
  const form = node.querySelector("#destination-legacy-form");
  if (form === null) {
    return;
  }
  const key = formKey(ns, LEGACY_FORM);
  const mutation = mutationFor(key);
  listen(form, "input", () => {
    if (active(lifecycle)) {
      keepDraft(key, readDestinationValues(form),
        ["name", "sourceSchedule", "sourceBackup", "archiveWriteSecret"]);
    }
  }, lifecycle);
  watchMutation(node, key, mutation, (state) => {
    if (state.phase === "succeeded") {
      dropDraft(key);
      mountDestinations(node, ns, parse, lifecycle, api);
      return;
    }
    const slot = node.querySelector("#destination-form-slot");
    if (slot !== null) {
      replace(slot, parse(
        renderDestinationForm(viewFor(ns, CREATE_FORM)) + renderLegacyForm(viewFor(ns, LEGACY_FORM)),
      ));
      wireCreate(node, ns, parse, lifecycle, api);
      wireLegacy(node, ns, parse, lifecycle, api);
    }
  }, lifecycle);
  listen(form, "submit", (event) => {
    event.preventDefault();
    if (!active(lifecycle) || mutation.pending()) {
      return;
    }
    const values = readDestinationValues(form);
    const body = { name: String(values.name || "").trim(), access: {} };
    const secret = String(values.archiveWriteSecret || "").trim();
    body.access.archiveWrite = secret.length > 0
      ? { mode: "secretKeys", secret: { existing: { name: secret } } }
      : { mode: "workloadIdentity" };
    const schedule = String(values.sourceSchedule || "").trim();
    const backup = String(values.sourceBackup || "").trim();
    if (schedule.length > 0) {
      body.sourceSchedule = schedule;
    }
    if (backup.length > 0) {
      body.sourceBackup = backup;
    }
    mutation.run(() => api.destinationFromLegacy(ns, body));
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
      const extra = { usage: view.usage, usageError: view.usageError, test: made, testStopped: false };
      paintTest(node, ns, name, parse, lifecycle, api, item, extra);
      followDestinationTest(node, ns, name, parse, lifecycle, api, item, view, made);
      return;
    }
    paintTest(node, ns, name, parse, lifecycle, api, item, {
      usage: view.usage, usageError: view.usageError,
    });
  }, lifecycle);
  listen(form, "submit", (event) => {
    event.preventDefault();
    if (!active(lifecycle) || mutation.pending()) {
      return;
    }
    const values = readDestinationValues(form);
    const roles = Array.isArray(values.roles) ? values.roles : [];
    // THE TOKEN IS MINTED INSIDE THE EXECUTOR, as the connection check's is,
    // so a platform with no random source is a refusal in this form's status
    // region rather than an exception out of an event handler.
    mutation.run(() => api.testDestination(ns, name, roles.length > 0 ? { roles: roles } : {}, {
      attempt: nextDestinationTestAttempt(ns, name),
    }));
  }, lifecycle);
}

/** Re-reads the test this page started until a READ answers terminal, or the
 *  budget is spent -- defect P8.
 *
 *  THE CREATE ANSWER IS NOT A READ (`owesRead`): even a terminal one, which
 *  is a replay, was projected without recomputing staleness. Every read is
 *  guarded by the route (PLAT-13.1), the loop is bounded, a failed read is not
 *  a verdict (the rows on screen stay what the check last recorded and the
 *  panel says it stopped), and an older follower stops as soon as a newer test
 *  is the one on screen. */
async function followDestinationTest(node, ns, name, parse, lifecycle, api, item, view, first) {
  const key = formKey(ns, TEST_FORM, name);
  const wait = typeof api.wait === "function"
    ? api.wait
    : (ms) => new Promise((done) => { globalThis.setTimeout(done, ms); });
  let current = first;
  const mine = () => ((testViews.get(key) || {}).test || {}).id === (first || {}).id;
  const paint = (stopped) => {
    if (active(lifecycle) && mine()) {
      paintTest(node, ns, name, parse, lifecycle, api, item, {
        usage: view.usage, usageError: view.usageError, test: current, testStopped: stopped,
      });
    }
  };
  for (let read = 0; read < DESTINATION_TEST_POLLS; read += 1) {
    if (!owesRead(current, read)) {
      return;
    }
    await wait(DESTINATION_TEST_INTERVAL_MS);
    if (!active(lifecycle) || !mine()) {
      return;
    }
    let answer;
    try {
      answer = await api.preflight(ns, current.id, readOptions(lifecycle));
    } catch (error) {
      if (!cancelled(error, lifecycle)) {
        paint(true);
      }
      return;
    }
    if (!active(lifecycle) || !mine()) {
      return;
    }
    current = ((answer || {}).item) || current;
    paint(false);
    if (current.terminal === true) {
      return;
    }
  }
  paint(true);
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
    mutation.run(() => submitRotation(ns, name, values, item.generation, api));
  }, lifecycle);
}
