// pages/clusters.js -- the KafkaCluster list, one cluster's detail, and the
// create form.
//
// THE RENDER HALF IS PURE. Every `render*` function below is a function from a
// JSON object -- one the API server could have returned, which is what the
// checked-in fixtures are -- to an HTML string. No DOM, no network, no clock.
// `ui/tests/pages.spec.js` asserts the rules on those strings and
// `scripts/check-ui-behaviour.sh` runs it on every `just lint`.
//
// THE MOUNT HALF IS GLUE. `mountClusters` reads through `api.js` -- the only
// module in this tree that issues a network request -- and hands the string to
// the shell, which parses it. It holds no rule of its own.
//
// WHAT THE `AUTH` COLUMN MAY SAY. The mode, the username, and the NAME of the
// Secret that carries the credential. A Secret's name is not a secret; its
// value never appears in a status field, a log line, a rendered document or
// this page, and `no_secret_value_is_rendered` asserts the rendered output for
// a SCRAM cluster names the Secret and carries no credential word at all.
//
// THE CREATE FORM KEEPS WHAT WAS TYPED (PLAT-13.2). Its values are an
// in-memory draft (`../lifecycle.js`), so a refused field, an API refusal, a
// lost response or a trip to another route leaves them in place. The create is
// idempotent by name: submitting the same draft again after an unknown outcome
// resolves to the object the first request made, and a different object under
// that name is reported as a conflict and never touched.
//
// THE FORM TAKES CONTRACT v1's REFERENCES AND NEVER A VALUE (PLAT-07.1,
// PLAT-07.2). `spec.auth.secretRef.passwordKey` names the data key inside the
// Secret that holds the SASL password -- absent means `password`, which is what
// every earlier release projected -- and `spec.auth.tlsCa` names exactly one
// key of a Secret or a ConfigMap holding the private CA that signs the brokers'
// certificates. Both are NAMES. There is no field on this form a password could
// be typed into, `CLUSTER_DRAFT_FIELDS` names none, and
// `the_cluster_form_has_no_password_input` asserts both against the rendered
// bytes so a field added later cannot quietly become one.
//
// AND `status.reachable` IS A CONNECTION PROBE, NEVER "READY" (PLAT-07.2, D2
// section 9). The badge, the stale label and the controller's own refusal
// reason all come from `../select.js`, which is also what the schedule form and
// the restore wizard select a saved connection with -- so the three surfaces
// agree about what an observation means and about what is too old to present as
// current.

import { CONSOLE, LEGACY, apiClient, mayOperate, mode } from "../client.js";
import {
  active,
  askCheck,
  cancelled,
  createOnce,
  DISCOVERY_FOLLOW_MS,
  discoverySpent,
  dropDraft,
  fieldErrors,
  followCheck,
  followStopped,
  formKey,
  invalidInput,
  isFollowed,
  keepDraft,
  listen,
  mutationFor,
  PREFLIGHT_FOLLOW_MS,
  readDraft,
  readOptions,
  refusal,
  watchMutation,
  wireCheckRetry,
} from "../lifecycle.js";
import {
  ABSENT,
  EMPTY_INVENTORY_SENTENCE,
  badge,
  checkStoppedBlock,
  disableKeepingFocus,
  cell,
  detailLink,
  errorBlock,
  errorBox,
  esc,
  facts,
  fieldErrorLine,
  invalidAttributes,
  listFooter,
  messageText,
  mutationStatus,
  phaseBadge,
  preflightVerdict,
  replace,
  table,
  visibilityLine,
  when,
} from "../render.js";
import {
  CONNECTION_REFUSAL_REASONS,
  laterProbeNote,
  probeSummary,
  PROBE_SENTENCE,
  REFUSAL_GLOSS,
  TEST_CONNECTION_SENTENCE,
  clusterUid,
  probeBadge,
  probeLine,
  probeState,
  renderProbePanel,
  renderTestConnection,
  staleBadge,
} from "../select.js";
import { renderPreflight } from "./destinations.js";

const PLURAL = "kafkaclusters";

const API = apiClient();

/** The form's identity in the draft and mutation registries. */
export const CLUSTER_FORM = "cluster-form";

/** The fields a draft of this form keeps. All of them are public connection
 *  settings or the NAME of a Secret; the form has no field a credential could
 *  be typed into, and a field added later is not kept unless it is named here. */
export const CLUSTER_DRAFT_FIELDS = Object.freeze([
  "name", "servers", "role", "mode", "username", "secret", "passwordKey",
  "tls", "tlsCaKind", "tlsCaName", "tlsCaKey", "intent",
]);

// ------------------------------------------------ who names the connection
//
// DEFECT P7 (poc-install, 2026-09-24). In console (shared) mode this form
// offered a required "name" field and the product API threw it away: the
// create route has no name member (`CreateConnectionRequest` is `role`,
// `bootstrapServers`, `auth`, `markerTopic`, `additionalProperties: false`)
// and mints `conn-<26 base32>` from the request's idempotency scope, as
// `docs/api.md` says of every create it serves. An operator who typed
// `source` got `conn-u5cb27icdjt5vdf6sc4wehb4nb` and was told nothing.
//
// The API contract is the one that holds (it names schedules `sch-...`,
// restores `rst-...` and runs `logweir-manual-...` the same way), so the console
// stops offering a choice it cannot honour. What the typed name used to do --
// make a double click, a lost response or a retry ONE connection -- is done by
// an INTENT held in the draft, exactly as "Back up now" holds its own
// (`schedules.js`'s `RUN_NOW_DRAFT_FIELDS`): random, minted once per draft,
// sent as the idempotency seed and never as a name.

/** Whether the server, not this form, names a new connection: console mode. */
export function connectionNamesMinted() {
  return mode() === CONSOLE;
}

/** What the form says where the name field would be, in console mode. */
export const CONNECTION_NAME_MINTED_SENTENCE =
  "Through the product API (console mode) a connection is named by the server: conn- followed " +
  "by 26 characters, derived from this request's idempotency key. There is no name to choose " +
  "here; the created name is shown below once the connection exists, and the role is how the " +
  "list, the schedule form and the restore wizard tell a source from a target.";

/** A fresh intent for one connection draft: `logweir-ui.connection.` plus 32
 *  random hex characters, inside the product API's 8-to-128 key budget.
 *
 *  A PLATFORM WITH NO RANDOM SOURCE GETS A REFUSAL, not a counter: a counter
 *  restarts on every load and would replay one load's create in the next --
 *  review F1/F3's defect. Called inside the mutation's executor, so the
 *  refusal lands in the form's status region. */
export function mintConnectionIntent() {
  const source = globalThis.crypto;
  if (source === undefined || source === null || typeof source.getRandomValues !== "function") {
    throw refusal(
      "this page will not create a connection here: minting the idempotency key that makes a " +
        "double click or a retry ONE connection needs the platform's random source, and it is " +
        "unavailable.",
    );
  }
  let hex = "";
  for (const byte of source.getRandomValues(new Uint8Array(16))) {
    hex += byte.toString(16).padStart(2, "0");
  }
  return "logweir-ui.connection." + hex;
}

/** The credential-shaped field names this form must NEVER have, held as data
 *  so `the_cluster_form_has_no_password_input` can assert the rendered bytes
 *  and `CLUSTER_DRAFT_FIELDS` against the same list rather than against a
 *  spelling inside one test. `passwordKey` is the NAME OF A DATA KEY and is
 *  deliberately not on it; a key's name is not a secret, and contract v1 needs
 *  it to project the right entry of the Secret. */
export const FORBIDDEN_CLUSTER_FIELDS = Object.freeze([
  "password", "secretValue", "credential", "passphrase", "token",
]);

/** The API server's field paths, mapped to this form's inputs, so a 422's
 *  `causes[]` lands beside the field it is about. */
export const CLUSTER_FIELD_PATHS = Object.freeze([
  ["metadata.name", "name"],
  ["spec.bootstrapServers", "servers"],
  ["spec.role", "role"],
  ["spec.auth.mode", "mode"],
  ["spec.auth.username", "username"],
  ["spec.auth.secretRef.passwordKey", "passwordKey"],
  ["spec.auth.secretRef", "secret"],
  ["spec.auth.tlsCa.secretKeyRef.key", "tlsCaKey"],
  ["spec.auth.tlsCa.configMapKeyRef.key", "tlsCaKey"],
  ["spec.auth.tlsCa.secretKeyRef.name", "tlsCaName"],
  ["spec.auth.tlsCa.configMapKeyRef.name", "tlsCaName"],
  ["spec.auth.tlsCa", "tlsCaName"],
  ["spec.auth.tls", "tls"],
]);

/** What the API server fills in when a create omits it: the CRD defaults
 *  `auth.tls` to false. Nothing else of a KafkaCluster's spec may change. */
const CLUSTER_SPEC_RULES = Object.freeze({ defaults: { "auth.tls": false } });

/** The objects a renderer was handed, whatever shape they arrived in: a
 *  `KafkaClusterList` from the API server, a bare array, or one object (which
 *  is what a checked-in fixture is). */
export function itemsOf(input) {
  if (input === null || input === undefined) {
    return [];
  }
  if (Array.isArray(input)) {
    return input;
  }
  if (Array.isArray(input.items)) {
    return input.items;
  }
  return [input];
}

/** A `metadata.name`, or the absent marker. */
function nameOf(object) {
  const meta = (object && object.metadata) || {};
  return cell(meta.name);
}

/** The name as a link to this object's detail view, when it has a name. */
function nameCell(object, ns) {
  const meta = (object && object.metadata) || {};
  if (typeof meta.name !== "string" || meta.name.length === 0) {
    return cell(null);
  }
  return detailLink("clusters", ns || (meta.namespace || "default"), meta.name);
}

/** `spec.auth` as one cell: the mode, the username, and the Secret's NAME.
 *  Never a credential value -- there is none on this object to render, in any
 *  mode, by construction (`crates/weirkeeper/src/crds/kafka_cluster.rs`). */
export function authCell(spec) {
  const auth = (spec && spec.auth) || {};
  const parts = [cell(auth.mode)];
  if (typeof auth.username === "string" && auth.username.length > 0) {
    parts.push("as " + esc(auth.username));
  }
  const ref = auth.secretRef || {};
  if (typeof ref.name === "string" && ref.name.length > 0) {
    parts.push("via Secret " + esc(ref.name));
    // CONTRACT v1's KEY, and only when it was set. An absent key means the
    // legacy entry and rendering a default here would claim the object says
    // something it does not. The key's NAME is not a secret; its value is
    // never read by this page, by a status field or by a rendered document.
    if (typeof ref.passwordKey === "string" && ref.passwordKey.length > 0) {
      parts.push("key " + esc(ref.passwordKey));
    }
  }
  parts.push(auth.tls === true ? "TLS" : "no TLS");
  parts.push(caWords(auth.tlsCa));
  return parts.filter((part) => part.length > 0).join(" ");
}

/** Contract v1's `spec.auth.tlsCa` as words: which kind of object holds the
 *  private CA, its name and its key. The empty string when none is named,
 *  which is a connection that trusts the runner image's own store. */
export function caWords(tlsCa) {
  const ca = tlsCa || {};
  const secret = ca.secretKeyRef || null;
  const configMap = ca.configMapKeyRef || null;
  const source = secret !== null ? secret : configMap;
  if (source === null) {
    return "";
  }
  return (
    "CA from " + (secret !== null ? "Secret " : "ConfigMap ") + cell(source.name) +
    " key " + cell(source.key)
  );
}

/** `status.reachable` AS A CONNECTION PROBE, in one badge whose words carry
 *  the state as well as its colour.
 *
 *  It takes a `status` rather than a whole object because that is what its
 *  callers had before PLAT-07.2 and what the design suite exercises it with;
 *  the judgement itself lives in `../select.js`, so this page, the schedule
 *  form and both wizard sides say the same words about the same field. An
 *  unset `reachable` with no reason at all is `never probed` -- not the absent
 *  marker and not a claim either way, because "the controller has not written
 *  anything yet" is itself worth saying. */
export function reachableBadge(status, now, freshSeconds) {
  return probeBadge(probeState({ status: status || {} }, now, freshSeconds));
}

/** The probe as a table cell: the verdict badge, the stale badge beside it
 *  when the observation is older than the freshness budget, and nothing else
 *  -- the reason and the observed instant have their own columns. */
export function probeCell(object, now, freshSeconds) {
  const state = probeState(object, now, freshSeconds);
  const stale = staleBadge(state);
  return probeBadge(state) + (stale.length > 0 ? " " + stale : "");
}

/** The newest probe's own reason, when it differs from the verdict it sits
 *  beside (MCP-9): its code visible and its meaning one disclosure away. */
export function laterProbeDisclosure(state) {
  const note = laterProbeNote(state);
  if (note.length === 0) {
    return "";
  }
  const code = note.slice(0, note.indexOf("</code>") + "</code>".length);
  const gloss = note.slice(code.length);
  return "<details class=\"cell-more\"><summary>" + code + "</summary>" + gloss + "</details>";
}

/** The sentence the clusters table carries when the namespace holds none. */
export const NO_CLUSTER_SENTENCE =
  "No KafkaCluster in this namespace yet. Create one with the form below, or pick another " +
  "namespace above.";

/** The clusters table. NAME (with the role beneath it), CONNECTION PROBE
 *  (verdict, staleness, age and the newest probe's own reason), CLUSTER-ID,
 *  AUTH, and one Re-read probe control per row.
 *
 *  THE PROBE COLUMN IS CALLED WHAT IT IS. It was headed REACHABLE and rendered
 *  a bare `reachable` badge with the observed instant two columns away, so a
 *  five-hour-old success and a five-second-old one looked identical at a
 *  glance. The column is now the probe's verdict, the stale label when the
 *  observation is past the freshness budget, and the controller's own reason
 *  beside it -- and the word `ready` appears nowhere, because nothing on this
 *  page has decided that anything is.
 *
 *  `now` is epoch milliseconds, defaulted to the caller's clock by
 *  `probeState`; a test passes one so a freshness verdict is reproducible. */
export function renderClusterList(input, ns, now, freshSeconds) {
  // FIVE COLUMNS, NOT EIGHT (MCP round 2, R2-2 / R2-3 / R2-4). The table was
  // wider than its card at 1440 px -- the Re-read probe button was clipped --
  // and a NoExitCode gloss made a row 220 px tall in a narrow REASON column.
  // The role is the name's second line, the observation's age sits in the
  // probe cell as it does in the wizard (MCP-28), and the newest probe's own
  // reason is one disclosure away, its code still visible to grep for.
  const rows = itemsOf(input).map((object) => {
    const spec = object.spec || {};
    const status = object.status || {};
    const state = probeState(object, now, freshSeconds);
    return [
      nameCell(object, ns) + "<span class=\"cell-sub\">" + cell(spec.role) + "</span>",
      "<div class=\"probe-cell\">" + probeSummary(state) + laterProbeDisclosure(state) + "</div>",
      cell(status.clusterId),
      authCell(spec),
      renderTestConnection(object, false),
    ];
  });
  const attributes = itemsOf(input).map(
    (object) => "data-cluster-uid=\"" + esc(clusterUid(object)) + "\"",
  );
  return (
    "<h2>Clusters</h2>" +
    "<p class=\"blurb\">Every KafkaCluster in this namespace. " +
    "<code>clusterId</code> is read from the broker and never from a spec.</p>" +
    "<p class=\"note\">" + PROBE_SENTENCE + "</p>" +
    table(
      ["NAME", "CONNECTION PROBE", "CLUSTER-ID", "AUTH", ""],
      rows,
      NO_CLUSTER_SENTENCE,
      attributes,
      { id: "clusters", label: "connections" },
    ) +
    "<p class=\"note\">" + TEST_CONNECTION_SENTENCE + "</p>" +
    listFooter()
  );
}

/** One cluster, in full: its probe panel with the re-read control, the Test
 *  connection panel that really dials, and then the saved connection contract
 *  v1 carries -- every reference by name, no value of anything. */
export function renderClusterDetail(object, now, freshSeconds, pending, discovery, check) {
  const spec = (object && object.spec) || {};
  const status = (object && object.status) || {};
  const servers = Array.isArray(spec.bootstrapServers) ? spec.bootstrapServers : [];
  const auth = spec.auth || {};
  const ref = auth.secretRef || {};
  const state = probeState(object, now, freshSeconds);
  return (
    "<h2>Cluster " + nameOf(object) + "</h2>" +
    renderProbePanel(object, { now: now, freshSeconds: freshSeconds, pending: pending === true }) +
    facts([
      ["role", cell(spec.role)],
      ["bootstrap servers", servers.length === 0 ? cell(null) : esc(servers.join(", "))],
      ["marker topic", cell(spec.markerTopic)],
      ["auth", authCell(spec)],
      ["credential Secret", cell(ref.name)],
      ["credential key", typeof ref.passwordKey === "string" && ref.passwordKey.length > 0
        ? esc(ref.passwordKey)
        : (typeof ref.name === "string" && ref.name.length > 0
          ? "- (absent means the key every earlier release projected)"
          : cell(null))],
      ["TLS", auth.tls === true ? "on" : "off"],
      ["private CA", caWords(auth.tlsCa).length === 0 ? cell(null) : caWords(auth.tlsCa)],
      ["cluster id", cell(status.clusterId)],
      ["probe observed at", when(status.observedAt)],
      ["probe reason", cell(status.reason)],
      ["probe freshness", esc(String(state.freshSeconds)) + "s budget"],
      ["uid", "<code id=\"cluster-uid\">" + esc(clusterUid(object)) + "</code>"],
    ]) +
    // THE CONNECTION CHECK IS PART OF THE DETAIL FOR THE SAME REASON THE
    // DISCOVERY PANEL IS: what a `connection.authenticated` row is about --
    // which brokers, which principal, which recorded cluster id -- is the
    // block directly above it.
    (check === undefined || check === null ? "" : renderConnectionCheck(check)) +
    // THE DISCOVERY PANEL IS PART OF THE DETAIL AND NOT A SECOND VIEW, because
    // what it is about -- which principal, which cluster id, how fresh -- is
    // the block directly above it, and a reader who had to change routes to
    // compare them would be comparing from memory.
    (discovery === undefined || discovery === null ? "" : renderDiscoveryPanel(discovery))
  );
}

// ===========================================================================
// THE "TEST CONNECTION" PANEL (PLAT-03.1's check kind, PLAT-07.2's control)
// ===========================================================================
//
// WHAT IT DOES NOW. It creates a `Preflight` of operation `sourceConnection`:
// the controller resolves this `KafkaCluster`, renders a check plan carrying
// that connection and nothing else, and runs ONE isolated Job that projects
// this connection's own credential and dials the brokers. What comes back is
// `connection.resolved`, `connection.credentialProjected`,
// `connection.authenticated`, `connection.clusterIdentity`, `runner.*` and
// `configuration.policy`, each with its own state, closed-vocabulary code,
// remedy and the instant it was observed. None of it is computed here.
//
// AND WHAT IT STILL IS NOT. A `ready` verdict authorises nothing (D2 section
// 6.8) and it is a statement about the moment the Job ran, which is why every
// row carries its own `observedAt` and `expiresAt` and why they are rendered.
// The connection probe panel above is a different fact: the controller's own
// cadence, recorded minutes ago.
//
// AND IT ASKS NOTHING ABOUT TOPICS. `connection.topicsDescribable` is about
// NAMED topics -- `TopicNotFound` and `TopicNotAuthorized` are facts about a
// name the requester chose -- and this request names none, so the runner emits
// no such row and this panel claims none. "Discover topics" below is the
// control that answers what this principal can see.

/** The identity of the connection check in the mutation registry. */
export const CONNECTION_CHECK_FORM = "connection-check";

/** What the control does, in the control's own words. */
export const CONNECTION_CHECK_SENTENCE =
  "Test connection starts a Preflight: an isolated Job that projects this connection's own " +
  "credential and dials these brokers now. The rows below are that Job's own recorded result, " +
  "each with the instant it was observed. It is not a promise about the next run, and a ready " +
  "verdict authorises nothing: every execution-time guard still runs.";

/** What the panel says instead of a topic claim. */
export const CONNECTION_CHECK_NO_TOPICS_SENTENCE =
  "This check names no topic, so it reports nothing about which topics this principal can " +
  "describe. That is what Discover topics below is for.";

/** What a controller refusal means for this control.
 *
 *  THE CONTROL IS DISABLED AND THE CONTROLLER'S OWN REASON IS PRINTED. A
 *  `KafkaCluster` the resolver has refused has no renderable credential, so the
 *  Preflight would be created, fail to render a check plan and land on
 *  `phase: Failed` with no row at all -- a worse answer than this one, and one
 *  that costs a Job. */
export const CONNECTION_CHECK_REFUSED_SENTENCE =
  "The controller has refused this connection, so there is nothing to dial with: a check would " +
  "be created, fail to render a plan and record no row. Fix the object first; the controller's " +
  "own reason is beside this sentence.";

/** Why a `kubectl proxy` console has no control here.
 *
 *  REVIEW F6. `renderConnectionCheck` had an `unavailable` branch that nothing
 *  ever set, which is a dead state in a panel whose whole job is to say what
 *  is true. There IS a real answer for it: the check routes are the product
 *  API's, the legacy proxy serves read-only summaries of the three D2 kinds
 *  (D2 section 7.4) and creates none of them, so in that mode the control
 *  could only fail at the click. Saying so up front is what the discovery
 *  panel beside it already does. */
export const CONNECTION_CHECK_LEGACY_SENTENCE =
  "This console is reading through a kubectl proxy, which serves read-only summaries of check " +
  "requests and creates none. A connection check needs the product API; the connection probe " +
  "above is what this mode can show.";

/** The controller's refusal reason for this connection, or `""`.
 *
 *  READ FROM `status.reason` AND MATCHED AGAINST THE RESOLVER'S OWN LIST. A
 *  reason this build does not know is NOT treated as a refusal: the control
 *  stays enabled and the check itself reports what it finds, which is the
 *  failing-open direction that costs a Job rather than the one that hides a
 *  control an operator needs. */
export function connectionRefusal(object) {
  const status = (object && object.status) || {};
  const reason = typeof status.reason === "string" ? status.reason : "";
  return CONNECTION_REFUSAL_REASONS.includes(reason) ? reason : "";
}

/** The body `POST .../preflights` is given for one cluster.
 *
 *  EXPORTED SO THE SUITE CAN ASSERT THE SHAPE the product API's own contract
 *  declares: the operation, one block, one reference, and no field this check
 *  does not ask about. `CreatePreflightRequest` is `deny_unknown_fields`, so a
 *  destination or a topic list smuggled in here is a 422 and not a wider
 *  check. */
export function connectionCheckRequest(name) {
  return { operation: "sourceConnection", sourceConnection: { connectionRef: String(name) } };
}

/** What a reload can still say about the last check.
 *
 *  A CONNECTIVITY CHECK OUTLIVES THE PAGE THAT STARTED IT. The panel's rows
 *  live in the loaded page; the `Preflight` lives in the namespace until the
 *  collector takes it. Until the API labelled these objects, a reload found
 *  nothing at all and the panel read as though no check had ever run, which
 *  invited a second one for an answer that already existed.
 *
 *  IT IS A SUMMARY AND NEVER THE ROWS. `lastTest` carries the id, the
 *  aggregate, the instant and whether the verdict still applies -- no codes
 *  and no remedies -- so it says what happened and points at the check rather
 *  than reprinting a verdict the reader cannot act on from here. A STALE test
 *  is labelled and never rendered as health: it is the same rule the probe
 *  badge keeps one panel above. */
export function renderLastConnectionCheck(test) {
  const t = test || null;
  if (t === null) {
    return "<p class=\"note\" id=\"connection-check-last-none\">No connectivity check for this " +
      "connection is recorded in this namespace. That is a statement about what is stored, not " +
      "about the connection.</p>";
  }
  return (
    "<div class=\"last-test\" id=\"connection-check-last\">" +
    "<p>Last connectivity check: " + preflightVerdict(t.state) +
    " <code>" + cell(t.preflightId) + "</code>, observed " + when(t.observedAt) +
    (t.stale === true
      ? " " + badge("unverified", "stale: this verdict no longer describes the object as it is")
      : "") +
    ".</p>" +
    (t.truncated === true
      ? "<p class=\"note\">The search for the newest check hit its page bound, so this may not " +
        "be the newest one. A last test that might not be last is worse than none, so it is " +
        "labelled rather than presented as current.</p>"
      : "") +
    "</div>"
  );
}

/** The panel: the control, the reason it is unavailable when it is, the last
 *  check this namespace still holds, and the started check's own rows. */
export function renderConnectionCheck(view) {
  const v = view || {};
  const state = v.state || {};
  const pending = state.phase === "pending";
  const result = v.preflight || null;
  const refused = typeof v.refusedReason === "string" ? v.refusedReason : "";
  const gloss = REFUSAL_GLOSS[refused];
  return (
    "<section class=\"connection-check\" id=\"connection-check\"><h3>Test connection</h3>" +
    "<p class=\"note\">" + esc(CONNECTION_CHECK_SENTENCE) + "</p>" +
    "<p class=\"note\" id=\"connection-check-no-topics\">" +
    esc(CONNECTION_CHECK_NO_TOPICS_SENTENCE) + "</p>" +
    (v.unavailable === true
      ? "<p class=\"note\" id=\"connection-check-unavailable\">" + messageText(v.unavailableReason) +
        "</p>"
      : (v.mayOperate === false
        ? "<p class=\"note\" id=\"connection-check-forbidden\">This login may read connection " +
          "checks in this namespace and not start one.</p>"
        : "<form id=\"connection-check-form\" novalidate" +
          (pending ? " aria-busy=\"true\"" : "") + ">" +
          "<fieldset class=\"form-body\"" +
          (pending || refused.length > 0 ? " disabled" : "") + ">" +
          "<div class=\"actions\"><button type=\"submit\"" +
          (pending || refused.length > 0 ? " disabled" : "") + ">Test connection</button>" +
          "</div></fieldset>" +
          (refused.length === 0
            ? ""
            : "<p class=\"refusal\" id=\"connection-check-refused\">" +
              esc(CONNECTION_CHECK_REFUSED_SENTENCE) + " Controller reason: <code>" +
              esc(refused) + "</code>" +
              (gloss === undefined ? "" : " -- " + esc(gloss)) + ".</p>") +
          "<div class=\"form-status\" id=\"connection-check-status\" tabindex=\"-1\">" +
          mutationStatus(state, { kind: "Preflight", name: (result || {}).id || "" }, null) +
          "</div></form>")) +
    // THE SUMMARY ONLY WHEN THERE ARE NO ROWS. Once this page has started a
    // check, the rows below ARE the newest answer and are richer than the
    // summary; printing both would show one verdict twice and invite a reader
    // to compare them for a difference that cannot exist.
    (result === null ? renderLastConnectionCheck(v.lastTest) : renderPreflight(result)) +
    "</section>"
  );
}

// ===========================================================================
// THE TOPIC DISCOVERY PANEL (PLAT-09.1)
// ===========================================================================
//
// WHAT A DISCOVERY IS, AND WHAT IT IS NOT. A `TopicDiscovery` is a bounded
// inventory: a check Job dials this connection with this connection's own
// credential, asks the broker for metadata, and stores what came back. It is
// the only control on this page that makes anything DIAL -- the probe panel
// above it re-reads a status the controller wrote on its own schedule, which
// is why that one says "connection probe" and this one does not borrow the
// word.
//
// AND A SUCCESSFUL LISTING IS NOT A COMPLETE ONE. An all-topics Metadata
// request silently omits every topic the principal cannot DESCRIBE: no error,
// no count, nothing to notice. So `visibility.state` is `unknown` for a
// listing that worked perfectly, `unknown` is that field's HEALTHY default,
// and the word "complete" appears on this page only inside
// `attestedComplete` -- which is an administrator's claim, rendered with its
// author and with "not verified by Logweir" attached.
//
// TWO SLOTS, NEVER ONE. `?latest=true` answers `{latestAttempt,
// lastSuccessful}`. A failed attempt must not hide the last inventory that
// worked, and a working inventory must not hide that the newest attempt
// failed, so both are rendered and each says which it is.
//
// A SHORT PAGE IS NOT THE LAST PAGE. `scan.complete: false` means the eight-
// chunk budget was spent before the end of the result -- which is exactly what
// a sparse `q` over a large inventory looks like. Treating a short page as the
// end would silently show an operator four topics out of five thousand. So the
// control below follows `page.nextCursor` and says, in words, what it is
// doing.

/** The identity of the discovery form in the draft and mutation registries. */
export const DISCOVERY_FORM = "topic-discovery";

/** What a short page means, rendered beside one. */
export const SCAN_INCOMPLETE_SENTENCE =
  "This page stopped at its chunk budget, not at the end of the result. There is more to read: " +
  "follow the cursor. A short page is not the last page, and a filter that matches little is " +
  "exactly what makes a page short.";

/** What a truncated discovery means. */
export const TRUNCATION_SENTENCE =
  "The inventory hit a ceiling before the cluster ran out of topics, so this is a prefix of what " +
  "the broker listed and not the whole of it.";

/** What "no visible topics" is not. */
export const DISCOVERY_STALE_SENTENCE =
  "This inventory is past its freshness or its connection changed under it. It is shown because " +
  "an old fact with a date on it is more use than a blank, and it is labelled because an old " +
  "fact presented as current is not.";

/** One discovery's counts, freshness and error, as facts. */
export function renderDiscoveryFacts(discovery) {
  const d = discovery || {};
  const counts = d.counts || {};
  const expected = d.expected || {};
  const connection = d.connection || {};
  const error = d.error || {};
  return facts([
    ["id", "<code>" + cell(d.id) + "</code>"],
    ["state", cell(d.state)],
    ["reason", cell(d.reason)],
    ["observed at", when(d.observedAt)],
    ["fresh until", cell(d.freshUntil)],
    ["cluster id", cell(d.clusterId)],
    ["principal", cell(connection.principal)],
    ["listed / stored", cell(counts.listed) + " / " + cell(counts.returned)],
    ["internal excluded", cell(counts.internalExcluded)],
    ["entries whose metadata could not be read", cell(counts.errored)],
    ["expected topics", cell(expected.requested) === ABSENT
      ? ABSENT
      : esc(String(expected.requested) + " asked, " + String(expected.visible) + " visible, " +
        String(expected.notAuthorized) + " not authorized, " + String(expected.notFound) +
        " not found, " + String(expected.unknown) + " unknown")],
    ["error", cell(error.code) === ABSENT ? ABSENT : cell(error.code) + " " + cell(error.message)],
  ]);
}

/** One discovery, with its visibility banner and its labels. `role` is
 *  `latest` or `successful` and becomes part of the heading, because a reader
 *  must never have to work out which of the two slots they are looking at. */
export function renderDiscovery(discovery, role) {
  const d = discovery || {};
  const counts = d.counts || {};
  const heading = role === "successful" ? "Last successful inventory" : "Latest attempt";
  const empty = d.state === "succeeded" && counts.returned === 0;
  return (
    "<section class=\"discovery discovery-" + esc(String(role)) + "\" id=\"discovery-" +
    esc(String(role)) + "\"><h4>" + esc(heading) + "</h4>" +
    phaseBadge(d.state) +
    (d.stale === true
      ? " " + badge("unverified", "stale: " +
        (Array.isArray(d.staleReasons) && d.staleReasons.length > 0
          ? d.staleReasons.join(", ")
          : "reason not recorded"))
      : "") +
    (d.truncated === true
      ? " " + badge("unverified", "truncated: " + String(d.truncationReason || "unrecorded"))
      : "") +
    (d.stale === true ? "<p class=\"note\">" + esc(DISCOVERY_STALE_SENTENCE) + "</p>" : "") +
    (d.truncated === true ? "<p class=\"note\">" + esc(TRUNCATION_SENTENCE) + "</p>" : "") +
    // A DISCOVERY THIS PAGE STOPPED FOLLOWING says so, with "Run the discovery
    // again" (`lifecycle.js`'s `followCheck`); only the latest attempt is ever
    // followed.
    (role === "successful" ? "" : checkStoppedBlock(d, "discovery")) +
    visibilityLine(d.visibility) +
    (empty
      ? "<p class=\"note\" id=\"discovery-empty\">" + esc(EMPTY_INVENTORY_SENTENCE) + "</p>"
      : "") +
    renderDiscoveryFacts(d) +
    "</section>"
  );
}

/** The topic table: name, partitions, internal, expected, error code. */
export function renderTopicTable(page) {
  const p = page || {};
  const items = Array.isArray(p.items) ? p.items : [];
  const scan = p.scan || {};
  const paging = p.page || {};
  const rows = items.map((t) => [
    "<code>" + cell(t.name) + "</code>",
    cell(t.partitions),
    t.internal === true ? "yes" : "no",
    t.expected === true ? "yes" : "no",
    cell(t.errorCode),
  ]);
  return (
    table(
      ["TOPIC", "PARTITIONS", "INTERNAL", "EXPECTED", "ERROR"],
      rows,
      "No topic on this page. That is a statement about this page and its filters, not about " +
        "the cluster.",
    ) +
    "<p class=\"note\" id=\"topic-scan\">" +
    (scan.complete === true
      ? "The scan reached the end of the stored result (" + cell(scan.chunksScanned) +
        " chunk(s) read)."
      : esc(SCAN_INCOMPLETE_SENTENCE) + " (" + cell(scan.chunksScanned) + " chunk(s) read)") +
    "</p>" +
    (typeof paging.nextCursor === "string" && paging.nextCursor.length > 0
      ? "<div class=\"actions\"><button type=\"button\" id=\"topics-more\" " +
        "data-cursor=\"" + esc(paging.nextCursor) + "\">Read the next page</button></div>"
      : "<p class=\"note\">No cursor: this is the end of the result.</p>") +
    "<p class=\"note\">snapshot " + cell(paging.snapshot) + "</p>"
  );
}

/** The whole panel: the controls, the two slots, and the topic table. */
export function renderDiscoveryPanel(view) {
  const v = view || {};
  const may = v.mayOperate !== false;
  const latest = v.latestAttempt || null;
  const successful = v.lastSuccessful || null;
  // WHAT THE INPUTS SHOW is what the reader typed (`v.typed`, read off the
  // live controls before a repaint -- P13's class) over the filters last
  // APPLIED. The two are kept apart because the applied filters are what
  // "Show more" pages with: a cursor is bound to them, and typed text nobody
  // submitted must not become a filter a cursor was never issued for.
  const filters = Object.assign({}, v.filters || {}, v.typed || {});
  const state = v.state || {};
  const pending = state.phase === "pending";
  const running = latest !== null && latest.terminal === false;
  return (
    "<section class=\"discover\" id=\"cluster-discovery\"><h3>Discover topics</h3>" +
    "<p class=\"note\">A discovery starts a check Job that dials this connection with its own " +
    "credential and asks the broker for metadata. It is the only control on this page that " +
    "makes anything dial; the connection probe above re-reads what the controller already " +
    "recorded.</p>" +
    (v.unavailable === true
      ? "<p class=\"note\" id=\"discovery-unavailable\">" + messageText(v.unavailableReason) + "</p>"
      : "") +
    (may && v.unavailable !== true
      ? "<form id=\"discovery-form\" novalidate" + (pending ? " aria-busy=\"true\"" : "") + ">" +
        "<fieldset class=\"form-body\"" + (pending ? " disabled" : "") + ">" +
        "<label class=\"inline\" for=\"discovery-internal\">" +
        "<input type=\"checkbox\" id=\"discovery-internal\" name=\"includeInternal\"" +
        (filters.includeInternal === true ? " checked" : "") +
        "> include Kafka internal topics (__-prefixed)</label>" +
        "<div class=\"field\"><label for=\"discovery-expected\">expected topics, comma " +
        "separated</label>" +
        "<input id=\"discovery-expected\" name=\"expectedTopics\" value=\"" +
        esc(String(filters.expectedTopics || "")) + "\">" +
        "<p class=\"help\">Named topics the check asks about EXPLICITLY. A broker returns " +
        "TOPIC_AUTHORIZATION_FAILED for a name this principal cannot describe whether or not " +
        "it exists, which is the only way a listing's silence becomes visible.</p></div>" +
        "<div class=\"actions\">" +
        "<button type=\"submit\" id=\"discovery-start\">Discover topics</button>" +
        (running
          ? "<button type=\"button\" id=\"discovery-cancel\">Cancel</button>"
          : "") +
        "</div></fieldset>" +
        "<div class=\"form-status\" id=\"discovery-status\" tabindex=\"-1\">" +
        mutationStatus(state, { kind: "TopicDiscovery", name: (latest || {}).id || "" }, null) +
        "</div></form>"
      : (v.unavailable === true ? "" :
        "<p class=\"note\">This login may read topic discoveries in this namespace and not " +
        "start one.</p>")) +
    (v.reused === true
      ? "<p class=\"note\" id=\"discovery-reused\">A fresh identical inventory already existed, " +
        "so no second check was started. This is that result.</p>"
      : "") +
    (latest === null && successful === null
      ? "<p class=\"note\" id=\"discovery-none\">No topic discovery has run for this connection. " +
        "Nothing below claims anything about its topics.</p>"
      : "") +
    (latest === null ? "" : renderDiscovery(latest, "latest")) +
    (successful === null
      ? (latest === null
        ? ""
        : "<p class=\"note\" id=\"no-successful\">No discovery for this connection has ever " +
          "produced an inventory.</p>")
      : (latest !== null && latest.id === successful.id
        ? "<p class=\"note\" id=\"same-discovery\">The latest attempt is also the last " +
          "successful inventory.</p>"
        : renderDiscovery(successful, "successful"))) +
    renderTopicFilters(filters) +
    (v.topics === null || v.topics === undefined
      ? ""
      : "<div id=\"topics-slot\">" + renderTopicTable(v.topics) + "</div>") +
    (v.topicsError === null || v.topicsError === undefined
      ? ""
      : "<p class=\"note\" id=\"topics-error\">The stored inventory could not be read: " +
        cell(v.topicsError.message) + "</p>") +
    "</section>"
  );
}

/** The search, prefix and internal filters over a stored inventory. */
export function renderTopicFilters(filters) {
  const f = filters || {};
  return (
    "<form id=\"topic-filters\" novalidate><fieldset><legend>search the stored inventory" +
    "</legend>" +
    "<p class=\"help\">These filter what the SERVICE reads out of the stored result. They " +
    "change nothing about the cluster and start no check.</p>" +
    "<div class=\"field-row\">" +
    "<div class=\"field\"><label for=\"topic-q\">contains</label>" +
    "<input id=\"topic-q\" name=\"q\" value=\"" + esc(String(f.q || "")) + "\"></div>" +
    "<div class=\"field\"><label for=\"topic-prefix\">prefix</label>" +
    "<input id=\"topic-prefix\" name=\"prefix\" value=\"" + esc(String(f.prefix || "")) +
    "\"></div>" +
    "<div class=\"field\"><label for=\"topic-internal\">internal</label>" +
    "<select id=\"topic-internal\" name=\"internal\">" +
    "<option value=\"exclude\"" + (f.internal === "include" ? "" : " selected") + ">exclude" +
    "</option>" +
    "<option value=\"include\"" + (f.internal === "include" ? " selected" : "") + ">include" +
    "</option></select></div>" +
    "<div class=\"field\"><label for=\"topic-errored\">unreadable entries</label>" +
    "<select id=\"topic-errored\" name=\"errored\">" +
    ["include", "exclude", "only"].map((o) =>
      "<option value=\"" + esc(o) + "\"" + (f.errored === o ? " selected" : "") + ">" + esc(o) +
      "</option>").join("") +
    "</select></div>" +
    "</div>" +
    "<div class=\"actions\"><button type=\"submit\">Search</button></div>" +
    "</fieldset></form>"
  );
}

/** The values an empty form starts from. */
const CLUSTER_DEFAULTS = Object.freeze({
  name: "", servers: "", role: "source", mode: "plaintext", username: "", secret: "",
  passwordKey: "", tls: false, tlsCaKind: "none", tlsCaName: "", tlsCaKey: "",
});

/** The three answers the private-CA control takes. `none` is the default and
 *  means the runner image's own trust store, which is what every connection to
 *  a publicly signed broker uses. */
export const TLS_CA_KINDS = Object.freeze(["none", "secret", "configMap"]);

/** A Kubernetes data key: the `^[-._a-zA-Z0-9]+$` the CRD spells for
 *  `secretRef.passwordKey` and for both halves of `tlsCa`. */
const DATA_KEY = /^[-._a-zA-Z0-9]+$/;

/** True when `value` is a data key the API server will accept. */
export function isDataKey(value) {
  return typeof value === "string" && value.length > 0 && value.length <= 253 && DATA_KEY.test(value);
}

/** A lowercase RFC 1123 subdomain: what Kubernetes accepts as an object name,
 *  and what a Secret's name must be. */
const DNS_SUBDOMAIN = /^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$/;

/** True when `value` is a name the API server will accept for an object. */
export function isObjectName(value) {
  return typeof value === "string" && value.length > 0 && value.length <= 253 && DNS_SUBDOMAIN.test(value);
}

const HOST_PORT = /^(\[[0-9A-Fa-f:.]+\]|[^\s:[\]]+):([0-9]{1,5})$/;

/** The page's own checks, by field: the message for each field that is not
 *  acceptable, and nothing for a field that is. A CONVENIENCE -- the API
 *  server's schema and the controller's probe are the gate -- that turns a
 *  round trip and a generic 422 into a message beside the field. */
export function validateCluster(values) {
  const v = values || {};
  const problems = Object.create(null);
  if (!isObjectName(v.name)) {
    problems.name = "a KafkaCluster name is lowercase letters, digits, '-' and '.', starting and " +
      "ending with a letter or digit";
  }
  const servers = String(v.servers || "").split(",").map((s) => s.trim()).filter((s) => s.length > 0);
  if (servers.length === 0) {
    problems.servers = "name at least one bootstrap server, as host:port";
  } else {
    const bad = servers.filter((s) => {
      const match = HOST_PORT.exec(s);
      return match === null || Number(match[2]) < 1 || Number(match[2]) > 65535;
    });
    if (bad.length > 0) {
      problems.servers = "not host:port: " + bad.join(", ");
    }
  }
  if (typeof v.role !== "string" || v.role.trim().length === 0) {
    problems.role = "a role is required; source or target is what the controller reports";
  }
  if (v.mode !== "plaintext" && v.mode !== "scramSha512") {
    problems.mode = "auth mode is plaintext or scramSha512";
  }
  if (v.mode === "scramSha512" && (typeof v.username !== "string" || v.username.length === 0)) {
    problems.username = "scramSha512 needs the SASL username";
  }
  if (v.mode === "scramSha512" && (typeof v.secret !== "string" || v.secret.length === 0)) {
    problems.secret = "scramSha512 needs the name of the Secret that holds the password";
  } else if (typeof v.secret === "string" && v.secret.length > 0 && !isObjectName(v.secret)) {
    problems.secret = "a Secret name is lowercase letters, digits, '-' and '.'";
  }
  // CONTRACT v1, THE SAME FOUR RULES `weirkeeper::connection::resolve` AND THE
  // CRD's CEL APPLY. A CONVENIENCE and never the gate: the API server refuses
  // three of these at admission and the resolver refuses the fourth before a
  // Job exists. Saying so here turns a round trip into a message beside the
  // field, and each message names the rule rather than restating the value.
  const key = String(v.passwordKey || "").trim();
  if (key.length > 0 && !isDataKey(key)) {
    problems.passwordKey = "a Secret data key is letters, digits, '-', '_' and '.'";
  }
  if (key.length > 0 && (typeof v.secret !== "string" || v.secret.length === 0)) {
    problems.passwordKey = "a data key belongs to a Secret; name the Secret first, or leave " +
      "this blank for the key every earlier release projected";
  }
  if (v.mode === "plaintext" && v.tls === true) {
    problems.tls = "auth mode plaintext with TLS on -- TLS without SASL -- is not supported by " +
      "saved-connection contract v1 and is refused rather than dialled without TLS. Choose " +
      "scramSha512 over TLS, or leave TLS off for a plaintext listener";
  }
  const kind = TLS_CA_KINDS.indexOf(v.tlsCaKind) === -1 ? "none" : v.tlsCaKind;
  if (kind !== "none") {
    if (v.tls !== true) {
      problems.tlsCaName = "a CA verifies a TLS transport, so naming one requires TLS on";
    }
    if (!isObjectName(String(v.tlsCaName || "").trim())) {
      problems.tlsCaName = "the name of the " + (kind === "secret" ? "Secret" : "ConfigMap") +
        " in this namespace that holds the PEM CA certificate(s)";
    }
    if (!isDataKey(String(v.tlsCaKey || "").trim())) {
      problems.tlsCaKey = "the data key inside it, letters, digits, '-', '_' and '.'";
    }
  }
  return problems;
}

/** The create form, rendered from a draft, the field messages and the form's
 *  mutation record. Called with nothing it is the empty form. */
export function renderClusterForm(view) {
  const v = view || {};
  const d = Object.assign({}, CLUSTER_DEFAULTS, v.draft || {});
  const errors = ((v.errors || {}).fields) || {};
  const state = v.state || {};
  const pending = state.phase === "pending";
  const field = (id, name) => invalidAttributes(id, errors[name]);
  const line = (id, name) => fieldErrorLine(id, errors[name]);
  return (
    "<section class=\"create\" id=\"cluster-create\"><h3>Create a KafkaCluster</h3>" +
    "<p class=\"note\">A KafkaCluster names a set of brokers and how to reach them. The " +
    "controller probes it and records the cluster id it reads from the broker.</p>" +
    "<form id=\"cluster-form\" novalidate" + (pending ? " aria-busy=\"true\"" : "") + ">" +
    "<fieldset class=\"form-body\"" + (pending ? " disabled" : "") + ">" +
    (v.minted === true
      // P7: NO FIELD FOR A CHOICE THE SERVER DOES NOT HONOUR.
      ? "<div class=\"field\" id=\"cluster-name-minted\"><p class=\"label\">name</p>" +
        "<p class=\"help\">" + esc(CONNECTION_NAME_MINTED_SENTENCE) + "</p>" +
        line("cluster-name", "name") + "</div>"
      : "<div class=\"field\"><label for=\"cluster-name\">name</label>" +
        "<input id=\"cluster-name\" name=\"name\" required value=\"" + esc(d.name) + "\"" +
        field("cluster-name", "name") + ">" +
        "<p class=\"help\">A Kubernetes object name: lowercase, digits and dashes.</p>" +
        line("cluster-name", "name") + "</div>") +
    "<div class=\"field\"><label for=\"cluster-servers\">bootstrap servers, comma separated</label>" +
    "<input id=\"cluster-servers\" name=\"servers\" required value=\"" + esc(d.servers) + "\"" +
    field("cluster-servers", "servers") + ">" +
    "<p class=\"help\">host:port pairs, as a client would dial them from inside the cluster.</p>" +
    line("cluster-servers", "servers") + "</div>" +
    "<div class=\"field-row\">" +
    "<div class=\"field\"><label for=\"cluster-role\">role</label>" +
    "<input id=\"cluster-role\" name=\"role\" required value=\"" + esc(d.role) + "\"" +
    field("cluster-role", "role") + ">" +
    "<p class=\"help\">A label the controller reports: source or target. It authorises nothing.</p>" +
    line("cluster-role", "role") + "</div>" +
    "<div class=\"field\"><label for=\"cluster-mode\">auth mode</label>" +
    "<select id=\"cluster-mode\" name=\"mode\"" + field("cluster-mode", "mode") + ">" +
    "<option value=\"plaintext\"" + (d.mode === "plaintext" ? " selected" : "") + ">plaintext</option>" +
    "<option value=\"scramSha512\"" + (d.mode === "scramSha512" ? " selected" : "") + ">scramSha512</option>" +
    "</select>" + line("cluster-mode", "mode") + "</div>" +
    "</div>" +
    "<div class=\"field-row\">" +
    "<div class=\"field\"><label for=\"cluster-username\">auth username</label>" +
    "<input id=\"cluster-username\" name=\"username\" value=\"" + esc(d.username) + "\"" +
    field("cluster-username", "username") + ">" + line("cluster-username", "username") + "</div>" +
    "<div class=\"field\"><label for=\"cluster-secret\">auth Secret name</label>" +
    "<input id=\"cluster-secret\" name=\"secret\" value=\"" + esc(d.secret) + "\"" +
    field("cluster-secret", "secret") + ">" +
    "<p class=\"help\">The NAME of the Secret holding the credential. The value is never read " +
    "by this page.</p>" + line("cluster-secret", "secret") + "</div>" +
    "</div>" +
    "<div class=\"field\"><label for=\"cluster-password-key\">data key in that Secret " +
    "(spec.auth.secretRef.passwordKey)</label>" +
    "<input id=\"cluster-password-key\" name=\"passwordKey\" value=\"" + esc(d.passwordKey) +
    "\"" + field("cluster-password-key", "passwordKey") + ">" +
    "<p class=\"help\">Which entry of that Secret the controller projects. Leave it blank for " +
    "the entry every earlier release used, which is what a KafkaCluster created before " +
    "connection contract v1 means. This is the KEY's name, not its value: nothing on this page " +
    "reads the Secret.</p>" + line("cluster-password-key", "passwordKey") + "</div>" +
    "<label class=\"inline\"><input id=\"cluster-tls\" name=\"tls\" type=\"checkbox\"" +
    (d.tls === true ? " checked" : "") + "> TLS</label>" +
    line("cluster-tls", "tls") +
    "<p class=\"help\">The only switch that turns TLS on, and it is independent of the auth " +
    "mode: contract v1 supports scramSha512 over TLS (SASL_SSL). plaintext with TLS on is " +
    "refused rather than dialled in the clear.</p>" +
    "<fieldset class=\"ca\"><legend>private certificate authority " +
    "(spec.auth.tlsCa)</legend>" +
    "<p class=\"help\">Only when the brokers' certificates are signed by an authority the " +
    "runner image does not already trust. Exactly one key of a Secret or a ConfigMap in this " +
    "namespace, holding PEM certificate(s); a CA certificate is public, so a ConfigMap is an " +
    "ordinary home for it. It replaces the default trust store for this connection and requires " +
    "TLS on.</p>" +
    "<div class=\"field\"><label for=\"cluster-tls-ca-kind\">CA source</label>" +
    "<select id=\"cluster-tls-ca-kind\" name=\"tlsCaKind\">" +
    TLS_CA_KINDS.map(
      (kind) =>
        "<option value=\"" + esc(kind) + "\"" + (d.tlsCaKind === kind ? " selected" : "") + ">" +
        esc(kind === "none" ? "none (trust the runner image's own store)" : kind) + "</option>",
    ).join("") +
    "</select></div>" +
    "<div class=\"field-row\">" +
    "<div class=\"field\"><label for=\"cluster-tls-ca-name\">CA object name</label>" +
    "<input id=\"cluster-tls-ca-name\" name=\"tlsCaName\" value=\"" + esc(d.tlsCaName) + "\"" +
    field("cluster-tls-ca-name", "tlsCaName") + ">" +
    line("cluster-tls-ca-name", "tlsCaName") + "</div>" +
    "<div class=\"field\"><label for=\"cluster-tls-ca-key\">CA data key</label>" +
    "<input id=\"cluster-tls-ca-key\" name=\"tlsCaKey\" value=\"" + esc(d.tlsCaKey) + "\"" +
    field("cluster-tls-ca-key", "tlsCaKey") + ">" +
    line("cluster-tls-ca-key", "tlsCaKey") + "</div>" +
    "</div></fieldset>" +
    "<div class=\"actions\"><button type=\"submit\" class=\"primary\">Create</button></div>" +
    "</fieldset>" +
    "<div class=\"form-status\" id=\"cluster-form-status\" tabindex=\"-1\">" +
    mutationStatus(state, { kind: "KafkaCluster", name: d.name }, ((v.errors || {}).unmatched)) +
    "</div>" +
    "</form>" +
    "<p class=\"note\">The credential itself lives in the Secret named above and " +
    "is never read by this page, by a status field or by a rendered document. What you type " +
    "here is kept in this page's memory until the cluster exists -- through an error, a lost " +
    "response or a visit to another page -- and never written to browser storage; a reload " +
    "starts empty.</p>" +
    "</section>"
  );
}

/** The request body a filled-in form produces. Pure: it reads a plain object
 *  of field values, not the DOM. */
export function clusterBody(values) {
  const auth = { mode: values.mode || "plaintext", tls: values.tls === true };
  if (values.username) {
    auth.username = values.username;
  }
  if (values.secret) {
    auth.secretRef = { name: values.secret };
    // ABSENT IS A MEANING, so a blank key is left out rather than sent as the
    // default: an object that omits `passwordKey` is byte-for-byte what every
    // release before contract v1 wrote, and one that spells the default is a
    // different object with the same behaviour. The difference matters because
    // `spec` is immutable and the frozen execution inputs record what is here.
    const key = String(values.passwordKey || "").trim();
    if (key.length > 0) {
      auth.secretRef.passwordKey = key;
    }
  }
  const kind = TLS_CA_KINDS.indexOf(values.tlsCaKind) === -1 ? "none" : values.tlsCaKind;
  if (kind !== "none") {
    const reference = {
      name: String(values.tlsCaName || "").trim(),
      key: String(values.tlsCaKey || "").trim(),
    };
    auth.tlsCa = kind === "secret"
      ? { secretKeyRef: reference }
      : { configMapKeyRef: reference };
  }
  return {
    apiVersion: "logweir.dev/v1alpha1",
    kind: "KafkaCluster",
    metadata: { name: values.name },
    spec: {
      bootstrapServers: String(values.servers || "")
        .split(",")
        .map((s) => s.trim())
        .filter((s) => s.length > 0),
      auth: auth,
      role: values.role || "source",
    },
  };
}

/** Checks the values, then creates the cluster idempotently by name. Throws an
 *  `invalid` error carrying the field messages, without a request, when the
 *  page's own checks refuse; see `createOnce` for what a retry resolves to. */
export async function submitCluster(ns, values, deps) {
  // P7: IN CONSOLE MODE THE SERVER NAMES THE OBJECT, so there is no name to
  // check, and `metadata.name` carries the draft's INTENT -- the one value
  // `ui/client.js` composes the idempotency key from. `requestBody` never
  // sends it: the create body has no name member.
  const minted = connectionNamesMinted();
  const v = values || {};
  const problems = validateCluster(minted ? Object.assign({}, v, { name: "minted" }) : v);
  if (minted && !(typeof v.intent === "string" && v.intent.length >= 8)) {
    problems.name = "this draft carries no idempotency intent; reload the page and fill it in again";
  }
  if (Object.keys(problems).length > 0) {
    throw invalidInput(problems);
  }
  const body = clusterBody(minted ? Object.assign({}, v, { name: v.intent }) : v);
  return createOnce(deps || API, ns, PLURAL, body, CLUSTER_SPEC_RULES);
}

/** What the form renders from in namespace `ns`: its draft, its record and the
 *  messages the record's failure carries. A record that already succeeded has
 *  consumed its draft, even if no view was current to see it happen. */
export function clusterFormView(ns) {
  const key = formKey(ns, CLUSTER_FORM);
  const state = mutationFor(key).state;
  if (state.phase === "succeeded") {
    dropDraft(key);
  }
  return {
    draft: readDraft(key),
    state: state,
    errors: state.phase === "failed" ? fieldErrors(state.error, CLUSTER_FIELD_PATHS) : null,
    minted: connectionNamesMinted(),
  };
}

// --------------------------------------------------------------- mount half

/** Reads the namespace's clusters and renders them. On any API error -- a 403
 *  from the viewer's own RBAC included -- the API server's OWN reason and
 *  message are rendered verbatim, because the page made no authorisation
 *  decision and must not narrate one. */
export async function mountClusters(node, ns, parse, lifecycle, deps) {
  const api = deps || API;
  try {
    const collection = await api.list(ns, PLURAL, readOptions(lifecycle));
    if (!active(lifecycle)) {
      return;
    }
    replace(
      node,
      parse(
        renderClusterList(collection, ns) +
          "<div class=\"form-slot\" id=\"cluster-form-slot\">" +
          renderClusterForm(clusterFormView(ns)) +
          "</div>",
      ),
    );
    wireProbeTests(node, ns, parse, lifecycle, api);
    wireForm(node, ns, parse, lifecycle, api);
  } catch (error) {
    if (!cancelled(error, lifecycle) && active(lifecycle)) {
      replace(node, errorBox(error));
    }
  }
}

/** Reads one cluster and renders its detail, with the Test connection control
 *  wired to a re-read of the same object and the discovery panel wired to the
 *  product API's own check routes.
 *
 *  THE DISCOVERY READ IS SEPARATE AND ITS FAILURE IS NOT THE PAGE'S. In legacy
 *  mode it is refused by name -- `kubectl proxy` does not serve these routes --
 *  and the panel says so instead of the whole detail view disappearing behind
 *  an error box for a connection whose own facts read perfectly well. */
export async function mountClusterDetail(node, ns, name, parse, lifecycle, deps) {
  const api = deps || API;
  try {
    const object = await api.get(ns, PLURAL, name, readOptions(lifecycle));
    if (!active(lifecycle)) {
      return;
    }
    const discovery = await readDiscoveries(api, ns, name, lifecycle);
    if (!active(lifecycle)) {
      return;
    }
    paintClusterDetail(node, ns, name, parse, lifecycle, api, object, discovery);
    resumeFollows(node, ns, name, parse, lifecycle, api, discovery);
  } catch (error) {
    if (!cancelled(error, lifecycle) && active(lifecycle)) {
      replace(node, errorBox(error));
    }
  }
}

/** The two slots for this connection, or the reason there are none. */
async function readDiscoveries(api, ns, name, lifecycle) {
  const base = {
    mayOperate: mayOperate(ns),
    filters: Object.create(null),
    topics: null,
    topicsError: null,
  };
  try {
    const answer = await api.latestDiscoveries(ns, name, readOptions(lifecycle));
    return Object.assign(base, {
      latestAttempt: answer.latestAttempt,
      lastSuccessful: answer.lastSuccessful,
    });
  } catch (error) {
    if (cancelled(error, lifecycle)) {
      throw error;
    }
    return Object.assign(base, {
      latestAttempt: null,
      lastSuccessful: null,
      unavailable: true,
      unavailableReason: error.message,
    });
  }
}

/** A CHECK ON SCREEN THAT NOTHING READS IS A CHECK LEFT "NOT FINISHED" FOR
 *  GOOD (poc-upgrade-3's P15, its second face). The started check's rows are
 *  remembered across visits (`checkViews`), and a follow ends with the route
 *  that started it: a reader who left while a check ran came back to "this
 *  page reads it again until then" and nothing did. Likewise a discovery the
 *  read shows unfinished -- started here before, or by anybody -- used to sit
 *  at `pending` until a reload. So a mount follows each of them again, unless
 *  a live follow already reads it or this page already stopped following it;
 *  the follow is bounded by the check's own deadline as every follow is. */
function resumeFollows(node, ns, name, parse, lifecycle, api, discovery) {
  const unfinished = (check) => check !== null && check !== undefined &&
    typeof check.id === "string" && check.id.length > 0 && check.terminal !== true &&
    followStopped(check) === "" && !isFollowed(check.id);
  const held = (checkViews.get(formKey(ns, CONNECTION_CHECK_FORM, name)) || {}).preflight;
  if (unfinished(held)) {
    followConnectionCheck(node, ns, name, parse, lifecycle, api, held);
  }
  const latest = (discovery || {}).latestAttempt;
  if (unfinished(latest)) {
    followDiscovery(node, ns, name, parse, lifecycle, api, latest);
  }
}

// WHAT THE DETAIL LAST PAINTED -- the connection object and the discovery
// view -- per connection, so a follow's answer repaints the view as it is NOW
// and not as it was when the follow began: a probe re-read or a page of
// topics landing between two reads is kept.
const detailViews = new Map();

// THE STARTED CHECK'S OWN RESULT, REMEMBERED PER CLUSTER FOR THE LIFE OF THE
// LOADED PAGE. Every other control on this view repaints the whole detail --
// the re-read, a discovery, a topic page -- and each of those calls
// `paintClusterDetail` with no check view of its own. Without this the first
// such repaint would wipe a verdict the operator had just asked for, leaving a
// `succeeded` mutation status over an empty panel: the record says a check was
// made and the panel shows none. It is keyed exactly like the mutation record
// it belongs beside.
const checkViews = new Map();

/** What the reader has put into the discovery form and the topic filters,
 *  read off the live controls, or `null` when none is on screen (P13's class).
 *
 *  THE WHOLE DETAIL REPAINTS on every answer this view waits for -- each read
 *  of a followed "Test connection", the probe re-read, the discovery's own
 *  record, a page of topics -- and it rendered those inputs from the view
 *  alone, so text typed while a check was being followed was gone when its
 *  next read landed. Nothing here is a credential: names and filters. */
export function readDiscoveryTyped(node) {
  const typed = {};
  const text = (id, field) => {
    const control = node.querySelector("#" + id);
    if (control !== null && control.value !== undefined && control.value !== null) {
      typed[field] = String(control.value);
    }
  };
  const internal = node.querySelector("#discovery-internal");
  if (internal !== null) {
    typed.includeInternal = internal.checked === true;
  }
  text("discovery-expected", "expectedTopics");
  text("topic-q", "q");
  text("topic-prefix", "prefix");
  text("topic-internal", "internal");
  text("topic-errored", "errored");
  return Object.keys(typed).length === 0 ? null : typed;
}

function paintClusterDetail(node, ns, name, parse, lifecycle, api, object, discovery, check) {
  const key = formKey(ns, DISCOVERY_FORM, name);
  const typed = readDiscoveryTyped(node);
  const view = Object.assign({ state: mutationFor(key).state }, discovery || {},
    typed === null ? {} : { typed: typed });
  const checkKey = formKey(ns, CONNECTION_CHECK_FORM, name);
  const remembered = checkViews.get(checkKey);
  const checkView = Object.assign(
    {
      mayOperate: mayOperate(ns),
      preflight: null,
      // THE OBJECT'S OWN, from the read this view already did. `lastTest` is
      // the product API's summary of the newest connectivity check for this
      // connection; in legacy mode it is simply absent, which the panel
      // renders as "none recorded" rather than as "none ran".
      lastTest: (object || {}).lastTest || null,
      // F6: the branch is no longer dead. `mode()` is `null` until the page
      // has decided, and an undecided page is not a legacy one -- the check is
      // deliberately `=== LEGACY` and not `!== CONSOLE`.
      unavailable: mode() === LEGACY,
      unavailableReason: CONNECTION_CHECK_LEGACY_SENTENCE,
    },
    remembered || {},
    check || {},
    {
      state: mutationFor(checkKey).state,
      refusedReason: connectionRefusal(object),
    },
  );
  checkViews.set(checkKey, { preflight: checkView.preflight });
  // WITHOUT the mutation state and the typed text: both are read fresh at
  // every paint, and a stored copy would paint an old one over them.
  const stored = Object.assign({}, discovery || {});
  delete stored.state;
  delete stored.typed;
  detailViews.set(key, { object: object, discovery: stored });
  replace(
    node,
    parse(renderClusterDetail(object, undefined, undefined, undefined, view, checkView)),
  );
  wireDetailProbe(node, ns, name, parse, lifecycle, api, view, checkView);
  wireConnectionCheck(node, ns, name, parse, lifecycle, api, object, view, checkView);
  wireDiscovery(node, ns, name, parse, lifecycle, api, object, view);
}

// THE ATTEMPT TOKEN. One per ACCEPTED click, minted here and carried into the
// idempotency key `ui/client.js` composes. A double click cannot mint two,
// because the second event is refused by the record's own machine before this
// is reached; a DELIBERATE second test -- the whole point of the control --
// mints a new one, so the product API creates a new `Preflight` instead of
// replaying the first for ever. That replay is precisely the re-read this
// control stopped being.
//
// AN ORDINAL ALONE IS NOT A TOKEN, AND THAT WAS REVIEW FINDING F1. The first
// cut composed `"<ns>.<name>.attempt-" + <module counter>`, and a module
// counter starts at zero on every page load -- so the FIRST click in one load
// and the FIRST click in the next composed the same key, byte for byte. The
// product API names a created object `sha256(issuer, subject, ns, route, key)`,
// so a repeated key with an identical body is a REPLAY: an operator who read
// `AuthenticationFailed`, fixed the Secret, reloaded and clicked again got the
// first check's stale rows back as the new verdict, for as long as the old
// object lived (`policy.preflight.retentionSeconds`, default an hour). The
// reviewer reproduced the collision across two browser sessions.
//
// So the token is a per-LOAD nonce composed with the per-click ordinal. The
// nonce makes two loads differ; the ordinal makes two clicks in one load
// differ. Neither alone is enough, and neither is a clock: two tabs opened in
// the same millisecond collide, which is the argument `schedules.js`'s
// `mintIntent` already records for the manual-run intent.
let attempts = 0;
let loadNonce = null;

/** How many hex characters the per-load nonce carries. Sixteen bytes, as
 *  `mintIntent`'s: the composed token is digested into the key by
 *  `ui/client.js`, so the budget is not the constraint -- collision resistance
 *  across page loads is. */
export const CONNECTION_NONCE_BYTES = 16;

/** Mints this page load's nonce, once.
 *
 *  A BROWSER WITH NO RANDOM SOURCE GETS A REFUSAL AND NOT A WEAKER TOKEN, for
 *  `mintIntent`'s reason in this control's own terms: a token this page could
 *  not make unique is a token that silently replays a verdict about a broker
 *  that has changed since. There is no counter fallback, because the counter
 *  fallback IS the defect F1 named. It is minted lazily rather than at module
 *  scope so that importing this page in a context without `crypto` is not
 *  itself a throw. */
export function connectionNonce() {
  if (loadNonce !== null) {
    return loadNonce;
  }
  const source = globalThis.crypto;
  if (source === undefined || source === null || typeof source.getRandomValues !== "function") {
    throw refusal(
      "this page will not start a connection check here: minting an idempotency key needs the " +
        "platform's random source, and it is unavailable. Without a unique key a second test " +
        "would return the earlier check instead of dialling again.",
    );
  }
  const bytes = source.getRandomValues(new Uint8Array(CONNECTION_NONCE_BYTES));
  let hex = "";
  for (const byte of bytes) {
    hex += byte.toString(16).padStart(2, "0");
  }
  loadNonce = hex;
  return loadNonce;
}

/** One started check, in the shape `mutationStatus` reads.
 *
 *  IT RENDERED `(uid )` WITH A HOLE IN IT, and the lab saw it. Every other
 *  create on this page goes through `lifecycle.js`'s `createOnce`, which
 *  answers with the stored OBJECT, so the status line reads
 *  `result.object.metadata.{name,uid}`. A console create answers with the
 *  product API's DTO instead, which carries the same two facts under its own
 *  names -- `id` IS `metadata.name` and `uid` IS `metadata.uid`, as the
 *  `Preflight` schema declares them -- so this projects them back rather than
 *  teaching the shared status helper a second shape. `item` is kept beside it
 *  because the panel's own watcher reads that.
 *
 *  A MISSING uid STAYS MISSING. Nothing here invents one: if the server ever
 *  answered without it the line would say so rather than print a name twice. */
export function startedPreflight(made) {
  const m = made || {};
  const item = m.item || null;
  return {
    item: item,
    replayed: m.replayed === true,
    object: item === null ? null : {
      metadata: { name: item.id, uid: item.uid },
    },
  };
}

/** The token one accepted click carries: this load's nonce and this load's
 *  click ordinal. Exported so the suite can assert that two clicks in one load
 *  differ, that two loads differ, and that a double click mints one. */
export function nextConnectionAttempt(ns, name) {
  const mint = connectionNonce();
  attempts += 1;
  return String(ns) + "." + String(name) + "." + mint + "-" + String(attempts);
}

/** Forgets this load's nonce and its click ordinal.
 *
 *  THE SUITE'S SEAM, and nothing else calls it -- `ui/client.js`'s `resetMode`
 *  is the same shape for the same reason. A page load mints one nonce; a page
 *  that could re-mint mid-life would be a page whose two clicks could compose
 *  one key, which is the bug this exists to let the suite reproduce. */
export function resetConnectionAttempts() {
  attempts = 0;
  loadNonce = null;
}

/** The "Test connection" control: create one `sourceConnection` `Preflight`,
 *  then follow it until a read is terminal or the check's own deadline has
 *  passed (`followConnectionCheck`).
 *
 *  THE DOUBLE-CLICK GUARD IS THE MUTATION RECORD, not a boolean in this
 *  closure: it is the same record every mount of this form in this namespace
 *  reads, so a second click while the first create is in flight is refused
 *  even across a re-render. */
function wireConnectionCheck(node, ns, name, parse, lifecycle, api, object, discovery, check) {
  const form = node.querySelector("#connection-check-form");
  if (form === null) {
    return;
  }
  const key = formKey(ns, CONNECTION_CHECK_FORM, name);
  const mutation = mutationFor(key);
  watchMutation(node, key, mutation, (state) => {
    if (!active(lifecycle)) {
      return;
    }
    if (state.phase === "succeeded") {
      const made = (state.result || {}).item || null;
      const next = Object.assign({}, check, { preflight: made });
      paintClusterDetail(node, ns, name, parse, lifecycle, api, object, discovery, next);
      followConnectionCheck(node, ns, name, parse, lifecycle, api, made);
      return;
    }
    paintClusterDetail(node, ns, name, parse, lifecycle, api, object, discovery, check);
  }, lifecycle);

  const start = () => {
    if (!active(lifecycle) || mutation.pending() || connectionRefusal(object).length > 0) {
      return;
    }
    // THE TOKEN IS MINTED INSIDE THE EXECUTOR so that a platform with no
    // random source becomes a refusal in the form's own status region -- the
    // mutation record catches a throwing executor -- rather than an exception
    // out of an event handler that nothing renders.
    mutation.run(async () => {
      const attempt = nextConnectionAttempt(ns, name);
      const made = await api.startPreflight(ns, connectionCheckRequest(name), { attempt: attempt });
      return startedPreflight(made);
    });
  };
  listen(form, "submit", (event) => {
    event.preventDefault();
    start();
  }, lifecycle);
  // "RUN THE CHECK AGAIN" IS TEST CONNECTION AGAIN: a per-click token, so a
  // new check.
  wireCheckRetry(node, (check || {}).preflight, start, lifecycle);
}

/** Re-reads a started check until a read answers terminal, or the longest time
 *  a check may take has passed (`lifecycle.js`'s `followCheck`; this panel's
 *  thirty seconds was poc-upgrade-3's P15).
 *
 *  EVERY READ IS GUARDED BY THE ROUTE (PLAT-13.1). The wait is `api.wait` when
 *  the caller supplies one; in a browser it is one `setTimeout` per read and
 *  nothing is left running when the route leaves, because the next read checks
 *  `active` before it issues. A newer check on this panel ends the follow of
 *  an older one, and each answer repaints the detail as it was last painted. */
function followConnectionCheck(node, ns, name, parse, lifecycle, api, first) {
  const checkKey = formKey(ns, CONNECTION_CHECK_FORM, name);
  const detailKey = formKey(ns, DISCOVERY_FORM, name);
  const mine = () => (((checkViews.get(checkKey) || {}).preflight) || {}).id === (first || {}).id;
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
      const last = detailViews.get(detailKey) || {};
      paintClusterDetail(node, ns, name, parse, lifecycle, api, last.object, last.discovery,
        { preflight: current });
    },
  });
}

/** Re-reads a discovery the panel shows unfinished until a read answers
 *  terminal, or the longest time a discovery may take has passed; then reads
 *  the two slots again, because a finished attempt may now be the last
 *  successful inventory too.
 *
 *  THE PANEL USED NOT TO READ A STARTED DISCOVERY AGAIN AT ALL: "Discover
 *  topics" painted the create answer, `pending`, and it stayed `pending` until
 *  the reader reloaded. A timer that kept reading after its reader stopped
 *  looking was the worry, and the follow answers it: it ends with the route,
 *  at the first terminal read, or at the discovery's own deadline, and it
 *  says in words when it gave up. */
function followDiscovery(node, ns, name, parse, lifecycle, api, first) {
  // A DISCOVERY ALREADY FOLLOWED IS LEFT TO ITS FOLLOW, which re-reads the
  // slots when it ends: a second ask that replayed it adds no second follow
  // (`followCheck`) and no second re-read here.
  if (isFollowed((first || {}).id)) {
    return Promise.resolve(null);
  }
  const key = formKey(ns, DISCOVERY_FORM, name);
  const mine = () => ((((detailViews.get(key) || {}).discovery) || {}).latestAttempt || {}).id ===
    (first || {}).id;
  const keep = () => active(lifecycle) && mine();
  const paint = (extra) => {
    const last = detailViews.get(key) || {};
    paintClusterDetail(node, ns, name, parse, lifecycle, api, last.object,
      Object.assign({}, last.discovery || {}, extra));
  };
  return followCheck({
    first: first,
    budgetMs: DISCOVERY_FOLLOW_MS,
    wait: api.wait,
    keep: keep,
    cancelled: (error) => cancelled(error, lifecycle),
    signal: (readOptions(lifecycle) || {}).signal,
    read: async (current, options) =>
      (((await api.discovery(ns, current.id, options)) || {}).item) || current,
    show: (current) => paint({ latestAttempt: current }),
  }).then((ended) => {
    if (ended === null || ended === undefined || ended.terminal !== true ||
      followStopped(ended) !== "" || !keep()) {
      return;
    }
    api.latestDiscoveries(ns, name, readOptions(lifecycle)).then(
      (answer) => {
        if (!keep()) {
          return;
        }
        const before = ((((detailViews.get(key) || {}).discovery) || {}).lastSuccessful) || {};
        const best = answer.lastSuccessful || null;
        paint(Object.assign(
          { latestAttempt: answer.latestAttempt || ended, lastSuccessful: best },
          // A NEW LAST INVENTORY IS NOT THE ONE THE TOPIC TABLE PAGED.
          (best || {}).id === before.id ? {} : { topics: null, topicsError: null },
        ));
      },
      () => {
        // The finished attempt is on screen already; the slots catch up on the
        // next visit.
      },
    );
  });
}

/** The discovery panel's three controls: start, cancel, and read a page of the
 *  stored inventory (with the filters, and following the cursor).
 *
 *  A STARTED DISCOVERY IS FOLLOWED (`followDiscovery`), as every check this
 *  console starts is: until a read answers terminal, the route leaves, or the
 *  discovery's own deadline passes -- and then the panel says so and offers
 *  to run it again. It used not to be read again at all, and "Discover
 *  topics" left `pending` on screen until a reload. */
function wireDiscovery(node, ns, name, parse, lifecycle, api, object, view) {
  const key = formKey(ns, DISCOVERY_FORM, name);
  const mutation = mutationFor(key);
  const form = node.querySelector("#discovery-form");
  if (form !== null) {
    watchMutation(node, key, mutation, (state) => {
      if (!active(lifecycle)) {
        return;
      }
      if (state.phase === "succeeded") {
        const made = state.result || {};
        paintClusterDetail(node, ns, name, parse, lifecycle, api, object, Object.assign(
          {}, view,
          {
            latestAttempt: made.item || view.latestAttempt,
            reused: made.reused === true,
          },
        ));
        // A REUSED INVENTORY IS ALREADY AN ANSWER; anything else is followed
        // until it is one.
        if (made.reused !== true && made.item) {
          followDiscovery(node, ns, name, parse, lifecycle, api, made.item);
        }
        return;
      }
      paintClusterDetail(node, ns, name, parse, lifecycle, api, object, view);
    }, lifecycle);

    const start = () => {
      if (!active(lifecycle) || mutation.pending()) {
        return;
      }
      const values = readFormValues(form);
      const request = {};
      if (values.includeInternal === true) {
        request.includeInternal = true;
      }
      const expected = String(values.expectedTopics || "")
        .split(",").map((t) => t.trim()).filter((t) => t.length > 0);
      if (expected.length > 0) {
        request.expectedTopics = expected;
      }
      // ASKED AGAIN, NOT REPLAYED, once the inventory these parameters made is
      // spent (P14's class): a failed or stale discovery is a new key's
      // question, and a fresh identical one is the server's to reuse.
      mutation.run(() => askCheck({
        intent: key,
        question: JSON.stringify(request),
        held: view.latestAttempt || null,
        spent: discoverySpent,
        start: (token) => api.startDiscovery(ns, name, request, { attempt: token }),
      }));
    };
    listen(form, "submit", (event) => {
      event.preventDefault();
      start();
    }, lifecycle);
    // "RUN THE DISCOVERY AGAIN" asks again with the form's parameters; the
    // stopped attempt is spent (`discoverySpent`), so it is a new discovery.
    wireCheckRetry(node, view.latestAttempt, start, lifecycle);

    const cancel = node.querySelector("#discovery-cancel");
    if (cancel !== null) {
      listen(cancel, "click", () => {
        const latest = view.latestAttempt;
        if (!active(lifecycle) || latest === null || latest === undefined) {
          return;
        }
        disableKeepingFocus(cancel, true, node.querySelector("#discovery-status"));
        api.cancelDiscovery(ns, latest.id).then(
          () => {
            if (active(lifecycle)) {
              mountClusterDetail(node, ns, name, parse, lifecycle, api);
            }
          },
          (error) => {
            if (cancelled(error, lifecycle) || !active(lifecycle)) {
              return;
            }
            const status = node.querySelector("#discovery-status");
            if (status !== null) {
              replace(status, parse(errorBlock(error)));
            }
            cancel.disabled = false;
          },
        );
      }, lifecycle);
    }
  }

  const filters = node.querySelector("#topic-filters");
  if (filters !== null) {
    listen(filters, "submit", (event) => {
      event.preventDefault();
      readTopics(node, ns, name, parse, lifecycle, api, object, view,
        readFormValues(filters), null);
    }, lifecycle);
  }
  const more = node.querySelector("#topics-more");
  if (more !== null) {
    listen(more, "click", () => {
      readTopics(node, ns, name, parse, lifecycle, api, object, view,
        view.filters || {}, more.getAttribute("data-cursor"));
    }, lifecycle);
  }
}

/** Reads one page of a stored inventory. THE DISCOVERY IT READS IS THE LAST
 *  SUCCESSFUL ONE, and never the latest attempt: an attempt that failed has no
 *  stored result, and a filter run against it would answer 404 for a reason
 *  that has nothing to do with the filter. */
function readTopics(node, ns, name, parse, lifecycle, api, object, view, filters, cursor) {
  const source = view.lastSuccessful || null;
  if (source === null) {
    return;
  }
  const options = Object.assign({}, readOptions(lifecycle));
  for (const key of ["q", "prefix", "internal", "errored"]) {
    const value = filters[key];
    if (typeof value === "string" && value.length > 0) {
      options[key] = value;
    }
  }
  if (typeof cursor === "string" && cursor.length > 0) {
    options.cursor = cursor;
  }
  api.discoveryTopics(ns, source.id, options).then(
    (page) => {
      if (!active(lifecycle)) {
        return;
      }
      paintClusterDetail(node, ns, name, parse, lifecycle, api, object, Object.assign(
        {}, view, { filters: filters, topics: page, topicsError: null },
      ));
    },
    (error) => {
      if (cancelled(error, lifecycle) || !active(lifecycle)) {
        return;
      }
      paintClusterDetail(node, ns, name, parse, lifecycle, api, object, Object.assign(
        {}, view, { filters: filters, topics: null, topicsError: error },
      ));
    },
  );
}


/** THE "TEST CONNECTION" CONTROL, ON THE DETAIL VIEW.
 *
 *  It is a READ, and the panel says so: the page re-reads this KafkaCluster and
 *  renders whatever the controller has recorded since. It cannot make the
 *  controller dial -- `spec` is immutable, this page's whole write surface is
 *  five creates and one suspend patch, and the re-probe cadence is the probe
 *  Job's own TTL -- so a control that claimed to force a dial would be lying in
 *  the same way a stale probe rendered as current lies.
 *
 *  IT IS BOUND TO THE ROUTE'S LIFETIME LIKE EVERY OTHER READ (PLAT-13.1): the
 *  answer is dropped when the view is gone, so a slow re-read cannot paint a
 *  probe from namespace A over namespace B. */
function wireDetailProbe(node, ns, name, parse, lifecycle, api, discovery, check) {
  const form = node.querySelector("form.probe-test");
  if (form === null) {
    return;
  }
  let reading = false;
  listen(form, "submit", (event) => {
    event.preventDefault();
    if (!active(lifecycle) || reading) {
      return;
    }
    reading = true;
    const button = form.querySelector("button");
    if (button !== null) {
      disableKeepingFocus(button, true);
    }
    api.get(ns, PLURAL, name, readOptions(lifecycle)).then(
      (object) => {
        reading = false;
        if (!active(lifecycle)) {
          return;
        }
        paintClusterDetail(node, ns, name, parse, lifecycle, api, object, discovery, check);
      },
      (error) => {
        reading = false;
        if (cancelled(error, lifecycle) || !active(lifecycle)) {
          return;
        }
        const panel = node.querySelector("#cluster-probe-line");
        if (panel !== null) {
          replace(panel, parse(renderProbeReadFailure(error)));
        }
        if (button !== null) {
          button.disabled = false;
        }
      },
    );
  }, lifecycle);
}

/** What a failed re-read says. THE API SERVER'S OWN reason and message,
 *  verbatim, exactly as `errorBox` renders them elsewhere: the page made no
 *  authorisation decision and must not narrate one, and a re-read that was
 *  refused is not an observation about the broker. */
export function renderProbeReadFailure(error) {
  const e = error || {};
  const reason = typeof e.reason === "string" && e.reason.length > 0 ? e.reason : "";
  const message = typeof e.message === "string" ? e.message : String(e);
  return (
    "<span class=\"refusal\">Test connection could not re-read this KafkaCluster, so the " +
    "observation above is unchanged and is not a statement about right now" +
    (reason.length === 0 ? "" : " (" + esc(reason) + ")") + ": " + esc(message) + "</span>"
  );
}

/** The per-row Test connection controls on the LIST view. Each one re-reads
 *  its own cluster by name and replaces THAT row's probe cells; the rest of the
 *  page -- the create form's draft included -- is untouched, which is why this
 *  is not a re-mount. */
function wireProbeTests(node, ns, parse, lifecycle, api) {
  for (const form of node.querySelectorAll("form.probe-test")) {
    wireRowProbe(node, ns, parse, lifecycle, api, form);
  }
}

function wireRowProbe(node, ns, parse, lifecycle, api, form) {
  const name = form.getAttribute("data-probe-name");
  const uid = form.getAttribute("data-probe-uid");
  let reading = false;
  listen(form, "submit", (event) => {
    event.preventDefault();
    if (!active(lifecycle) || reading || typeof name !== "string" || name.length === 0) {
      return;
    }
    reading = true;
    const button = form.querySelector("button");
    if (button !== null) {
      disableKeepingFocus(button, true);
    }
    api.get(ns, PLURAL, name, readOptions(lifecycle)).then(
      (object) => {
        reading = false;
        if (button !== null) {
          button.disabled = false;
        }
        if (!active(lifecycle)) {
          return;
        }
        // THE UID IS CHECKED BEFORE ANYTHING IS PAINTED. A cluster deleted and
        // recreated under the same name between the list read and this one is
        // a DIFFERENT connection, and writing its probe into the old row would
        // be the exact substitution this task forbids everywhere else.
        if (clusterUid(object) !== uid) {
          paintRow(node, parse, uid, renderRecreatedRow(name, uid, clusterUid(object)));
          return;
        }
        paintRow(node, parse, uid, probeLine(probeState(object)));
      },
      (error) => {
        reading = false;
        if (button !== null) {
          button.disabled = false;
        }
        if (cancelled(error, lifecycle) || !active(lifecycle)) {
          return;
        }
        paintRow(node, parse, uid, renderProbeReadFailure(error));
      },
    );
  }, lifecycle);
}

/** The sentence a row gets when the name it re-read answers to a different
 *  object than the one the row is about. */
export function renderRecreatedRow(name, was, now) {
  return (
    "<span class=\"refusal\">The KafkaCluster named <code>" + esc(name) + "</code> is no longer " +
    "the object this row is about: this row is uid <code>" + esc(was) + "</code> and that name " +
    "now answers to uid <code>" + esc(now) + "</code>. Nothing was painted over: reload the " +
    "list to see what this namespace holds.</span>"
  );
}

/** Replaces the probe cell of the row carrying `uid`. Finds the row by its
 *  recorded identity rather than by counting, so a list that changed under the
 *  read cannot be edited in the wrong place. */
function paintRow(node, parse, uid, html) {
  for (const row of node.querySelectorAll("tr[data-cluster-uid]")) {
    if (row.getAttribute("data-cluster-uid") !== uid) {
      continue;
    }
    // THE PROBE CELL BY ITS CLASS, not by counting columns: the table's
    // columns changed once (R2-3) and a count would repaint the wrong cell.
    const probe = row.querySelector(".probe-cell");
    if (probe !== null) {
      replace(probe, parse(html));
    }
  }
}

/** The form's values, read from the DOM and trimmed where a name is involved. */
/** EVERY NAMED CONTROL OF A FORM, BY NAME, WHATEVER THE FORM IS.
 *
 *  The three readers beside this one know their form's fields and name each
 *  one, which is what makes a missing field a loud `undefined.value` rather
 *  than a silently absent key. This one exists for the forms that DO NOT have
 *  a fixed shape -- the discovery panel's controls, the topic filters, the
 *  readiness panel, the destination form's four repeated grant blocks -- where
 *  naming every field would be a second copy of the markup that renders them.
 *
 *  IT HANDLES BOTH `elements` SHAPES. A browser's `form.elements` is an
 *  array-like collection; the behaviour suite's fake DOM builds a plain object
 *  keyed by name. Reading one shape only would have made every row that drives
 *  one of these forms pass in the suite and throw in a browser, or the reverse
 *  -- and the point of the suite is that those two cannot differ. */
export function readFormValues(form) {
  const values = Object.create(null);
  const bag = (form || {}).elements;
  if (bag === null || bag === undefined) {
    return values;
  }
  const controls = typeof bag.length === "number"
    ? Array.prototype.slice.call(bag)
    : Object.keys(bag).map((key) => bag[key]);
  for (const element of controls) {
    if (element === null || element === undefined) {
      continue;
    }
    const name = element.name === undefined
      ? (typeof element.getAttribute === "function" ? element.getAttribute("name") : null)
      : element.name;
    if (typeof name !== "string" || name.length === 0) {
      continue;
    }
    const type = element.type || (element.tagName === "SELECT" ? "select-one" : "text");
    if (type === "checkbox") {
      values[name] = element.checked === true;
    } else if (type === "radio") {
      if (element.checked === true) {
        values[name] = element.value;
      } else if (values[name] === undefined) {
        values[name] = "";
      }
    } else if (type === "select-multiple") {
      values[name] = Array.prototype.slice
        .call(element.selectedOptions || [])
        .map((option) => option.value);
    } else {
      values[name] = element.value;
    }
  }
  return values;
}

export function readClusterValues(form) {
  const e = form.elements;
  return {
    // ABSENT IN CONSOLE MODE (P7): the server names the connection.
    name: e.name === undefined ? "" : String(e.name.value).trim(),
    servers: String(e.servers.value),
    role: String(e.role.value).trim(),
    mode: String(e.mode.value),
    username: String(e.username.value).trim(),
    secret: String(e.secret.value).trim(),
    passwordKey: String(e.passwordKey.value).trim(),
    tls: e.tls.checked === true,
    tlsCaKind: String(e.tlsCaKind.value),
    tlsCaName: String(e.tlsCaName.value).trim(),
    tlsCaKey: String(e.tlsCaKey.value).trim(),
  };
}

function wireForm(node, ns, parse, lifecycle, api) {
  const form = node.querySelector("#cluster-form");
  if (form === null) {
    return;
  }
  const key = formKey(ns, CLUSTER_FORM);
  const mutation = mutationFor(key);

  // EVERY KEYSTROKE IS KEPT, so whatever happens to this view next -- an
  // answer that re-renders the form, or a route change -- the draft is intact.
  const remember = () => {
    if (!active(lifecycle)) {
      return;
    }
    // THE INTENT SURVIVES EVERY KEYSTROKE: it is minted on the first submit
    // and belongs to the draft until the connection exists (P7).
    keepDraft(key, withIntent(readClusterValues(form), readDraft(key)), CLUSTER_DRAFT_FIELDS);
    if (mutation.state.phase === "succeeded") {
      mutation.clear();
      const status = node.querySelector("#cluster-form-status");
      if (status !== null) {
        replace(status, []);
      }
    }
  };
  listen(form, "input", remember, lifecycle);
  listen(form, "change", remember, lifecycle);

  watchMutation(node, key, mutation, (state) => {
    if (state.phase === "succeeded") {
      dropDraft(key);
      mountClusters(node, ns, parse, lifecycle, api);
      return;
    }
    const slot = node.querySelector("#cluster-form-slot");
    if (slot === null) {
      return;
    }
    replace(slot, parse(renderClusterForm(clusterFormView(ns))));
    wireForm(node, ns, parse, lifecycle, api);
    if (state.phase === "failed") {
      focusFirstProblem(node, "#cluster-form-status");
    }
  }, lifecycle);

  listen(form, "submit", (event) => {
    event.preventDefault();
    if (!active(lifecycle) || mutation.pending()) {
      return;
    }
    const values = withIntent(readClusterValues(form), readDraft(key));
    keepDraft(key, values, CLUSTER_DRAFT_FIELDS);
    mutation.run(async () => {
      // MINTED INSIDE THE EXECUTOR, once per draft: a double click, a retry
      // after a timeout and a resend after a refusal all carry the same one.
      if (connectionNamesMinted() && !values.intent) {
        values.intent = mintConnectionIntent();
        keepDraft(key, values, CLUSTER_DRAFT_FIELDS);
      }
      return submitCluster(ns, values, api);
    });
  }, lifecycle);
}

/** `values` carrying the intent `draft` already holds, if it holds one. */
function withIntent(values, draft) {
  const intent = (draft || {}).intent;
  return typeof intent === "string" && intent.length > 0
    ? Object.assign(values, { intent: intent })
    : values;
}

/** Moves focus to the first field marked invalid, or to the status region, so
 *  a keyboard or screen-reader user lands on what needs doing. */
export function focusFirstProblem(node, statusSelector) {
  const target = node.querySelector("[aria-invalid=\"true\"]") || node.querySelector(statusSelector);
  if (target !== null && typeof target.focus === "function") {
    target.focus();
  }
}
