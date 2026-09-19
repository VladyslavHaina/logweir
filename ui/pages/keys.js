// pages/keys.js -- THE KEYS VIEW, READ-ONLY, AND `unknown` IS NOT `valid`.
//
// THIS PAGE WRITES NOTHING, AND THE PLURALS IT READS ARE NOT WRITABLE. Both
// `TrustPolicy` and `TrustRoster` are cluster-scoped and admin-only: a
// namespace-scoped adopter cannot edit either at all, neither plural is in the
// frozen writable set in `api.js`, and the product API serves no trust write
// in v1 at all (`capabilities.trustAdministration: false`, D3 section 10). This page
// never calls either writer; `the_keys_page_submits_nothing` asserts that
// twice -- once with a stub whose writers throw, and once by reading this file.
//
// SO IT SURFACES THE `kubectl apply` SNIPPET AND DOES NOT SUBMIT IT. That is
// the honest shape for an admin-only step: show the exact document a cluster
// admin applies, and let them apply it.
//
// AND IT PRINTS THE FINGERPRINT COMMAND, because that is the only step that
// catches an undisclosed key rotation. A row says which key id the controller
// will accept; it cannot say that the key material behind that id is still the
// one its holder has. Comparing the fingerprint out of band, against the
// person who holds the key, is what does.
//
// ===========================================================================
// WHAT D3 section 7.7 CHANGED, AND WHY IT IS THE POINT OF THIS FILE
// ===========================================================================
//
// THE OLD RULE WAS WRONG IN A WAY THAT ALWAYS READ GREEN. The roster view
// printed `valid` for any key whose id was not in `status.expiredKeyIds[]` --
// INCLUDING when the object carried no status at all, which is what an
// unreconciled roster, a controller that is not running and a roster whose
// every key failed to parse all look like. "Nothing evaluated this" and "this
// is valid" rendered identically, and the one that is a fault rendered as the
// one that is fine.
//
// THE NEW RULE HAS A THIRD WORD. An evaluation is FRESH only when all of:
// the object carries a `status`; `status.observedGeneration` equals
// `metadata.generation`, so the verdicts are about the spec on screen; and
// `status.evaluatedAt` is inside the freshness window measured against the
// SERVER's clock. Anything else is `unknown`. `valid` and `expired` are
// rendered only for a fresh evaluation, and `unknown` says which of the four
// causes it was.
//
// THE CLOCK IS THE SERVER'S AND NEVER THE BROWSER'S. `ui/api.js`'s
// `serverClock()` is the `Date` header of the most recent answer -- the
// `logweir-api` process in console mode, kube-apiserver through `kubectl
// proxy` in legacy mode. With no server instant at all, freshness has not been
// established and every row reads `unknown`: that is the fail-closed side, and
// it is the side this page takes.
//
// RETIREMENT AND REVOCATION ARE DIFFERENT AND ARE EXPLAINED AS SUCH. A retired
// key still verifies everything it signed before it was retired (`Historical`)
// and authorises nothing new. A key revoked for compromise does NOT get that
// courtesy: its own claimed signing time is attacker-controlled, so only a
// controller's earlier independent observation counts, and without one the
// evidence fails closed.

import { apiClient, mode, CONSOLE } from "../client.js";
import { active, cancelled, readOptions } from "../lifecycle.js";
import {
  ABSENT,
  EVALUATION_UNKNOWN,
  EVALUATION_UNKNOWN_REASONS,
  UNKNOWN_IS_NOT_VALID_SENTENCE,
  badge,
  cell,
  copyBlock,
  errorBox,
  esc,
  evaluationWord,
  facts,
  listFooter,
  replace,
  table,
} from "../render.js";
import { listD3, serverClock } from "../operation-watch.js";
import { itemsOf } from "./clusters.js";

const PLURAL = "trustrosters";

/** `weirkeeper::ROSTER_NAME` -- the ONE cluster-scoped roster this product
 *  reads (interface I16). There is no per-namespace roster and no second
 *  name. */
export const ROSTER_NAME = "default";

/** The out-of-band fingerprint command. It takes the PUBLIC half and produces
 *  the same digest `logweir` uses as a key id, so an approver can read theirs
 *  aloud and an operator can compare it with a row below. */
export const FINGERPRINT_COMMAND =
  "openssl pkey -pubin -outform DER -in <key>.pub.pem | openssl dgst -sha256";

/** How recent an evaluation has to be to be rendered as a verdict at all
 *  (D3 section 7.7). Fifteen minutes, measured against the SERVER's clock. */
export const EVALUATION_FRESHNESS_MS = 900000;

const API = apiClient();

/** The roster named `default` out of a collection, or `null`. */
export function rosterOf(collection) {
  for (const object of itemsOf(collection)) {
    if (((object.metadata || {}).name) === ROSTER_NAME) {
      return object;
    }
  }
  return null;
}

/** WHETHER AN EVALUATION IS FRESH ENOUGH TO BE RENDERED AS A VERDICT.
 *
 *  PURE, AND THE SERVER INSTANT IS AN ARGUMENT. `now` is
 *  the server clock `ui/api.js` recorded -- the instant the cluster dated its own answer
 *  by. Handing it in rather than reading it here is what lets the suite drive
 *  every branch, and what stops a future edit reaching for `Date.now()`
 *  because it was convenient.
 *
 *  Returns `{fresh, reason}`. `reason` is one of
 *  [`EVALUATION_UNKNOWN_REASONS`]' keys when `fresh` is false. */
export function evaluationFreshness(object, now, windowMs) {
  const meta = (object && object.metadata) || {};
  const status = (object && object.status) || null;
  if (status === null || Object.keys(status).length === 0) {
    return { fresh: false, reason: "NoStatus" };
  }
  if (typeof meta.generation === "number" && status.observedGeneration !== meta.generation) {
    return { fresh: false, reason: "GenerationBehind" };
  }
  const evaluatedAt = typeof status.evaluatedAt === "string" ? Date.parse(status.evaluatedAt) : NaN;
  if (isNaN(evaluatedAt)) {
    return { fresh: false, reason: "NoStatus" };
  }
  if (typeof now !== "number" || !isFinite(now)) {
    // NO SERVER INSTANT IS NOT A FRESH ONE. Freshness has not been
    // established, and `unknown` is what "not established" reads as -- under
    // its OWN reason, because "this evaluation is old" and "this page has no
    // clock the cluster saw" are two different things and explaining one with
    // the other's sentence is a page saying something that did not happen.
    return { fresh: false, reason: "NoServerClock" };
  }
  const budget = typeof windowMs === "number" && windowMs > 0 ? windowMs : EVALUATION_FRESHNESS_MS;
  return now - evaluatedAt > budget
    ? { fresh: false, reason: "Stale" }
    : { fresh: true, reason: null };
}

/** The verdict recorded for one key id, or `null`. */
export function verdictFor(status, keyId) {
  for (const verdict of Array.isArray((status || {}).keys) ? status.keys : []) {
    if ((verdict || {}).keyId === keyId) {
      return verdict;
    }
  }
  return null;
}

/** THE EVALUATION CELL. One word, and a title saying why when it is
 *  `unknown`. */
export function evaluationCell(freshness, verdict) {
  if (freshness.fresh !== true) {
    return badge("flat", EVALUATION_UNKNOWN) + " " +
      esc(EVALUATION_UNKNOWN_REASONS[freshness.reason] || EVALUATION_UNKNOWN_REASONS.NoVerdict);
  }
  if (verdict === null) {
    return badge("flat", EVALUATION_UNKNOWN) + " " +
      esc(EVALUATION_UNKNOWN_REASONS.NoVerdict);
  }
  const state = evaluationWord(true, verdict.effectiveState);
  const kind = state === "Active" ? "green" : (state === "Unparseable" ? "unverified" : "warn");
  return badge(kind, state);
}

/** What a key may still do, in words and never as two ticks. */
export function usabilityCell(freshness, verdict) {
  if (freshness.fresh !== true || verdict === null) {
    return EVALUATION_UNKNOWN;
  }
  const signing = verdict.usableForNewSignatures === true
    ? "may sign something new"
    : "may not sign anything new";
  const verification = typeof verdict.usableForVerification === "string"
    ? verdict.usableForVerification
    : EVALUATION_UNKNOWN;
  return esc(signing + "; verification: " + verification);
}

/** THE TRUST POLICY'S KEY TABLE.
 *
 *  Seven columns, and the two that matter most are the last two: what the
 *  controller decided about this key, and whether that decision is one this
 *  page is allowed to render at all. */
export function renderPolicyKeys(object, now) {
  const spec = (object && object.spec) || {};
  const status = (object && object.status) || {};
  const freshness = evaluationFreshness(object, now, EVALUATION_FRESHNESS_MS);
  const rows = (Array.isArray(spec.keys) ? spec.keys : []).map((k) => {
    const entry = k || {};
    const verdict = verdictFor(status, entry.keyId);
    const usages = Array.isArray(entry.usages) ? entry.usages : [];
    return [
      "<code>" + cell(entry.keyId) + "</code>",
      cell((entry.principal || {}).display) + " " +
        "<code>" + cell((entry.principal || {}).id) + "</code>",
      usages.length === 0 ? ABSENT : esc(usages.join(", ")),
      cell(entry.state),
      cell(entry.notBefore) + " " + ARROW_TO + " " + cell(entry.notAfter),
      lifecycleCell(entry),
      evaluationCell(freshness, verdict),
      usabilityCell(freshness, verdict),
    ];
  });
  return (
    "<h3>Keys</h3>" +
    "<p class=\"note\" data-evaluation-fresh=\"" + (freshness.fresh ? "true" : "false") + "\">" +
    esc(UNKNOWN_IS_NOT_VALID_SENTENCE) + "</p>" +
    table(
      ["KEY ID", "PRINCIPAL", "USAGES", "STATE", "VALIDITY", "LIFECYCLE", "EVALUATION", "MAY"],
      rows,
      "This policy carries no key. A policy with no key verifies nothing and authorises nothing.",
    )
  );
}

// The arrow between the two validity bounds. `render.js`'s ARROW is the same
// character; it is spelled here as its own constant so this file's one
// non-ASCII byte has a name.
const ARROW_TO = "->";

/** Retirement and revocation, per key, in the words that distinguish them. */
export function lifecycleCell(entry) {
  const e = entry || {};
  if (e.state === "Revoked") {
    const reason = String(e.revocationReason || "Unspecified");
    const effective = cell(e.revocationEffectiveFrom);
    return reason === "KeyCompromise"
      ? esc("revoked for compromise, effective ") + effective + " " +
        esc("-- evidence it signed verifies only where a controller recorded seeing it before " +
          "that instant; the document's own claimed signing time is not accepted here")
      : esc("revoked (" + reason + "), effective ") + effective + " " +
        esc("-- treated as a retirement at that instant: what it signed before still verifies");
  }
  if (e.state === "Retired") {
    return esc("retired at ") + cell(e.retiredAt) + " " +
      esc("-- it authorises nothing new, and everything it signed before that instant still " +
        "verifies");
  }
  return esc("active -- it may sign new documents inside its validity window");
}

/** The policy's own facts, including the CONFLICT case, which resolves to
 *  nothing rather than to one of the two policies. */
export function renderPolicyFacts(object, now) {
  const spec = (object && object.spec) || {};
  const status = (object && object.status) || {};
  const meta = (object && object.metadata) || {};
  const freshness = evaluationFreshness(object, now, EVALUATION_FRESHNESS_MS);
  const conflicts = Array.isArray(status.conflicts) ? status.conflicts : [];
  const bound = Array.isArray(status.boundNamespaces) ? status.boundNamespaces : [];
  return (
    facts([
      ["policy", cell(meta.name)],
      ["generation", cell(meta.generation)],
      ["observed generation", cell(status.observedGeneration)],
      ["evaluated at", cell(status.evaluatedAt)],
      ["evaluation", freshness.fresh
        ? badge("green", "fresh")
        : badge("flat", EVALUATION_UNKNOWN) + " " +
          esc(EVALUATION_UNKNOWN_REASONS[freshness.reason] || "")],
      ["loaded", cell(status.loaded)],
      ["default policy", cell(spec.default)],
      ["namespaces it claims", Array.isArray(spec.namespaces) && spec.namespaces.length > 0
        ? esc(spec.namespaces.join(", "))
        : ABSENT],
      ["namespaces it governs", bound.length === 0 ? ABSENT : esc(bound.join(", "))],
      ["allowed target cluster ids",
        Array.isArray(spec.allowedTargetClusterIds) && spec.allowedTargetClusterIds.length > 0
          ? esc(spec.allowedTargetClusterIds.join(", "))
          : ABSENT],
    ]) +
    (conflicts.length === 0
      ? ""
      : "<p class=\"complaint\" data-trust-conflict=\"true\">" +
        esc(CONFLICT_SENTENCE) + " " +
        esc(conflicts.map((c) => String((c || {}).namespace) + " (" +
          (Array.isArray((c || {}).policies) ? c.policies.join(", ") : "") + ")").join("; ")) +
        "</p>")
  );
}

/** What a contested namespace means. It resolves to NOTHING -- not to one of
 *  the two policies -- and every approval and verification in it is refused. */
export const CONFLICT_SENTENCE =
  "These namespaces are claimed by more than one TrustPolicy, so each of them resolves to no " +
  "policy at all: every approval and every evidence verification in them is refused with " +
  "TrustPolicyConflict rather than silently taking one policy's answer.";

/** The whole page.
 *
 *  `view` is `{policies, roster, reason, now}`. A bare COLLECTION is accepted
 *  too and read as "no policy, this roster, and no server instant" -- which is
 *  the `kubectl proxy` shape this page was called with before D3, and which
 *  now renders every roster verdict the roster's own status supports and
 *  `unknown` for the rest. */
export function renderKeysPage(view) {
  const v = looksLikeCollection(view)
    ? { policies: [], roster: rosterOf(view), now: null, reason: "" }
    : (view || {});
  const policies = Array.isArray(v.policies) ? v.policies : [];
  const head =
    "<h2>Keys</h2>" +
    "<p class=\"blurb\">The cluster-scoped trust material this installation verifies against: " +
    "which keys exist, what each one is allowed to do, where it is in its lifecycle, and what " +
    "the controller last decided about it. This page reads; it submits nothing.</p>";
  if (policies.length === 0) {
    return head + renderRosterHalf(v) + renderFingerprint() + renderPolicySnippet() + listFooter();
  }
  return (
    head +
    policies.map((object) =>
      "<section class=\"trust-policy\">" +
      renderPolicyFacts(object, v.now) +
      renderPolicyKeys(object, v.now) +
      "</section>"
    ).join("") +
    renderFingerprint() +
    renderPolicySnippet() +
    (v.roster === null || v.roster === undefined ? "" : renderLegacyRosterNote()) +
    listFooter()
  );
}

/** Whether this is a Kubernetes collection rather than a view bag. */
function looksLikeCollection(input) {
  return Array.isArray(input) ||
    (input !== null && typeof input === "object" && Array.isArray(input.items));
}

/** Why a roster is still on screen once a policy exists. */
export function renderLegacyRosterNote() {
  return (
    "<p class=\"note\">A TrustRoster named " + esc(ROSTER_NAME) + " also exists in this cluster. " +
    "It is NOT deleted by migration and it is not consulted for a namespace a TrustPolicy " +
    "governs; it is what an older controller reached by rollback would still read.</p>"
  );
}

/** The roster half: what this page shows when no TrustPolicy is readable.
 *
 *  THE ROSTER'S OWN COLUMN IS `unknown` TOO. The old page printed `valid` for
 *  any key not in `status.expiredKeyIds[]`, including for a roster with no
 *  status at all. The roster has no `observedGeneration` and no `evaluatedAt`,
 *  so there is nothing to establish freshness FROM -- and D3 section 7.7's answer to
 *  that is the same word it is everywhere else. */
export function renderRosterHalf(view) {
  const v = view || {};
  const roster = v.roster || null;
  if (roster === null) {
    return (
      "<p class=\"complaint\">" + esc(v.reason || NO_TRUST_SENTENCE) + "</p>"
    );
  }
  const spec = roster.spec || {};
  const status = roster.status || {};
  return (
    "<p class=\"note\">" + esc(ROSTER_FALLBACK_SENTENCE) + "</p>" +
    facts([
      ["loaded", cell(status.loaded)],
      ["allowed cluster ids",
        (Array.isArray(spec.allowedClusterIds) ? spec.allowedClusterIds : []).length === 0
          ? cell(null)
          : esc(spec.allowedClusterIds.join(", "))],
    ]) +
    renderRosterKeys("approverKeys", spec.approverKeys, status) +
    renderRosterKeys("signingKeys", spec.signingKeys, status)
  );
}

/** One roster key list. The EXPIRY column is the controller's own
 *  `status.expiredKeyIds[]` where there is a status, and `unknown` where there
 *  is not -- never `valid`. */
export function renderRosterKeys(caption, entries, status) {
  const evaluated = status !== null && status !== undefined &&
    Array.isArray(status.expiredKeyIds);
  const expired = evaluated ? status.expiredKeyIds : [];
  const rows = (Array.isArray(entries) ? entries : []).map((entry) => {
    const e = entry || {};
    return [
      "<code>" + esc(e.keyId) + "</code>",
      cell(e.subject),
      cell(e.notAfter),
      evaluated
        ? (expired.indexOf(e.keyId) === -1 ? badge("green", "valid") : badge("warn", "expired"))
        : badge("flat", EVALUATION_UNKNOWN) + " " +
          esc(EVALUATION_UNKNOWN_REASONS.NoStatus),
    ];
  });
  return (
    "<h3>" + esc(caption) + "</h3>" +
    table(
      ["KEY ID", "SUBJECT", "NOT AFTER", "EVALUATION"],
      rows,
      "no key of this kind in the roster",
    )
  );
}

/** The sentence this page carries when neither a policy nor a roster answers. */
export const NO_TRUST_SENTENCE =
  "No TrustPolicy and no TrustRoster named " + ROSTER_NAME + " could be read from this cluster. " +
  "Both are cluster-scoped and admin-only; until one exists and this page's identity may read " +
  "it, no approval can verify and no evidence can be checked against a key.";

/** Why a roster is what is on screen. */
export const ROSTER_FALLBACK_SENTENCE =
  "No TrustPolicy answered, so what is shown is the legacy TrustRoster named default -- which is " +
  "what a controller with no policy in the cluster synthesizes `legacy-roster-v1` from. " +
  "approverKeys authorise an approval; signingKeys are what weirkeeper verifies signed evidence " +
  "against.";

/** The fingerprint block. */
export function renderFingerprint() {
  return (
    "<section class=\"check\"><h3>Check a key out of band</h3>" +
    "<p class=\"note\">A row above says which key id the controller accepts. It cannot say " +
    "that the material behind that id is still the one its holder has. Run this against " +
    "the public half they give you and compare the digest with the KEY ID column.</p>" +
    copyBlock([FINGERPRINT_COMMAND]) +
    "</section>"
  );
}

/** THE SNIPPET A CLUSTER ADMIN APPLIES. RENDERED AND NEVER SUBMITTED.
 *
 *  It is a TrustPolicy and no longer a roster, because the policy is what the
 *  lifecycle lives on: a roster cannot express a retirement, an overlap window
 *  or a usage, and D3 section 7.2 replaces every one of its fields. The migration
 *  command is beside it, because an existing roster is not edited by hand into
 *  a policy -- `logweir trust migrate-roster` produces one and the roster is
 *  left in place for rollback. */
export function renderPolicySnippet() {
  return (
    "<section class=\"check\"><h3>Editing trust is a cluster-admin step</h3>" +
    "<p class=\"note\">This page shows the document and does not apply it. `trustpolicies` is " +
    "cluster-scoped, its plural is absent from this page's writable set, the product API serves " +
    "no trust write in v1, and the API server would refuse a namespace-scoped viewer in any " +
    "case. Save this as trustpolicy.yml and apply it yourself. Editing is MONOTONIC: a key's " +
    "public material can never be edited out, `notAfter` may only move earlier, and `state` may " +
    "only move Active to Retired to Revoked.</p>" +
    copyBlock([
      "# From an existing roster, the reviewable way across is a migration, not a rewrite:",
      "#   kubectl --context <ctx> get trustroster default -o json \\",
      "#     | logweir trust migrate-roster --stdin --name org-default --default > trustpolicy.yml",
      "",
      "apiVersion: logweir.dev/v1alpha1",
      "kind: TrustPolicy",
      "metadata:",
      "  name: org-default",
      "spec:",
      "  default: false",
      "  namespaces: [<the namespaces this policy governs>]",
      "  allowedTargetClusterIds: []",
      "  keys:",
      "    - keyId: <sha256 of the DER SPKI, lowercase hex>",
      "      spkiPem: |",
      "        -----BEGIN PUBLIC KEY-----",
      "        <the PUBLIC half; never a private key, here or anywhere>",
      "        -----END PUBLIC KEY-----",
      "      algorithm: p256",
      "      usages: [EvidenceSigning]",
      "      principal: {id: \"install:<who holds it>\", display: \"<what it is>\"}",
      "      notBefore: \"2026-01-01T00:00:00Z\"",
      "      notAfter: \"2027-01-01T00:00:00Z\"",
      "      state: Active",
      "",
      "kubectl --context <ctx> apply -f trustpolicy.yml",
    ]) +
    "</section>"
  );
}

// --------------------------------------------------------------- mount half

/** Reads the cluster's trust material and renders it.
 *
 *  TWO READS, AND THE SECOND IS A FALLBACK AND NOT A FAILURE. The policy read
 *  is the one D3 section 7 makes authoritative. Where it is refused -- an identity
 *  with no grant on `trustpolicies`, which is exactly what the chart's own UI
 *  ServiceAccount holds today, or a build whose product API has no trust route
 *  yet -- the roster is read instead and the page says which it is looking at.
 *  A refusal is never absorbed into an empty table. */
export async function mountKeys(node, parse, deps, lifecycle) {
  // THE SEAM TAKES EITHER SHAPE. Callers before D3 handed this the API object
  // itself; the D3 reads need a bag (the mode probe, the server clock). An
  // object that can `listCluster` IS the API object, and is read as one.
  const d = deps !== null && deps !== undefined && typeof deps.listCluster === "function"
    ? { api: deps }
    : (deps || {});
  const api = d.api || API;
  const view = { policies: [], roster: null, now: (d.serverClock || serverClock)(), reason: "" };
  try {
    const collection = await listD3("trust", "", readOptions(lifecycle), d);
    view.policies = itemsOf(collection);
  } catch (error) {
    if (cancelled(error, lifecycle)) {
      return;
    }
    view.reason = describeRefusal(error);
  }
  try {
    view.roster = rosterOf(await api.listCluster(PLURAL, readOptions(lifecycle)));
  } catch (error) {
    if (cancelled(error, lifecycle)) {
      return;
    }
    // A ROSTER REFUSAL IS ONLY WORTH REPORTING WHEN IT IS THE LAST THING LEFT.
    // With a policy on screen the roster is a footnote about rollback, and a
    // 403 on it says nothing this reader needs. With no policy it is the whole
    // page, and its refusal is what goes on screen.
    if (view.policies.length === 0 && view.reason.length === 0) {
      view.reason = describeRefusal(error);
    }
  }
  view.now = (d.serverClock || serverClock)();
  if (active(lifecycle)) {
    replace(node, parse(renderKeysPage(view)));
  }
}

/** A refusal, named, so the page says which read failed and why rather than
 *  showing an empty table. */
function describeRefusal(error) {
  const e = error || {};
  const status = typeof e.status === "number" ? String(e.status) + " " : "";
  const reason = typeof e.reason === "string" && e.reason.length > 0 ? e.reason + ": " : "";
  return NO_TRUST_SENTENCE + " The trust policy read answered " + status + reason +
    String(e.message || "");
}

/** Whether this page is talking to the product API. Exported so the suite can
 *  assert the page reads the mode rather than deciding one. */
export function consoleMode() {
  return mode() === CONSOLE;
}
