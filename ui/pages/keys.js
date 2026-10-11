// pages/keys.js -- THE KEYS VIEW, READ-ONLY, AND `unknown` IS NOT `valid`.
//
// THIS PAGE WRITES NOTHING. `TrustPolicy` and `TrustRoster` are
// cluster-scoped and admin-only, neither plural is in `api.js`'s writable
// set, and the product API serves no trust write in v1
// (`capabilities.trustAdministration: false`, D3 section 10);
// `the_keys_page_submits_nothing` asserts it with a stub whose writers throw
// and by reading this file. So it shows the exact `kubectl apply` document a
// cluster admin applies, and does not submit it.
//
// IT PRINTS THE FINGERPRINT COMMAND, because comparing the fingerprint out of
// band with the key's holder is the only step that catches an undisclosed
// key rotation: a row says which key id the controller accepts, not that the
// material behind it is still the holder's.
//
// THE THIRD WORD (D3 section 7.7). A verdict is rendered only for a FRESH
// evaluation: the object carries a `status`, `status.observedGeneration`
// equals `metadata.generation` (the verdicts are about the spec on screen),
// and `status.evaluatedAt` is inside the freshness window measured against
// the SERVER's clock. Anything else is `unknown`, saying which cause it was
// -- because "nothing evaluated this" (no status: unreconciled, no
// controller, unparseable keys) must never read as "valid".
//
// THE CLOCK IS THE SERVER'S AND NEVER THE BROWSER'S: `ui/api.js`'s
// `serverClock()`, the `Date` header of the most recent answer. With no
// server instant, freshness is not established and every row reads
// `unknown` -- the fail-closed side.
//
// RETIREMENT AND REVOCATION ARE DIFFERENT. A retired key still verifies
// everything it signed before retirement (`Historical`) and authorises
// nothing new. A key revoked for compromise gets no such courtesy: its own
// claimed signing time is attacker-controlled, so only a controller's
// earlier independent observation counts, and without one the evidence
// fails closed.

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
  inert,
  evaluationWord,
  facts,
  listFooter,
  messageText,
  replace,
  table,
  flagBadge,
  when,
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
 *  Pure, with the server instant as an argument: `now` is the server clock
 *  `ui/api.js` recorded, so the suite can drive every branch and nothing here
 *  can reach for `Date.now()`. Returns `{fresh, reason}`; `reason` is one of
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
    // NO SERVER INSTANT IS NOT A FRESH ONE, and it has its OWN reason: "this
    // evaluation is old" and "this page has no clock the cluster saw" are
    // different things.
    return { fresh: false, reason: "NoServerClock" };
  }
  const budget = typeof windowMs === "number" && windowMs > 0 ? windowMs : EVALUATION_FRESHNESS_MS;
  return now - evaluatedAt > budget
    ? { fresh: false, reason: "Stale" }
    : { fresh: true, reason: null };
}

/** THE FRESHNESS ANSWER FOR ONE POLICY, FROM WHICHEVER DOCUMENT IT CAME.
 *
 *  Console mode: the API decided it. `TrustPolicyView.evaluation` carries
 *  `state` (`fresh` or `unknown`), `reason` (one of D3 section 7.7's five
 *  causes), the server instant, `freshWithinSeconds` and the evaluation's
 *  age, so the page renders the answer rather than a second opinion, and the
 *  console and the controller cannot disagree.
 *
 *  Legacy mode: the page decides it, by [`evaluationFreshness`], against the
 *  `Date` header of the answer that carried the object -- still a SERVER
 *  clock.
 *
 *  Returns `{fresh, reason, decidedBy}`. `reason` is a key of
 *  [`EVALUATION_UNKNOWN_REASONS`]; the API's five map onto four of them one
 *  for one, and `evaluated` never reaches a reason because it only
 *  accompanies `fresh`. */
export const API_EVALUATION_REASONS = Object.freeze({
  notEvaluated: "NoStatus",
  generationBehind: "GenerationBehind",
  noEvaluationTime: "NoStatus",
  stale: "Stale",
  evaluated: "NoVerdict",
});

export function evaluationOf(object, now, windowMs) {
  const published = (object || {}).__evaluation;
  if (published !== null && published !== undefined && typeof published === "object") {
    const fresh = published.state === "fresh";
    return {
      fresh: fresh,
      reason: fresh ? null : (API_EVALUATION_REASONS[published.reason] || "NoVerdict"),
      decidedBy: "api",
      decidedAt: published.decidedAt,
      ageSeconds: published.ageSeconds,
      freshWithinSeconds: published.freshWithinSeconds,
    };
  }
  const own = evaluationFreshness(object, now, windowMs);
  return { fresh: own.fresh, reason: own.reason, decidedBy: "page" };
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

/** THE TRUST POLICY'S KEY TABLE. The two columns that matter most are the
 *  last two: what the controller decided about this key, and whether that
 *  decision is one this page may render at all. */
export function renderPolicyKeys(object, now) {
  const spec = (object && object.spec) || {};
  const status = (object && object.status) || {};
  const freshness = evaluationOf(object, now, EVALUATION_FRESHNESS_MS);
  const rows = (Array.isArray(spec.keys) ? spec.keys : []).map((k) => {
    const entry = k || {};
    const verdict = verdictFor(status, entry.keyId);
    const usages = Array.isArray(entry.usages) ? entry.usages : [];
    return [
      "<code>" + cell(entry.keyId) + "</code>",
      // The usages and the declared state under the principal: both are facts
      // about the entry it names, and eight columns overflow the card at 1024 px.
      cell((entry.principal || {}).display) + " " +
        "<code>" + cell((entry.principal || {}).id) + "</code>" +
        "<span class=\"cell-sub\">" + (usages.length === 0 ? ABSENT : esc(usages.join(", "))) +
        " &middot; declared " + cell(entry.state) + "</span>",
      cell(entry.notBefore) + " " + ARROW_TO + " " + cell(entry.notAfter),
      lifecycleCell(entry, freshness, verdict),
      evaluationCell(freshness, verdict),
      usabilityCell(freshness, verdict),
    ];
  });
  return (
    "<h3>Keys</h3>" +
    "<p class=\"note\" data-evaluation-fresh=\"" + (freshness.fresh ? "true" : "false") + "\">" +
    messageText(UNKNOWN_IS_NOT_VALID_SENTENCE) + "</p>" +
    table(
      ["KEY ID", "PRINCIPAL", "VALIDITY", "LIFECYCLE", "EVALUATION", "MAY"],
      rows,
      "This policy carries no key. A policy with no key verifies nothing and authorises nothing.",
      undefined,
      { id: "trust-keys", label: "keys" },
    )
  );
}

// The arrow between the two validity bounds. `render.js`'s ARROW is the same
// character; it is spelled here as its own constant so this file's one
// non-ASCII byte has a name.
const ARROW_TO = "->";

/** Retirement and revocation, per key, in the words that distinguish them.
 *
 *  A KEY THIS POLICY DECLARES ALIVE CAN BE EVALUATED `Revoked`: a
 *  `KeyCompromise` revocation recorded on ANY TrustPolicy applies everywhere.
 *  "Active -- it may sign new documents" beside that verdict would vouch for
 *  a compromised key, so a FRESH `Revoked` verdict over a declared
 *  `Active`/`Retired` key says where the revocation lives instead. An
 *  unfresh verdict is not read at all: `unknown` is neither `valid` nor
 *  `revoked`. */
export function lifecycleCell(entry, freshness, verdict) {
  const e = entry || {};
  const evaluated = freshness !== undefined && freshness !== null && freshness.fresh === true &&
    verdict !== undefined && verdict !== null ? verdict.effectiveState : null;
  if (evaluated === "Revoked" && e.state !== "Revoked") {
    return esc("revoked for compromise by another TrustPolicy's record -- this policy still " +
      "declares it " + String(e.state || "") + ", and nothing it signed or authorises is " +
      "accepted here; record the revocation on this policy too so an older controller agrees " +
      "(see the compromise guard above)");
  }
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
    return esc("retired at ") + when(e.retiredAt) + " " +
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
  const freshness = evaluationOf(object, now, EVALUATION_FRESHNESS_MS);
  const conflicts = Array.isArray(status.conflicts) ? status.conflicts : [];
  const bound = Array.isArray(status.boundNamespaces) ? status.boundNamespaces : [];
  return (
    facts([
      ["policy", cell(meta.name)],
      ["generation", cell(meta.generation)],
      ["observed generation", cell(status.observedGeneration)],
      ["evaluated at", when(status.evaluatedAt)],
      ["evaluation", freshness.fresh
        ? badge("green", "fresh")
        : badge("flat", EVALUATION_UNKNOWN) + " " +
          esc(EVALUATION_UNKNOWN_REASONS[freshness.reason] || "")],
      ["freshness decided by", freshness.decidedBy === "api"
        ? esc("the product API, against its own clock ") + when(freshness.decidedAt) +
          esc(", within " + String(freshness.freshWithinSeconds || "") + "s")
        : esc("this page, against the server instant of the answer that carried this object")],
      ["loaded", flagBadge(status.loaded, "loaded", "not loaded")],
      ["default policy", flagBadge(spec.default, "the default policy", "not the default")],
      ["namespaces it claims", Array.isArray(spec.namespaces) && spec.namespaces.length > 0
        ? esc(spec.namespaces.join(", "))
        : ABSENT],
      ["namespaces it governs", bound.length === 0 ? ABSENT : esc(bound.join(", "))],
      // THE LIST MAY BE PARTIAL AND SAYS SO: the product API filters a policy's
      // namespace lists to those this reader administers. `namespacesTruncated`
      // is the separate, ordinary row bound.
      ["namespace lists narrowed to what you administer",
        object.__namespacesFiltered === true ? "yes" : "no"],
      ["namespace list truncated", object.__namespacesTruncated === true ? "yes" : "no"],
      ["key list truncated", object.__keysTruncated === true ? "yes" : "no"],
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
        "</p>") +
    renderCompromiseGuard(status)
  );
}

/** The `CompromiseGuard` condition's type. */
export const COMPROMISE_GUARD = "CompromiseGuard";

/** What a `KeyCompromise` record on this policy holds, verbatim from the
 *  controller: the finalizer that keeps a deletion from re-trusting the key,
 *  a deletion being held, a key inherited from another policy's record, or a
 *  roster still listing a compromised key (the rollback hazard). Rendered only
 *  when the condition is `True`; a held deletion and an inherited compromise
 *  are complaints, because each asks an administrator to act. */
export function renderCompromiseGuard(status) {
  const conditions = Array.isArray((status || {}).conditions) ? status.conditions : [];
  const guard = conditions.find((c) => (c || {}).type === COMPROMISE_GUARD) || null;
  if (guard === null || guard.status !== "True") {
    return "";
  }
  const reason = String(guard.reason || "");
  const kind = reason === "CompromiseRecorded" ? "note" : "complaint";
  return "<p class=\"" + kind + "\" data-compromise-guard=\"" + esc(reason) + "\">" +
    esc(inert("Compromise guard (" + reason + "): " + String(guard.message || ""))) + "</p>";
}

/** What a contested namespace means. It resolves to NOTHING -- not to one of
 *  the two policies -- and every approval and verification in it is refused. */
export const CONFLICT_SENTENCE =
  "These namespaces are claimed by more than one TrustPolicy, so each of them resolves to no " +
  "policy at all: every approval and every evidence verification in them is refused with " +
  "TrustPolicyConflict rather than silently taking one policy's answer.";

/** The whole page. `view` is `{policies, roster, reason, now}`; a bare
 *  COLLECTION (the older `kubectl proxy` call shape) is read as "no policy,
 *  this roster, and no server instant", so roster verdicts the roster's own
 *  status supports render and the rest read `unknown`. */
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
  if (v.forbidden === true) {
    return head +
      "<div class=\"empty-state\" id=\"keys-forbidden\"><p class=\"note\">" +
      esc(KEYS_FORBIDDEN_SENTENCE) + "</p><p class=\"note\">The product API answered " +
      "403 to your read of the trust policies.</p></div>";
  }
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
 *  The roster's own column is `unknown` too: a roster has no
 *  `observedGeneration` and no `evaluatedAt`, so there is nothing to
 *  establish freshness FROM, and D3 section 7.7's answer is the same word as
 *  everywhere else. */
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
    "<p class=\"note\">" + messageText(ROSTER_FALLBACK_SENTENCE) + "</p>" +
    facts([
      ["loaded", flagBadge(status.loaded, "loaded", "not loaded")],
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

/** What a reader who may not read trust material is told. */
export const KEYS_FORBIDDEN_SENTENCE =
  "Only administrators can view trust material. Your role does not include reading the " +
  "installation's TrustPolicy objects; that says nothing about whether approvals and evidence " +
  "verify, which the runs' own pages show.";

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
 *  A TrustPolicy, not a roster: a roster cannot express a retirement, an
 *  overlap window or a usage (D3 section 7.2). The migration command is
 *  beside it, because `logweir trust migrate-roster` produces a policy from
 *  an existing roster and leaves the roster in place for rollback. */
export function renderPolicySnippet() {
  return (
    "<section class=\"check\"><h3>Editing trust is a cluster-admin step</h3>" +
    "<p class=\"note\">This page shows the document and does not apply it. <code>trustpolicies</code> is " +
    "cluster-scoped, its plural is absent from this page's writable set, the product API serves " +
    "no trust write in v1, and the API server would refuse a namespace-scoped viewer in any " +
    "case. Save this as trustpolicy.yml and apply it yourself. Editing is MONOTONIC: a key's " +
    "public material can never be edited out, <code>notAfter</code> may only move earlier, and <code>state</code> may " +
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

/** Reads the cluster's trust material and renders it.
 *
 *  TWO READS, AND THE SECOND IS A FALLBACK, NOT A FAILURE. The policy read is
 *  authoritative (D3 section 7); where it is refused (no grant on
 *  `trustpolicies`, or a product API without the trust route) the roster is
 *  read instead and the page says which it is looking at. A refusal is never
 *  absorbed into an empty table. */
export async function mountKeys(node, parse, deps, lifecycle) {
  // The seam takes either shape: an object that can `listCluster` IS the API
  // object; otherwise it is a bag of D3 reads (the mode probe, the server
  // clock).
  const d = deps !== null && deps !== undefined && typeof deps.listCluster === "function"
    ? { api: deps }
    : (deps || {});
  const api = d.api || API;
  const view = { policies: [], roster: null, now: (d.serverClock || serverClock)(), reason: "",
    forbidden: false };
  try {
    const collection = await listD3("trust", "", readOptions(lifecycle), d);
    view.policies = itemsOf(collection);
  } catch (error) {
    if (cancelled(error, lifecycle)) {
      return;
    }
    // A 403 IS "YOU MAY NOT READ THIS", NOT "THERE IS NO TRUST": rendering it as
    // missing trust would mislead an operator who merely may not read it. Shared
    // console only: behind `kubectl proxy` a 403 is the page's own kubeconfig,
    // and the refusal and the document an administrator applies are what that
    // reader needs.
    view.forbidden = (error || {}).status === 403 && (d.modeOf || mode)() === CONSOLE;
    view.reason = describeRefusal(error);
  }
  if (view.forbidden) {
    if (active(lifecycle)) {
      replace(node, parse(renderKeysPage(view)));
    }
    return;
  }
  try {
    view.roster = rosterOf(await api.listCluster(PLURAL, readOptions(lifecycle)));
  } catch (error) {
    if (cancelled(error, lifecycle)) {
      return;
    }
    // A roster refusal is worth reporting only when it is the last thing left:
    // with a policy on screen the roster is a footnote about rollback; with no
    // policy it is the whole page.
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
