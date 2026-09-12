// pages/keys.js -- the TrustRoster, READ-ONLY.
//
// THIS PAGE WRITES NOTHING, AND THE PLURAL IT READS IS NOT WRITABLE. The
// `TrustRoster` is cluster-scoped and admin-only: a namespace-scoped adopter
// cannot edit it at all, and `trustrosters` is absent from the frozen writable
// set in `api.js`, so both of that module's writing functions throw a
// `RangeError` on it before anything reaches the network. This page never
// calls either of them; `the_keys_page_submits_nothing` asserts that twice --
// once with a stub whose writers throw, and once by reading this file.
//
// SO IT SURFACES THE `kubectl apply` SNIPPET AND DOES NOT SUBMIT IT. That is
// the honest shape for an admin-only step: show the exact document a cluster
// admin applies, and let them apply it.
//
// AND IT PRINTS THE FINGERPRINT COMMAND, because that is the only step that
// catches an undisclosed key rotation. A roster row says which key id the
// controller will accept; it cannot say that the key material behind that id
// is still the one the approver holds. Comparing the fingerprint out of band,
// against the person who holds the key, is what does.
//
// EXPIRY COMES FROM `status.expiredKeyIds[]`, WHICH THE CONTROLLER COMPUTED.
// A page that compared `notAfter` against the browser's own clock would be
// rendering a verdict from a clock the cluster never saw, and would disagree
// with the controller's own refusal by exactly the skew between them.

import { listCluster } from "../api.js";
import {
  cell,
  copyBlock,
  errorBox,
  esc,
  facts,
  listFooter,
  replace,
  table,
} from "../render.js";
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

const API = { listCluster: listCluster };

/** The roster named `default` out of a collection, or `null`. */
export function rosterOf(collection) {
  for (const object of itemsOf(collection)) {
    if (((object.metadata || {}).name) === ROSTER_NAME) {
      return object;
    }
  }
  return null;
}

/** One key list as a table, with the expiry column driven by
 *  `status.expiredKeyIds[]`. */
export function renderKeyTable(caption, entries, expiredKeyIds) {
  const expired = Array.isArray(expiredKeyIds) ? expiredKeyIds : [];
  const rows = (Array.isArray(entries) ? entries : []).map((entry) => {
    const e = entry || {};
    return [
      "<code>" + esc(e.keyId) + "</code>",
      cell(e.subject),
      cell(e.notAfter),
      expired.indexOf(e.keyId) === -1 ? "valid" : "expired",
    ];
  });
  return (
    "<h3>" + esc(caption) + "</h3>" +
    table(
      ["KEY ID", "SUBJECT", "NOT AFTER", "EXPIRY"],
      rows,
      "no key of this kind in the roster",
    )
  );
}

/** The whole page. */
export function renderKeysPage(collection) {
  const roster = rosterOf(collection);
  if (roster === null) {
    return (
      "<h2>Keys</h2>" +
      "<p class=\"blurb\">No TrustRoster named " + esc(ROSTER_NAME) + " exists in this " +
      "cluster. It is cluster-scoped and admin-only; until a cluster admin applies one, no " +
      "approval can verify and no evidence can be checked against a key.</p>" +
      renderRosterSnippet()
    );
  }
  const spec = roster.spec || {};
  const status = roster.status || {};
  return (
    "<h2>Keys</h2>" +
    "<p class=\"blurb\">The cluster-scoped TrustRoster named " + esc(ROSTER_NAME) + ". " +
    "approverKeys authorise an approval; signingKeys are what weirkeeper verifies signed " +
    "evidence against. They are two separate principals, and self-attestation means only " +
    "that two matched key ids differ.</p>" +
    facts([
      ["loaded", cell(status.loaded)],
      ["allowed cluster ids",
        (Array.isArray(spec.allowedClusterIds) ? spec.allowedClusterIds : []).length === 0
          ? cell(null)
          : esc(spec.allowedClusterIds.join(", "))],
    ]) +
    renderKeyTable("approverKeys", spec.approverKeys, status.expiredKeyIds) +
    renderKeyTable("signingKeys", spec.signingKeys, status.expiredKeyIds) +
    "<section class=\"check\"><h3>Check a key out of band</h3>" +
    "<p class=\"note\">A row above says which key id the controller accepts. It cannot say " +
    "that the material behind that id is still the one its holder has. Run this against " +
    "the public half they give you and compare the digest with the KEY ID column.</p>" +
    copyBlock([FINGERPRINT_COMMAND]) +
    "</section>" +
    renderRosterSnippet() +
    listFooter()
  );
}

/** The snippet a cluster admin applies. RENDERED AND NEVER SUBMITTED. */
export function renderRosterSnippet() {
  return (
    "<section class=\"check\"><h3>Editing the roster is a cluster-admin step</h3>" +
    "<p class=\"note\">This page shows the document and does not apply it. TrustRoster is " +
    "cluster-scoped, its plural is absent from this page's writable set, and the API " +
    "server would refuse a namespace-scoped viewer in any case. Save this as roster.yml " +
    "and apply it yourself.</p>" +
    copyBlock([
      "apiVersion: logweir.dev/v1alpha1",
      "kind: TrustRoster",
      "metadata:",
      "  name: " + ROSTER_NAME,
      "spec:",
      "  approverKeys:",
      "    - keyId: <sha256 of the DER SPKI, lowercase hex>",
      "      spkiPem: |",
      "        -----BEGIN PUBLIC KEY-----",
      "        <the approver's PUBLIC half>",
      "        -----END PUBLIC KEY-----",
      "      subject: <who holds it>",
      "      notAfter: \"2027-01-01T00:00:00Z\"",
      "  signingKeys:",
      "    - keyId: <sha256 of the DER SPKI, lowercase hex>",
      "      spkiPem: |",
      "        -----BEGIN PUBLIC KEY-----",
      "        <the runner's PUBLIC half>",
      "        -----END PUBLIC KEY-----",
      "      subject: <the runner identity>",
      "      notAfter: \"2027-01-01T00:00:00Z\"",
      "  allowedClusterIds: []",
      "",
      "kubectl --context docker-desktop apply -f roster.yml",
    ]) +
    "</section>"
  );
}

// --------------------------------------------------------------- mount half

export async function mountKeys(node, parse, deps) {
  const api = deps || API;
  try {
    const collection = await api.listCluster(PLURAL);
    replace(node, parse(renderKeysPage(collection)));
  } catch (error) {
    replace(node, errorBox(error));
  }
}
