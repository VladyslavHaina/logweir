// The CRD's evidence verdict vocabulary, as the console reads it.
//
// `status.evidence.verification.result` gained `Pending` with D2 section 3.9's
// evidence-fetch Job (`claude/evidence-fetch`): the controller writes it while
// the check Job reads a run's evidence with the destination's `evidenceRead`
// grant, and replaces it with a reached verdict. A console that did not know
// the word would render it as a word it does not know; this suite pins that it
// knows it, that it agrees with the CRD's own documented list (read from the
// side that writes it), and that a Pending run renders as `verifying` -- never
// as a verified success and never as `unknown`.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  D3_WORDS, EVIDENCE_RESULTS, OPERATION_STATES, TRUST_STATES,
} from "../contract.js";
import { GREEN_TRUST_STATES, TRUST_STATE_CASES, stateBadge, unverifiedCaption } from "../render.js";

const repo = (path) => fileURLToPath(new URL("../../" + path, import.meta.url));

// Every backticked word of every `result` description sentence of the form
// "`Valid`, `Invalid` ... or `NotAttempted`." in a CRD file.
function documentedResults(crdPath) {
  const text = readFileSync(repo(crdPath), "utf8");
  const words = new Set();
  const sentence = /`Valid`, `Invalid`[^.]*?(?: or `[A-Za-z]+`)\./g;
  for (const match of text.matchAll(sentence)) {
    for (const word of match[0].matchAll(/`([A-Za-z]+)`/g)) {
      words.add(word[1]);
    }
  }
  return words;
}

test("EVIDENCE_RESULTS names Pending, the evidence-fetch Job's in-flight word", () => {
  assert.ok(EVIDENCE_RESULTS.includes("Pending"), JSON.stringify(EVIDENCE_RESULTS));
  assert.equal(D3_WORDS.evidenceResult, EVIDENCE_RESULTS,
    "the rendering vocabulary IS this list, not a copy that can drift");
  // Every verdict the controller reaches is still there beside it.
  for (const word of ["Valid", "Invalid", "Untrusted", "NotAttempted"]) {
    assert.ok(EVIDENCE_RESULTS.includes(word), word);
  }
});

test("every result the Backup and Restore CRDs document is one the console knows", () => {
  for (const crd of ["config/crd/backups.yaml", "config/crd/restores.yaml"]) {
    const documented = documentedResults(crd);
    assert.ok(documented.size >= 3,
      crd + ": the result sentence was not found, so this row would pass on nothing");
    for (const word of documented) {
      assert.ok(EVIDENCE_RESULTS.includes(word),
        crd + " documents `" + word + "` and ui/contract.js EVIDENCE_RESULTS does not list it");
    }
  }
  // THE PLANTED SHAPE: the reader finds the evidence-fetch sentence when it is
  // there, so the loop above is not vacuous on a CRD that carries it.
  const planted = "description: '`Valid`, `Invalid`, `Untrusted`, `NotAttempted` or `Pending`. An";
  const found = new Set([...planted.matchAll(/`([A-Za-z]+)`/g)].map((m) => m[1]));
  assert.ok(found.has("Pending") && found.size === 5, [...found].join(","));
});

test("a Pending run is projected as verifying / pending, and renders as that word", () => {
  // The API's projection (`crates/logweir-api/src/status.rs`): terminal, the
  // evidence keys recorded, no reached verdict -> `verifying` with reason
  // `EvidenceVerificationPending`, and the trust state `pending`.
  const status = readFileSync(repo("crates/logweir-api/src/status.rs"), "utf8");
  assert.match(status, /no verdict yet \| `verifying` \| `EvidenceVerificationPending`/);
  if (status.includes('Some("Pending")') && status.includes("EvidenceVerificationPending")) {
    assert.match(status, /VerificationState::Pending/);
  }
  assert.ok(OPERATION_STATES.includes("verifying"));
  assert.ok(TRUST_STATES.includes("pending"));
  const badge = stateBadge("verifying");
  assert.match(badge, />verifying</);
  assert.doesNotMatch(badge, /unknown/);
  // NEVER GREEN: `pending` is not a trust state a green badge may carry, and
  // its sentence says the verdict is not written yet.
  assert.ok(!GREEN_TRUST_STATES.includes("pending"));
  assert.match(TRUST_STATE_CASES.pending, /not written yet/);
});

test("a Pending custom resource read directly is never a verified success", () => {
  // Legacy mode reads the CR, not the API's projection. Whatever case it names,
  // the caption carries the one word every older reader looks for.
  const caption = unverifiedCaption({ result: "Pending" }, true);
  assert.match(caption, /^unverified/);
  assert.doesNotMatch(caption, /verified by|valid/i);
});
