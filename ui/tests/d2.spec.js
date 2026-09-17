// d2.spec.js -- D2 (PLAT-08.1, PLAT-08.2, PLAT-09.1, PLAT-03.1, PLAT-03.2)
// under `node --test`: what the three new surfaces render for every state the
// product API can put in front of them, and what they refuse to render.
//
// WHY THIS IS A FILE OF ITS OWN. `pages.spec.js` asserts the rules the five
// older kinds carry, over fixtures shaped like `kubectl proxy`'s answers.
// These three domains have no legacy shape: the DTO IS the shape, the fixtures
// are the product API's own documents, and `contract.spec.js` holds every one
// of them against `schemas/logweir-api-v1.openapi.json`. Keeping them together
// means a reader looking for "what does the console say when a listing came
// back empty" finds one file rather than three neighbourhoods.
//
// THE RULE EVERY ROW BELOW IS ABOUT. This product may render what somebody
// OBSERVED, attributed to whoever observed it. It may not render a conclusion
// nobody drew. That shows up here as five separate refusals:
//
//   * a successful topic listing is `unknown`, never "complete";
//   * an attestation is always "...; not verified by Logweir";
//   * `status.valid` absent is "not judged", never "invalid" and never "valid";
//   * a `Preflight` with an empty `checks` array is not a pass;
//   * `unverifiable` is "could not be checked", never "out of date".
//
// NOTHING HERE DIALS, and `ui_lint.rs::the_ui_behaviour_suite_never_dials`
// holds that for the whole directory.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import {
  CONSOLE_ENUMS,
  decodeCancel,
  decodeCheckOperation,
  decodeConsoleItem,
  decodeConsoleList,
  decodeDestinationUsage,
  decodeDiscoveryLatest,
  decodeRequest,
  decodeTopicPage,
  isContractFailure,
} from "../contract.js";
import { keepDraft, readDraft } from "../lifecycle.js";
import {
  ATTESTATION_DISCLAIMER,
  CREDENTIALS_CLEARED_CLAUSE,
  applicabilityLine,
  checkTable,
  destinationVerdict,
  executionOnlyBlock,
  preflightSentence,
  preflightVerdict,
  staleReasonLine,
  visibilityLine,
} from "../render.js";
import {
  CREDENTIAL_INPUTS,
  DESTINATION_DRAFT_FIELDS,
  GRANT_ROLES,
  accessBody,
  destinationBody,
  grantBody,
  renderDestinationDetail,
  renderDestinationForm,
  renderDestinationList,
  renderLastTest,
  renderLegacyRefusal,
  renderPreflight,
  renderRotateForm,
  renderUsage,
  rotationBody,
  submitRotation,
  validateDestination,
  validateGrants,
} from "../pages/destinations.js";
import {
  NO_CONNECTIVITY_CHECK_KIND,
  renderDiscovery,
  renderDiscoveryPanel,
  renderTopicTable,
} from "../pages/clusters.js";
import {
  COVERAGE_LABELS,
  CREATE_HAS_NO_DESTINATION,
  EDIT_IS_A_REPLACE,
  claimsWholeCluster,
  defaultDestination,
  destinationCell,
  renderCoverageLine,
  renderDestinationSelector,
  renderReadinessPanel,
  renderScheduleForm,
  renderScheduleList,
  renderTopicPicker,
  resolveDestinationSelection,
} from "../pages/schedules.js";
import { SOURCE_DESTINATION_NOT_PUBLISHED } from "../pages/restore-wizard.js";

const console_ = (name) =>
  JSON.parse(readFileSync(new URL("./fixtures/console/" + name, import.meta.url), "utf8"));

const destination = (name) => decodeConsoleItem("destinations", console_(name)).value.item;
const discovery = (name) => decodeConsoleItem("topic-discoveries", console_(name)).value.item;
const preflight = (name) => decodeConsoleItem("preflights", console_(name)).value.item;

// ===========================================================================
// destinations
// ===========================================================================

test("a_destination_no_controller_has_judged_reads_not_judged_and_never_invalid", () => {
  // THE STATE EVERY DESTINATION ON THIS CLUSTER IS IN. No controller
  // reconciles `BackupDestination` on the laboratory images, so `status` comes
  // back `{}` and `valid` is absent. Absent is "not judged": rendering it as
  // `false` would report a missing controller as a broken destination, and
  // rendering it as `true` would be worse.
  const item = destination("destination-unjudged.json");
  assert.equal(item.status.valid, null, "the fixture is the unjudged shape");
  const badge = destinationVerdict(item.status);
  assert.match(badge, /not judged yet/);
  assert.doesNotMatch(badge, /\bvalid\b(?!.*not judged)/);
  assert.doesNotMatch(badge, /invalid/);

  const html = renderDestinationDetail(item, { mayOperate: true });
  assert.match(html, /id="destination-unjudged"/);
  assert.match(html, /No controller has recorded a verdict/);
  assert.match(html, /not a claim that it works/);
});

test("a_judged_destination_carries_the_controllers_own_reason", () => {
  const item = destination("destination.json");
  assert.match(destinationVerdict(item.status), /valid \(Valid\)/);
  assert.match(destinationVerdict({ valid: false, reason: "EndpointNotOrigin" }),
    /not valid \(EndpointNotOrigin\)/);
});

test("the_list_separates_transport_from_addressing_and_names_plaintext_as_plaintext", () => {
  const page = decodeConsoleList("destinations", console_("destinations-list.json")).value;
  const html = renderDestinationList(page, "team-a");
  assert.match(html, /<th scope="col">TRANSPORT<\/th>/);
  assert.match(html, /<th scope="col">ADDRESSING<\/th>/);
  assert.match(html, /insecureHttp \(plaintext\)/,
    "a plaintext destination is named as one and not left as a word in a cell");
  assert.match(html, /badge-green">default/);
  assert.match(html, /a namespace with two defaults has none/);
  assert.match(html, /id="destinations-unjudged"/, "the list discloses the unjudged row");
});

test("the_access_table_shows_references_and_what_each_absent_grant_means", () => {
  const item = destination("destination.json");
  const html = renderDestinationDetail(item, { mayOperate: true });
  for (const role of GRANT_ROLES) {
    assert.match(html, new RegExp("<code>" + role + "</code>"), role + " has a row");
  }
  assert.match(html, /inheritsArchiveWrite/);
  assert.match(html, /verification is NOT attempted/);
  assert.match(html, /access-key-id, secret-access-key/, "the KEY NAMES are public and shown");
  assert.doesNotMatch(html, /secretAccessKey|accessKeyId/,
    "and no credential FIELD ever appears in a rendered destination");
});

test("a_destination_with_no_recorded_test_says_so_rather_than_showing_nothing", () => {
  assert.match(renderLastTest(null), /No access test has been recorded/);
  assert.match(renderLastTest(null), /Nothing below claims it works/);
  const truncated = { preflightId: "pf-1", state: "ready", stale: false, truncated: true };
  assert.match(renderLastTest(truncated), /may not be the newest one/,
    "a last test that might not be last says so rather than reassuring");
  const stale = { preflightId: "pf-1", state: "ready", stale: true, truncated: false };
  assert.match(renderLastTest(stale), /stale: not health/);
});

test("the_usage_list_renders_its_basis_beside_an_empty_answer", () => {
  const usage = decodeDestinationUsage(console_("destination-usage.json")).value;
  const html = renderUsage(usage, null);
  assert.match(html, /id="usage-basis"/);
  assert.match(html, /created with kubectl carries no such label/);
  const empty = renderUsage(
    { name: "primary", truncated: false, basis: usage.basis, schedules: [], backups: [] },
    null,
  );
  assert.match(empty, /Nothing labelled by this service names it/);
  assert.match(empty, /id="usage-basis"/,
    "an empty list without its basis would read as 'nothing uses this'");
});

test("a_legacy_adoption_refusal_is_rendered_as_what_it_is_and_not_as_not_found", () => {
  const problem = console_("problem-legacy-unknown.json");
  const error = new Error(problem.detail);
  error.code = problem.code;
  const html = renderLegacyRefusal(error);
  assert.match(html, /id="legacy-location-unknown"/);
  assert.match(html, /The object was found/);
  assert.match(html, /refuses to derive a destination from a guess/);
  assert.doesNotMatch(html, /does not exist/);
  assert.equal(renderLegacyRefusal({ code: "not_found" }), "",
    "and any other code is left to the ordinary error box");
});

// ---------------------------------------------- the credential, and the draft

test("MUTANT_a_credential_field_in_the_draft_allowlist_fails_this_row", () => {
  // THE GUARD: `keepDraft` takes an ALLOWLIST, and the destination form's list
  // names no credential input. THE MUTANT: add one. It was planted by hand and
  // this row went red on the second assertion; the row is what keeps it red.
  for (const input of CREDENTIAL_INPUTS) {
    assert.equal(
      DESTINATION_DRAFT_FIELDS.indexOf(input),
      -1,
      input + " is a credential input and is in the draft allowlist. A draft is page memory " +
        "that survives a refusal, a re-render and a route change; a credential in one survives " +
        "all three.",
    );
  }
  const key = "d2-draft/destinations";
  const kept = keepDraft(key, {
    name: "primary",
    bucket: "kafka-backups",
    archiveWriteSource: "new",
    archiveWriteAccessKeyId: "AKIAIOSFODNN7EXAMPLE",
    archiveWriteSecretAccessKey: "wJalrXUtnFEMI-K7MDENG-bPxRfiCYEXAMPLEKEY",
  }, DESTINATION_DRAFT_FIELDS);
  assert.equal(kept.name, "primary", "the names are kept");
  assert.equal(kept.archiveWriteSecretAccessKey, undefined, "and the credential is not");
  const back = JSON.stringify(readDraft(key));
  assert.doesNotMatch(back, /wJalrXUtnFEMI/, "nor is it anywhere in what comes back");
  assert.doesNotMatch(back, /AKIAIOSFODNN7EXAMPLE/);
});

test("the_form_never_renders_a_value_attribute_on_a_credential_input", () => {
  // A RE-RENDER IS WHERE A KEPT CREDENTIAL WOULD SURFACE. Even with a draft
  // that somehow carried one, the markup has no `value=` on these inputs, so
  // there is nowhere for it to land.
  const html = renderDestinationForm({
    draft: {
      archiveWriteAccessKeyId: "AKIAIOSFODNN7EXAMPLE",
      archiveWriteSecretAccessKey: "wJalrXUtnFEMI-K7MDENG-bPxRfiCYEXAMPLEKEY",
    },
  });
  assert.doesNotMatch(html, /AKIAIOSFODNN7EXAMPLE/);
  assert.doesNotMatch(html, /wJalrXUtnFEMI/);
  assert.match(html, /name="archiveWriteSecretAccessKey" type="password"/);
  assert.match(html, /autocomplete="new-password"/);
  assert.match(html, /never read back/, "and the form says what write-only means");
});

// ------------------------------------------------ addressing is not transport

test("MUTANT_addressing_never_decides_transport_in_either_direction", () => {
  // DEFECT G5, WRITTEN AS A ROW. THE MUTANT: make `destinationBody` read the
  // addressing radio into `transport.security`, as the wizard once read
  // `pathStyle` into `allowHttp`. Planted by hand; the first assertion went
  // red. Both directions are asserted, because the derivation is just as wrong
  // the other way round.
  const base = {
    name: "p", bucket: "kafka-backups",
    archiveWriteSource: "existing", archiveWriteSecret: "s3",
  };
  const pathTls = destinationBody(Object.assign({}, base,
    { addressing: "pathStyle", security: "tls" }));
  assert.equal(pathTls.transport.security, "tls",
    "path-style addressing does not turn TLS off");
  assert.equal(pathTls.storage.addressing, "pathStyle");

  const virtualHttp = destinationBody(Object.assign({}, base, {
    addressing: "virtualHosted", security: "insecureHttp",
    endpoint: "http" + ":" + "//" + "minio.storage.svc:9000",
  }));
  assert.equal(virtualHttp.transport.security, "insecureHttp");
  assert.equal(virtualHttp.storage.addressing, "virtualHosted",
    "and plaintext transport does not force an addressing mode");
});

test("the_endpoint_scheme_and_the_transport_must_agree_and_neither_is_changed_to_suit", () => {
  const base = {
    name: "p", bucket: "kafka-backups", addressing: "pathStyle",
    archiveWriteSource: "existing", archiveWriteSecret: "s3",
  };
  const httpsWithPlaintext = validateDestination(Object.assign({}, base, {
    security: "insecureHttp", endpoint: "https" + ":" + "//" + "minio:9000",
  }));
  assert.match(httpsWithPlaintext.endpoint, /will not change one to suit the other/);
  const httpWithTls = validateDestination(Object.assign({}, base, {
    security: "tls", endpoint: "http" + ":" + "//" + "minio:9000",
  }));
  assert.match(httpWithTls.endpoint, /only when it is chosen explicitly/);
  const plaintextNoEndpoint = validateDestination(Object.assign({}, base,
    { security: "insecureHttp" }));
  assert.match(plaintextNoEndpoint.endpoint, /no way to reach AWS S3 in the clear/);
  assert.deepEqual(
    Object.keys(validateDestination(Object.assign({}, base, {
      security: "tls", endpoint: "https" + ":" + "//" + "minio.storage.svc:9000",
    }))),
    [],
    "and an agreeing pair is accepted",
  );
});

test("the_reserved_prefix_and_the_required_write_grant_are_refused_by_name", () => {
  const base = { name: "p", bucket: "kafka-backups", addressing: "pathStyle", security: "tls" };
  assert.match(
    validateDestination(Object.assign({}, base,
      { prefix: "logweir/readiness", archiveWriteSource: "existing", archiveWriteSecret: "s3" })).prefix,
    /reserved for evidence/,
  );
  assert.match(
    validateDestination(Object.assign({}, base, { archiveWriteSource: "absent" })).archiveWriteSource,
    /archiveWrite is required/,
  );
  assert.match(
    validateDestination(Object.assign({}, base,
      { archiveWriteSource: "controllerIdentity" })).archiveWriteSource,
    /evidenceRead answer only/,
  );
});

test("an_absent_grant_is_omitted_and_never_spelled_as_the_response_only_mode", () => {
  // `inheritsArchiveWrite` and `notConfigured` are RESPONSE spellings of an
  // absent grant. Sending either is a 422, because "absent" and "explicitly
  // say the thing absence means" would then be two names for one state.
  assert.equal(grantBody({ archiveReadSource: "absent" }, "archiveRead"), null);
  const access = accessBody({
    archiveWriteSource: "existing", archiveWriteSecret: "s3",
    archiveReadSource: "absent", evidenceWriteSource: "absent",
    evidenceReadSource: "archiveReadGrant",
  });
  assert.deepEqual(Object.keys(access).sort(), ["archiveWrite", "evidenceRead"]);
  assert.equal(JSON.stringify(access).indexOf("inheritsArchiveWrite"), -1);
  assert.equal(JSON.stringify(access).indexOf("notConfigured"), -1);
});

test("every_body_this_page_builds_is_the_published_request_shape", () => {
  const body = destinationBody({
    name: "primary", description: "prod", bucket: "kafka-backups", prefix: "team-a/prod",
    region: "us-east-1", endpoint: "https" + ":" + "//" + "minio.storage.svc:9000",
    addressing: "pathStyle", security: "tls", caName: "minio-ca", caKey: "ca.crt",
    writeProbe: "createOnlyMarker", isDefault: true,
    archiveWriteSource: "existing", archiveWriteSecret: "logweir-s3",
    archiveReadSource: "new", archiveReadAccessKeyId: "id", archiveReadSecretAccessKey: "key",
    evidenceWriteSource: "workloadIdentity", evidenceWriteServiceAccount: "logweir-runner",
    evidenceReadSource: "archiveReadGrant",
  });
  assert.deepEqual(decodeRequest("destinations", body).unknown, []);
  const rotation = rotationBody({
    archiveWriteSource: "existing", archiveWriteSecret: "logweir-s3-new",
    caChange: "clear",
  }, 7);
  assert.equal(rotation.expectedGeneration, 7);
  assert.deepEqual(rotation.transport, {}, "clearing the CA is an empty transport block");
  assert.deepEqual(decodeRequest("destinations:update-access", rotation).unknown, []);
});

test("a_rotation_sends_the_complete_access_block_because_an_omitted_grant_is_removed", () => {
  const html = renderDestinationDetail(destination("destination.json"), { mayOperate: true });
  assert.match(html, /a grant left at <code>absent<\/code> here is REMOVED/);
  assert.match(html, /id="rotate-generation"/);
  assert.match(html, /the answer is a 412 and nothing is written/);
});

// ===========================================================================
// topic discovery
// ===========================================================================

test("a_successful_listing_alone_is_unknown_and_this_page_never_calls_it_complete", () => {
  const item = discovery("discovery-unknown.json");
  assert.equal(item.visibility.state, "unknown");
  const html = renderDiscovery(item, "successful");
  assert.match(html, /visibility: unknown/);
  assert.match(html, /hides them without saying so/);
  assert.match(html, /records that as unknown rather than calling it complete/);
  assert.doesNotMatch(
    html.replace(/attestedComplete/g, ""),
    /\bcomplete inventory\b|\bis complete\b|\ball topics\b/i,
    "no sentence in an unknown-visibility render claims completeness",
  );
});

test("a_limited_visibility_says_the_list_is_a_subset_and_carries_its_basis", () => {
  const html = renderDiscovery(discovery("discovery-limited.json"), "successful");
  assert.match(html, /visibility: limited/);
  assert.match(html, /is a subset, and Logweir cannot say how large a subset/);
  assert.match(html, /basis: listingOnly, expectedTopicNotAuthorized/);
  assert.match(html, /1 not authorized/, "the expected-topic counts are rendered");
});

test("MUTANT_an_attestation_is_never_rendered_without_its_disclaimer", () => {
  // THE GUARD: `attestationLine` appends "not verified by Logweir" and is the
  // only path that renders an attestation. THE MUTANT: return the recorded
  // claim verbatim from it. Planted by hand; both the second and third
  // assertions went red.
  const item = discovery("discovery-attested.json");
  assert.equal(item.visibility.state, "attestedComplete");
  const html = renderDiscovery(item, "successful");
  assert.match(html, /platform-team@example\.com/, "the claim's author is shown");
  assert.match(html, new RegExp(ATTESTATION_DISCLAIMER),
    "and the disclaimer is not optional");
  assert.match(html, /An administrator attests/,
    "the sentence attributes the claim rather than stating it");
  const bare = visibilityLine({
    state: "attestedComplete",
    basis: ["administratorAttestation"],
    attestation: "attested by ops at 2026-01-01T00:00:00Z",
  });
  assert.ok(
    bare.indexOf("attested by ops at 2026-01-01T00:00:00Z; " + ATTESTATION_DISCLAIMER) !== -1,
    "the disclaimer follows the claim in the same sentence, so neither can be quoted alone",
  );
});

test("an_empty_inventory_is_not_a_claim_that_the_cluster_is_empty", () => {
  const html = renderDiscovery(discovery("discovery-empty.json"), "successful");
  assert.match(html, /id="discovery-empty"/);
  assert.match(html, /this is not proof that the cluster is empty/);
  assert.match(html, /visibility: unknown/);
});

test("a_cancelled_or_failed_discovery_reports_the_absence_of_a_result", () => {
  const cancelledHtml = renderDiscovery(discovery("discovery-cancelled.json"), "latest");
  assert.match(cancelledHtml, /badge-phase-cancelled/);
  assert.doesNotMatch(cancelledHtml, /visibility: attested/);
  const failed = discovery("discovery-failed.json");
  const failedHtml = renderDiscovery(failed, "latest");
  assert.match(failedHtml, /badge-phase-failed/);
  assert.match(failedHtml, /BrokerUnreachable/);
  assert.match(failedHtml, /no broker answered within the metadata budget/);
});

test("a_stale_or_truncated_inventory_is_labelled_and_still_shown", () => {
  const staleHtml = renderDiscovery(discovery("discovery-stale.json"), "successful");
  assert.match(staleHtml, /stale: expired, principalChanged/);
  assert.match(staleHtml, /an old fact presented as current is not/);
  const truncatedHtml = renderDiscovery(discovery("discovery-truncated.json"), "successful");
  assert.match(truncatedHtml, /truncated: RelayLimit/);
  assert.match(truncatedHtml, /a prefix of what the broker listed/);
});

test("the_two_slots_are_rendered_separately_and_neither_hides_the_other", () => {
  const latest = decodeDiscoveryLatest(console_("discovery-latest.json")).value;
  const html = renderDiscoveryPanel({
    mayOperate: true, latestAttempt: latest.latestAttempt,
    lastSuccessful: latest.lastSuccessful, filters: {}, state: {},
  });
  assert.match(html, /<h4>Latest attempt<\/h4>/);
  assert.match(html, /<h4>Last successful inventory<\/h4>/);
  assert.match(html, /badge-phase-failed/, "the failed newest attempt is not hidden");
  assert.match(html, /5004 \/ 5003/, "and the older successful inventory is not hidden either");
  const none = renderDiscoveryPanel({ mayOperate: true, filters: {}, state: {} });
  assert.match(none, /id="discovery-none"/);
  assert.match(none, /Nothing below claims anything about its topics/);
});

test("MUTANT_a_short_page_is_followed_and_never_treated_as_the_last_one", () => {
  // THE GUARD: `scan.complete: false` means the chunk budget was spent, not
  // that the result ended; the table says so and offers the cursor. THE
  // MUTANT: render the "end of the result" sentence whenever `items` is
  // shorter than `page.limit`. Planted by hand; the second assertion went red
  // for `topics-page.json`, whose four rows are a short page WITH a cursor.
  const page = decodeTopicPage(console_("topics-page.json")).value;
  assert.equal(page.scan.complete, false);
  assert.equal(page.items.length, 4, "a short page");
  const html = renderTopicTable(page);
  assert.match(html, /stopped at its chunk budget, not at the end of the result/);
  assert.doesNotMatch(html, /reached the end of the stored result/);
  assert.match(html, /id="topics-more"/, "and the cursor is offered");
  assert.match(html, /data-cursor="Y3Vyc29yOjI="/);

  const last = decodeTopicPage(console_("topics-page-last.json")).value;
  const lastHtml = renderTopicTable(last);
  assert.match(lastHtml, /reached the end of the stored result/);
  assert.doesNotMatch(lastHtml, /id="topics-more"/);
  assert.match(lastHtml, /No cursor: this is the end of the result/);
});

test("a_topic_row_carries_its_partition_count_its_internal_flag_and_its_error", () => {
  const html = renderTopicTable(decodeTopicPage(console_("topics-page.json")).value);
  assert.match(html, /<code>__consumer_offsets<\/code>/);
  assert.match(html, /UnknownTopicOrPartition/);
  assert.match(html, /snapshot 51627361-4b95-4ca5-aa26-8f97fe847b2a@sha256:aa/);
  assert.match(html, /<th scope="col">EXPECTED<\/th>/);
});

test("a_discovery_route_this_mode_cannot_reach_is_a_sentence_and_not_an_error_box", () => {
  const html = renderDiscoveryPanel({
    mayOperate: true, filters: {}, state: {},
    unavailable: true,
    unavailableReason: "Topic discovery is served by the Logweir product API",
  });
  assert.match(html, /id="discovery-unavailable"/);
  assert.match(html, /served by the Logweir product API/);
  assert.doesNotMatch(html, /id="discovery-form"/,
    "and no control is offered for a route this mode cannot reach");
});

// ===========================================================================
// operation readiness
// ===========================================================================

test("MUTANT_a_preflight_with_no_recorded_checks_is_never_a_pass", () => {
  // THE GUARD: the aggregate comes from `state`, and an empty `checks` array
  // gets a sentence saying an empty result is not a pass. THE MUTANT: render
  // `checks.every(c => c.state === "ready")` as the verdict -- which is `true`
  // for an empty array, so a pending check rendered green. Planted by hand;
  // the first and third assertions went red.
  const pending = preflight("preflight-pending.json");
  assert.deepEqual(pending.checks, []);
  const html = renderPreflight(pending);
  assert.match(html, /badge-pending">pending/);
  assert.doesNotMatch(html, /badge-green/);
  assert.match(html, /That is not a pass: it is an empty result/);
  assert.equal(preflightVerdict("pending"), "<span class=\"badge badge-pending\">pending</span>");
  assert.match(preflightVerdict("somethingNew"), /unknown/,
    "and a state this build does not recognise is unknown, never ready");
});

test("a_not_ready_check_carries_its_code_its_message_and_its_remedy", () => {
  const html = renderPreflight(preflight("preflight-not-ready.json"));
  assert.match(html, /badge-unverified">not ready/);
  assert.match(html, /AccessDenied/);
  assert.match(html, /the object store refused the listing for this prefix/);
  assert.match(html, /Grant s3:ListBucket/);
  assert.match(html, /<th scope="col">REMEDY<\/th>/);
  assert.match(html, /<th scope="col">EXPIRES<\/th>/);
});

test("a_skipped_blocking_check_is_labelled_as_never_a_pass", () => {
  const item = preflight("preflight-skipped.json");
  assert.equal(item.state, "unknown");
  const html = renderPreflight(item);
  assert.match(html, /skipped \(never a pass\)/);
  assert.match(html, /badge-pending">unknown/);
  // THE VERDICT IS THE ONE IN THE HEAD, and it is the one that must not be
  // green. The applicability badge below it is a different question -- "does
  // this result still describe your inputs" -- and it answers yes here.
  const head = html.slice(0, html.indexOf("</p>"));
  assert.doesNotMatch(head, /badge-green/);
});

test("execution_only_checks_are_rendered_and_ready_never_means_they_passed", () => {
  const html = renderPreflight(preflight("preflight-ready.json"));
  assert.match(html, /badge-green">ready/);
  assert.match(html, /Only knowable at execution time/);
  assert.match(html, /destination\.archivePrefixWritable/);
  assert.match(html, /a ready verdict never means they passed/);
  assert.equal(executionOnlyBlock([]), "", "and nothing is rendered when there are none");
});

test("an_advisory_check_is_a_warning_and_never_changes_the_aggregate", () => {
  const item = preflight("preflight-ready.json");
  assert.equal(item.warnings.length, 1);
  assert.equal(item.warnings[0].gating, "advisory");
  assert.equal(item.state, "ready", "an advisory unknown did not stop it being ready");
  const html = renderPreflight(item);
  assert.match(html, /<h4>Advisory<\/h4>/);
  assert.match(html, /EvidenceReadNotConfigured/);
});

test("MUTANT_unverifiable_is_could_not_be_checked_and_never_out_of_date", () => {
  // THE GUARD: `staleReasonLine` gives `unverifiable` its own words and prints
  // the `basis` that says WHAT could not be compared. THE MUTANT: fall through
  // to the generic arm, so it rendered as the bare token `unverifiable` beside
  // the real staleness reasons. Planted by hand; the third assertion went red.
  const item = preflight("preflight-stale.json");
  const reasons = item.staleReasons.map((r) => r.reason);
  assert.deepEqual(reasons, ["planHashChanged", "referentChanged", "unverifiable"]);
  assert.equal(staleReasonLine(item.staleReasons[0]), "planHashChanged");
  assert.equal(
    staleReasonLine(item.staleReasons[1]),
    "referentChanged (BackupDestination/primary)",
  );
  assert.match(staleReasonLine(item.staleReasons[2]), /^could not be checked \(TrustRoster\/8f0a\)/);
  assert.match(
    staleReasonLine(item.staleReasons[2]),
    /the console has no verb for a cluster-scoped TrustRoster/,
    "a refusal to answer that does not say what it could not check is not an answer",
  );
  const html = applicabilityLine(item);
  assert.match(html, /does not apply to your current inputs/);
  assert.match(html, /compared: expiry, planHash, referent:BackupDestination\/primary/);
});

test("an_applicable_result_says_what_the_comparison_actually_covered", () => {
  const item = preflight("preflight-ready.json");
  const html = applicabilityLine(item);
  assert.match(html, /applies to your current inputs/);
  assert.match(html, /compared: expiry, referent:BackupDestination\/primary/);
  const uncompared = applicabilityLine({ applicable: true, staleReasons: [], staleBasis: [] });
  assert.match(uncompared, /compared: nothing\. An empty comparison is not a match/,
    "an empty staleBasis is not silently rendered as a clean comparison");
});

test("a_cancelled_preflight_reports_an_absent_verdict_and_not_a_stale_one", () => {
  const item = preflight("preflight-cancelled.json");
  assert.deepEqual(item.staleReasons, []);
  assert.equal(item.stale, false);
  const html = renderPreflight(item);
  assert.match(html, /cancelled: no result/);
  assert.doesNotMatch(
    html.replace(/is shown as out of date, never as a verdict\./g, ""),
    /out of date/,
    "the only 'out of date' in the render is the standing sentence about applicability, not a " +
      "claim that this cancelled check's verdict is stale -- it has no verdict at all",
  );
  assert.match(html, /compared: nothing\. An empty comparison is not a match/);
});

test("the_check_table_prints_an_absent_field_rather_than_a_guess", () => {
  const html = checkTable([{ id: "a.b", state: "unknown" }], "none");
  assert.match(html, /<code>a\.b<\/code>/);
  assert.match(html, /badge-pending">unknown/);
  const cells = html.split("<td").length - 1;
  assert.equal(cells, 8, "every column is rendered even when the producer recorded nothing");
});

test("the_execution_intention_sentence_no_longer_calls_itself_a_check", () => {
  const sentence = preflightSentence(2);
  assert.match(sentence, /^At execution time Logweir will create 2 topics/);
  assert.match(sentence, /it is not a check and nothing above has confirmed it/);
});

test("a_check_operation_is_a_different_document_and_carries_no_verdict_fields", () => {
  const item = decodeCheckOperation(console_("check-operation-discovery.json")).value.item;
  assert.equal(item.kind, "discovery");
  assert.equal(item.cancellable, true);
  for (const absent of ["result", "evidence", "verification", "verifiedSuccess"]) {
    assert.equal(item[absent], undefined, absent + " is not a field of a transient check");
  }
  assert.deepEqual(CONSOLE_ENUMS.CheckOperationKind.slice(), ["discovery", "preflight"]);
});

test("a_cancel_that_found_a_finished_check_says_so_rather_than_claiming_it_stopped_one", () => {
  const first = decodeCancel(console_("cancel-discovery.json")).value;
  assert.equal(first.alreadyTerminal, false);
  assert.equal(first.state, "cancelled");
  const second = decodeCancel(console_("cancel-already-terminal.json")).value;
  assert.equal(second.alreadyTerminal, true);
  assert.equal(second.state, "succeeded", "and the state it really is, not 'cancelled'");
});

// ===========================================================================
// the destination selector, the coverage labels and the topic picker
// ===========================================================================

test("the_selector_is_bound_to_a_uid_and_refuses_a_recreated_destination", () => {
  const all = decodeConsoleList("destinations", console_("destinations-list.json")).value.items;
  const selected = resolveDestinationSelection(all, { uid: all[0].uid, name: "primary" });
  assert.equal(selected.state, "selected");

  const recreated = resolveDestinationSelection(all, { uid: "gone-uid", name: "primary" });
  assert.equal(recreated.state, "recreated");
  assert.equal(recreated.recreatedUid, all[0].uid);
  const html = renderDestinationSelector({
    id: "s", name: "destination", destinations: all,
    selection: { uid: "gone-uid", name: "primary" },
  });
  assert.match(html, /id="s-refusal"/);
  assert.match(html, /a different archive location reached with a different credential/);
  assert.match(html, /<option value="" selected/,
    "a refusal selects NOTHING, so one more click cannot submit a destination nobody chose");

  const missing = resolveDestinationSelection(all, { uid: "gone-uid", name: "vanished" });
  assert.equal(missing.state, "missing");
});

test("the_selector_preselects_the_namespace_default_and_refuses_to_pick_among_two", () => {
  const all = decodeConsoleList("destinations", console_("destinations-list.json")).value.items;
  assert.equal(defaultDestination(all).name, "primary");
  const html = renderDestinationSelector({ id: "s", destinations: all, selection: {} });
  assert.match(html, /id="s-default"/);
  assert.match(html, /namespace default/);

  const two = all.map((d) => Object.assign({}, d, { default: true }));
  assert.equal(defaultDestination(two), null, "two defaults are no default");
  const ambiguous = renderDestinationSelector({ id: "s", destinations: two, selection: {} });
  assert.match(ambiguous, /id="s-no-default"/);
  assert.match(ambiguous, /A namespace with two defaults has none/);
  assert.match(ambiguous, /<option value="" selected/);

  const none = renderDestinationSelector({ id: "s", destinations: [], selection: {} });
  assert.match(none, /id="s-none"/);
});

test("MUTANT_the_coverage_labels_are_the_controllers_own_strings_character_for_character", () => {
  // THE GUARD: `COVERAGE_LABELS` copies `Coverage::label()`'s three strings and
  // this row holds the copies. There is no JavaScript link to a Rust constant,
  // so the copy is what can drift -- and "visible user topics only" becoming
  // "all topics" in the one place an auditor reads is exactly what the Rust
  // side documents as the failure. THE MUTANT: shorten
  // `VisibleUserTopicsOnly` to "Visible user topics". Planted by hand; the
  // third assertion went red.
  assert.equal(COVERAGE_LABELS.NamedTopics, "Named topics");
  assert.equal(COVERAGE_LABELS.AllUserTopicsAttested, "All user topics (attested complete)");
  assert.equal(
    COVERAGE_LABELS.VisibleUserTopicsOnly,
    "Visible user topics only \u2014 completeness not established",
  );
  assert.equal(claimsWholeCluster("AllUserTopicsAttested"), true);
  assert.equal(claimsWholeCluster("VisibleUserTopicsOnly"), false);
  assert.equal(claimsWholeCluster("NamedTopics"), false);

  const attested = renderCoverageLine({
    spec: { topics: [] }, status: { selection: { coverage: "AllUserTopicsAttested" } },
  });
  assert.match(attested, /data-coverage="AllUserTopicsAttested"/);
  assert.match(attested, /All user topics \(attested complete\)/);
  const visible = renderCoverageLine({
    spec: { topics: [] }, status: { selection: { coverage: "VisibleUserTopicsOnly" } },
  });
  assert.match(visible, /completeness not established/);
  assert.doesNotMatch(visible, /badge-green/, "only an attested coverage gets the green badge");
});

test("an_empty_topic_list_is_named_and_a_named_allowlist_carries_no_coverage_line", () => {
  const html = renderCoverageLine({ spec: { topics: [] } });
  assert.match(html, /data-coverage="unknown"/);
  assert.match(html, /Read the object with kubectl/);
  assert.doesNotMatch(html, /All user topics/,
    "guessing 'all user topics' from an empty list would invent the claim the labels bound");

  const dynamic = renderCoverageLine({
    spec: { topics: [], allUserTopics: { incompleteDiscovery: "Refuse" } },
  });
  assert.match(dynamic, /data-coverage="dynamic"/);
  assert.match(dynamic, /<code>Refuse<\/code>/);
  assert.equal(renderCoverageLine({ spec: { topics: ["orders"] } }), "",
    "and a named allowlist needs no coverage line at all");
});

test("the_topic_picker_offers_names_and_never_replaces_the_text_field", () => {
  const latest = decodeDiscoveryLatest(console_("discovery-latest.json")).value;
  const topics = decodeTopicPage(console_("topics-page.json")).value.items;
  const html = renderTopicPicker({
    latestAttempt: latest.latestAttempt, lastSuccessful: latest.lastSuccessful, topics: topics,
  });
  assert.match(html, /<datalist id="topic-options">/);
  assert.match(html, /<option value="orders">/);
  assert.match(html, /data-picker="failed"/, "a failed newest attempt is labelled");
  assert.match(html, /The names offered, if any, are from an older run/);

  const limited = renderTopicPicker({ lastSuccessful: discovery("discovery-limited.json"), topics: [] });
  assert.match(limited, /data-picker="limited"/);
  assert.match(limited, /Type any topic it does not offer/);

  const stale = renderTopicPicker({ lastSuccessful: discovery("discovery-stale.json"), topics: [] });
  assert.match(stale, /data-picker="stale"/);

  assert.match(renderTopicPicker({}), /id="topic-picker-none"/);
});

test("the_readiness_panel_says_what_a_ready_verdict_is_not", () => {
  const all = decodeConsoleList("destinations", console_("destinations-list.json")).value.items;
  const html = renderReadinessPanel({
    mayOperate: true, destinations: all, clusters: { items: [] },
    preflight: preflight("preflight-ready.json"), state: {},
  });
  assert.match(html, /It is not a promise about the next run/);
  assert.match(html, /a credential can be rotated, a topic created and an ACL changed/);
  assert.match(html, /id="readiness-destination-field"/);
  assert.match(html, /badge-green">ready/);

  const readOnly = renderReadinessPanel({ mayOperate: false, destinations: all, state: {} });
  assert.match(readOnly, /may read readiness results in this namespace and not start one/);
  assert.doesNotMatch(readOnly, /id="readiness-form"/);
});

test("a_contract_failure_names_the_dto_and_the_path_for_every_new_domain", () => {
  const body = console_("destination.json");
  delete body.item.canonicalUrl;
  assert.throws(() => decodeConsoleItem("destinations", body), (error) => {
    assert.ok(isContractFailure(error));
    assert.equal(error.contract.dto, "Destination");
    assert.equal(error.contract.path, "item.canonicalUrl");
    return true;
  });

  const page = console_("topics-page.json");
  page.scan.complete = "yes";
  assert.throws(() => decodeTopicPage(page), (error) => {
    assert.equal(error.contract.path, "scan.complete");
    return true;
  });

  const check = console_("preflight-ready.json");
  check.item.state = "almostReady";
  assert.throws(() => decodeConsoleItem("preflights", check), (error) => {
    assert.match(error.message, /expected one of pending, queued, running, ready/);
    return true;
  });
});

// ===========================================================================
// what D1 W6 landed, and what is still missing — fix round 1
// ===========================================================================
//
// THE ROWS BELOW ARE PAIRED ON PURPOSE. For each of the four things D2 §9 asks
// for, one row asserts what the page now DOES with the field that landed, and
// one asserts that the sentence about what is still missing names the task
// that owes it. A "not available yet" sentence with no owner is how a gap
// becomes a permanent feature of a page; a "not available yet" sentence that
// is FALSE — which two of these became when `89bba9e` landed `destinationRef`
// and `allUserTopics` — is worse, because an operator acts on it.

test("a_schedule_that_names_a_destination_is_rendered_by_that_name_and_its_location", () => {
  // PRESENT: `ScheduleView.destinationRef` landed with D1 W6 (PLAT-06.2).
  const schedules = decodeConsoleList("schedules", console_("schedules-list.json")).value;
  const named = schedules.items.find((s) => s.name === "everything-nightly");
  assert.equal(named.destinationRef.name, "primary", "the fixture is the landed shape");
  assert.equal(named.allUserTopics.incompleteDiscovery, "Refuse");

  const destinations =
    decodeConsoleList("destinations", console_("destinations-list.json")).value.items;
  const cell = destinationCell(
    { spec: { destinationRef: { name: "primary" } } },
    destinations,
  );
  assert.match(cell, /badge-green">primary/);
  assert.match(cell, /s3:\/\/kafka-backups\/team-a\/prod/,
    "and the cell shows WHERE that destination writes, not just its name");
});

test("a_schedule_naming_a_destination_that_is_gone_is_a_refusal_to_say_where_it_writes", () => {
  // THE SAME RULE THE SELECTOR HOLDS. A destination deleted and recreated under
  // one name is a different archive location reached with a different
  // credential, so a cell that resolved the name onto whatever holds it now
  // would answer a question about the old object with a fact about the new one.
  const destinations =
    decodeConsoleList("destinations", console_("destinations-list.json")).value.items;
  const cell = destinationCell({ spec: { destinationRef: { name: "vanished" } } }, destinations);
  assert.match(cell, /badge-unverified">names vanished/);
  assert.match(cell, /will not say where this schedule writes/);
  assert.doesNotMatch(cell, /s3:\/\//, "and it names no location at all");
});

test("a_schedule_with_no_destination_ref_shows_its_inline_archive_and_says_which_it_is", () => {
  const cell = destinationCell(
    { spec: { archive: { url: "s3://kafka-backups/orders" } } },
    [],
  );
  assert.match(cell, /badge-pending">inline archive/);
  assert.match(cell, /s3:\/\/kafka-backups\/orders/);
  assert.equal(destinationCell({ spec: {} }, []), "-",
    "and a schedule with neither prints the absent marker rather than a guess");
});

test("the_schedule_list_carries_a_destination_column_for_all_three_shapes", () => {
  const schedules = decodeConsoleList("schedules", console_("schedules-list.json")).value;
  const destinations =
    decodeConsoleList("destinations", console_("destinations-list.json")).value.items;
  // `renderScheduleList` takes the CR-shaped projection the pages read, so the
  // rows are built the way `client.js` builds them.
  const projected = schedules.items.map((item) => ({
    metadata: { name: item.name },
    spec: {
      schedule: item.schedule,
      archive: item.archive,
      destinationRef: item.destinationRef === null ? undefined : item.destinationRef,
    },
    status: {},
  }));
  const html = renderScheduleList({ items: projected }, destinations);
  assert.match(html, /<th scope="col">DESTINATION<\/th>/);
  assert.match(html, /badge-green">primary/, "the one that names a live destination");
  assert.match(html, /badge-unverified">names vanished/, "the one that names a gone one");
  assert.match(html, /badge-pending">inline archive/, "and the legacy one");
});

test("MUTANT_the_create_form_names_the_task_that_owes_a_schedule_destination", () => {
  // ABSENT, AND STILL ABSENT AFTER THE REBASE: `CreateScheduleRequest` has no
  // `destinationRef`. THE MUTANT this row exists for is the sentence going
  // stale the other way -- claiming a field is missing after it lands. It is
  // held by naming BOTH halves: what is missing (the create route) and what is
  // not (the edit route), so a future reader can check either against the
  // schema in one step.
  const html = renderScheduleForm({});
  assert.match(html, /id="schedule-destination-gap"/);
  assert.match(html, /POST \/schedules/, "the sentence names the route that lacks the field");
  assert.match(html, /PLAT-06\.2 owes that one/, "and the task that owes it");
  assert.doesNotMatch(
    CREATE_HAS_NO_DESTINATION,
    /cannot be named here yet|kubectl or the CLI to bind a schedule/,
    "the pre-rebase sentence claimed no schedule could name a destination at all, which " +
      "`ScheduleView.destinationRef` and `PUT .../schedules/{name}` have both made false",
  );
});

test("the_edit_route_is_named_as_existing_and_as_out_of_this_pages_contract", () => {
  // THE HONEST REASON. `PUT .../schedules/{name}` DOES take `destinationRef`
  // under `expectedGeneration`. This page does not send it because `ui/api.js`
  // exports `create` plus one narrow suspend patch and no replace at all, and
  // `ui_lint::the_api_module_offers_no_delete_and_no_put` fails on the bare
  // token for one. Saying "the field does not exist" would have been false;
  // saying nothing would have left an operator looking for a control.
  const html = renderScheduleForm({});
  assert.match(html, /id="schedule-destination-edit"/);
  assert.match(EDIT_IS_A_REPLACE, /PUT \.\.\.\/schedules\/\{name\} takes destinationRef/);
  assert.match(EDIT_IS_A_REPLACE, /expectedGeneration/);
  assert.match(EDIT_IS_A_REPLACE, /a field omitted is removed/,
    "and why a partial edit from this page would be a destructive one");
  assert.match(EDIT_IS_A_REPLACE, /D1 W7/, "with the form that owns that route named");
});

test("a_dynamic_selection_is_now_readable_and_its_coverage_still_is_not", () => {
  // HALF LANDED. `ScheduleView.allUserTopics` arrived with D1 W6, so "which of
  // the two shapes is this schedule" is answerable and the old two-shapes
  // sentence is gone. `status.selection` / `coverage` did NOT arrive -- no API
  // projection publishes either -- so `Coverage::label()` still cannot be
  // rendered, and the page names who owes it instead of inventing a label.
  const html = renderCoverageLine({
    spec: {
      topics: [],
      allUserTopics: {
        incompleteDiscovery: "Refuse",
        exclude: { topics: ["scratch"], prefixes: ["tmp-"] },
      },
    },
  });
  assert.match(html, /data-coverage="dynamic"/);
  assert.match(html, /<code>Refuse<\/code>/, "the policy's own answer to incomplete visibility");
  assert.match(html, /Excluded: scratch, tmp-\*/, "and what the selection leaves out");
  assert.match(html, /PLAT-09\.2 owes the projection and D1 W7 owes the surface/);
  assert.doesNotMatch(html, /All user topics \(attested complete\)/,
    "no coverage LABEL is rendered, because none is published");
});

test("a_schedule_with_neither_shape_is_now_named_exactly_rather_than_guessed_between", () => {
  const html = renderCoverageLine({ spec: { topics: [] } });
  assert.match(html, /data-coverage="unknown"/);
  assert.match(html, /an empty allowlist is not an allowlist/);
  assert.doesNotMatch(html, /this page cannot tell you which it is/,
    "the pre-rebase sentence hedged between two shapes; `allUserTopics` distinguishes them now");
});

test("the_wizard_names_the_task_that_owes_a_recovery_points_frozen_destination", () => {
  assert.match(SOURCE_DESTINATION_NOT_PUBLISHED, /PLAT-08\.2 \(D2 W10\) owes that projection/);
  assert.match(SOURCE_DESTINATION_NOT_PUBLISHED, /neither destinationRef nor locationDigest/);
});

test("the_clusters_page_names_the_task_that_owes_a_connectivity_check_kind", () => {
  assert.match(NO_CONNECTIVITY_CHECK_KIND, /PLAT-03\.1 owes a source-connectivity check kind/);
  assert.match(NO_CONNECTIVITY_CHECK_KIND, /PLAT-07\.2 consumes it/);
  const panel = renderDiscoveryPanel({ mayOperate: true, filters: {}, state: {} });
  assert.match(panel, /id="no-connectivity-check"/);
});

// ------------------------------------------------- the rotation, after F3

test("MUTANT_the_rotation_is_checked_before_it_is_sent", () => {
  // THE GUARD: `submitRotation` runs `validateGrants` and throws `invalid`
  // without a request when a grant does not make a credential. THE MUTANT:
  // call `updateDestinationAccess` straight from the form, which is what the
  // code did before this round -- an operator who chose `new` and typed
  // nothing sent `secret.new.accessKeyId: ""`. Planted by hand; the second
  // assertion went red.
  const problems = validateGrants({ archiveWriteSource: "new" });
  assert.match(problems.archiveWriteAccessKeyId, /needs both an access key id and a secret/);
  assert.match(problems.archiveWriteAccessKeyId, /cleared on every render/,
    "and the message says why a retry needs them typed again");

  let sent = false;
  return submitRotation("team-a", "primary", { archiveWriteSource: "new" }, 2, {
    updateDestinationAccess() {
      sent = true;
      return Promise.resolve({});
    },
  }).then(
    () => assert.fail("the rotation was sent with an empty credential"),
    (error) => {
      assert.equal(sent, false, "nothing reached the transport");
      assert.equal(error.kind, "invalid");
      assert.match(error.fields.archiveWriteAccessKeyId, /needs both an access key id/,
        "and the message travels on the field it is about, which is what puts it beside the input");
    },
  );
});

test("MUTANT_a_credential_form_never_claims_it_kept_what_it_just_cleared", () => {
  // THE GUARD: a form whose subject carries `clearsCredentials: true` gets
  // `CREDENTIALS_CLEARED_CLAUSE` instead of " Your input is kept." THE MUTANT:
  // drop the flag from the rotate form's subject. Planted by hand; the second
  // and fourth assertions went red.
  //
  // The failure this is about is real and was on screen: the 409 banner said
  // "nothing was changed. Your input is kept" directly above twelve credential
  // fields the re-render had just emptied.
  const conflicted = {
    phase: "failed",
    kind: "conflict",
    error: {
      message: "The Secret `lwd-primary-archive-write` already exists",
      existing: { name: "primary", uid: "u1" },
    },
  };
  const rotate = renderRotateForm({ name: "primary", generation: 2 },
    { mayOperate: true, rotate: { state: conflicted } });
  assert.match(rotate, /nothing was changed/);
  assert.doesNotMatch(rotate, /Your input is kept/);
  assert.ok(rotate.indexOf("except the credential fields") !== -1);
  assert.ok(
    CREDENTIALS_CLEARED_CLAUSE.indexOf("never kept anywhere, so type them again") !== -1,
    "the true sentence tells the operator what to do, not just what happened",
  );

  const create = renderDestinationForm({ state: conflicted });
  assert.doesNotMatch(create, /Your input is kept/,
    "the create form carries credential inputs too and gets the same sentence");

  // AND EVERY OTHER FORM IS UNCHANGED: the clause is a property of the form,
  // not of the error, so a form with no credential input still says the true
  // thing for it.
  const schedule = renderScheduleForm({ state: conflicted });
  assert.match(schedule, /Your input is kept/);
});

test("MUTANT_every_credential_field_name_is_outside_the_draft_allowlist_including_the_token", () => {
  // THE EXTENSION OF M1 (review F4). `grantBody` already reads
  // `<role>SessionToken` and puts it in `secret.new`, so the name belongs in
  // `CREDENTIAL_INPUTS` before the input exists rather than after somebody
  // notices. THE MUTANT: add `archiveWriteSessionToken` to the draft
  // allowlist. Planted by hand; the loop below went red on that name.
  assert.equal(CREDENTIAL_INPUTS.length, 12, "three field names per role, four roles");
  for (const role of GRANT_ROLES) {
    assert.ok(CREDENTIAL_INPUTS.indexOf(role + "SessionToken") !== -1,
      role + "SessionToken is a credential value grantBody reads");
  }
  for (const input of CREDENTIAL_INPUTS) {
    assert.equal(DESTINATION_DRAFT_FIELDS.indexOf(input), -1, input + " is in the allowlist");
  }
  const kept = keepDraft("d2-draft/token", {
    bucket: "kafka-backups",
    archiveWriteSessionToken: "FwoGZXIvYXdzEAAaDK-session-token-value",
  }, DESTINATION_DRAFT_FIELDS);
  assert.equal(kept.bucket, "kafka-backups");
  assert.equal(kept.archiveWriteSessionToken, undefined);
  assert.doesNotMatch(JSON.stringify(readDraft("d2-draft/token")), /session-token-value/);
});
