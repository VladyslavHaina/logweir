// tests/selector.spec.js -- PLAT-07.2: the saved-cluster selector, connection
// contract v1's references in the cluster form, and the honesty of a connection
// probe.
//
// EVERY ROW HERE IS ABOUT ONE OF FIVE THINGS THE TRACKER NAMES: a cluster
// deleted and recreated, a delayed refresh, a credential rotation, a change of
// role/capability, and namespace navigation. Each is asserted in BOTH client
// modes wherever the two modes can disagree -- the legacy direct-CR mode reads
// a `KafkaCluster` and the console mode reads a `Connection` the product API
// projected, and a rule that held on one shape and not the other would be a
// rule this page does not actually have.
//
// AND EACH GUARD HAS A MUTANT. The five the task names are recorded beside the
// row that kills them: selection by name, a stale observation rendered as
// valid, a password field kept in a draft, a recreated cluster silently
// re-selected, and the word `ready`.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import {
  CONNECTION_REFUSAL_REASONS,
  EMPTY_OPTION,
  FORBIDDEN_PROBE_WORD,
  PROBE_FRESH_SECONDS,
  PROBE_NOUN,
  clusterHaystack,
  clusterUid,
  filterSelectorOptions,
  haystackMatches,
  optionCaption,
  preferredCluster,
  probeBadge,
  probeLine,
  observedBadge,
  probeState,
  readClusterSelection,
  renderClusterSelector,
  renderProbePanel,
  resolveClusterSelection,
  savedClusters,
  staleBadge,
} from "../select.js";
import {
  CLUSTER_DRAFT_FIELDS,
  CLUSTER_FORM,
  FORBIDDEN_CLUSTER_FIELDS,
  TLS_CA_KINDS,
  authCell,
  caWords,
  clusterBody,
  mountClusterDetail,
  mountClusters,
  probeCell,
  renderClusterDetail,
  renderClusterForm,
  renderClusterList,
  validateCluster,
} from "../pages/clusters.js";
import {
  SCHEDULE_DRAFT_FIELDS,
  confirmThenCreate,
  renderScheduleForm,
  scheduleBody,
  sourceRefusal,
  submitSchedule,
  validateSchedule,
} from "../pages/schedules.js";
import {
  confirmClusters,
  initialState,
  recoveryPoints,
  renderTargetStep,
  resolveTarget,
  selectTarget,
  submitRestore,
  validateRestore,
  wizardDraftValues,
  applyWizardDraft,
} from "../pages/restore-wizard.js";
import { CONSOLE, apiClient, resetMode, selectMode } from "../client.js";
import { formKey, keepDraft, readDraft } from "../lifecycle.js";
import { createRouteLifecycle } from "../app.js";

const fixture = (name) => JSON.parse(readFileSync(new URL("./fixtures/" + name, import.meta.url)));

/** A `KafkaCluster` the API server could have returned, with the identity and
 *  the probe the row is about. Built rather than fixtured because most rows
 *  here vary one field of it. */
function cluster(over) {
  const o = over || {};
  return {
    apiVersion: "logweir.dev/v1alpha1",
    kind: "KafkaCluster",
    metadata: {
      name: o.name === undefined ? "orders-prod" : o.name,
      namespace: "team-a",
      uid: o.uid === undefined ? "uid-a" : o.uid,
      resourceVersion: "1",
    },
    spec: {
      bootstrapServers: o.servers === undefined ? ["kafka-0.orders.svc:9093"] : o.servers,
      auth: o.auth === undefined ? { mode: "plaintext", tls: false } : o.auth,
      role: o.role === undefined ? "source" : o.role,
    },
    status: o.status === undefined
      ? { reachable: true, clusterId: "CID", observedAt: "2026-09-16T12:00:00Z", reason: "Reachable" }
      : o.status,
  };
}

const NOW = Date.parse("2026-09-16T12:01:00Z");

/** A minimal DOM stand-in for one selector: a `<select>` with its options and
 *  the two hidden inputs beside it. The suite has no DOM (Global Constraint
 *  21), so the DOM half is exercised against the same shape the browser hands
 *  it -- `value`, `options`, `selectedIndex`, `getAttribute`, `hidden`. */
function selectNode(options, selectedIndex) {
  const nodes = options.map((option) => ({
    value: option.uid,
    hidden: false,
    selected: false,
    getAttribute(name) {
      if (name === "data-name") {
        return option.name;
      }
      if (name === "data-search") {
        return option.search === undefined ? option.name : option.search;
      }
      return null;
    },
  }));
  const index = selectedIndex === undefined ? 0 : selectedIndex;
  if (nodes[index] !== undefined) {
    nodes[index].selected = true;
  }
  const select = {
    options: nodes,
    selectedIndex: index,
    get value() {
      return nodes[this.selectedIndex] === undefined ? "" : nodes[this.selectedIndex].value;
    },
  };
  const hidden = {
    "#s-uid": { value: "from-hidden-uid" },
    "#s-name": { value: "from-hidden-name" },
  };
  return {
    select: select,
    form: {
      querySelector(selector) {
        if (selector === "#s") {
          return select;
        }
        return hidden[selector] === undefined ? null : hidden[selector];
      },
    },
  };
}

// ===========================================================================
// 1. IDENTITY. The selector chooses an object, not a label.
// ===========================================================================

test("a_selection_is_resolved_by_uid_and_a_rename_does_not_move_it", () => {
  const clusters = [cluster({ uid: "uid-a", name: "orders-prod" })];
  const first = resolveClusterSelection(clusters, { uid: "uid-a", name: "orders-prod" });
  assert.equal(first.state, "selected");
  assert.equal(first.uid, "uid-a");
  assert.equal(first.renamedFrom, "");

  // THE RENAME. Same object, new label: the selection holds and the page says
  // what it used to be called.
  const renamed = [cluster({ uid: "uid-a", name: "orders-prod-2" })];
  const after = resolveClusterSelection(renamed, { uid: "uid-a", name: "orders-prod" });
  assert.equal(after.state, "selected", "the uid still answers, so the selection is unchanged");
  assert.equal(after.name, "orders-prod-2", "and the name it reports is the CURRENT one");
  assert.equal(after.renamedFrom, "orders-prod");
  assert.equal(after.cluster, renamed[0], "and the object is the one the uid named");

  // THE MUTANT this row kills: `resolveClusterSelection` matching on
  // `clusterName(cluster) === name` whenever a name is given. Under it the
  // rename resolves to nothing (`missing`) and the recreated case below
  // resolves to the impostor -- so selection by name fails here first.
  const byNameWouldFind = renamed.some((c) => c.metadata.name === "orders-prod");
  assert.equal(byNameWouldFind, false, "the old name is gone; only the uid can find this object");
});

test("a_deleted_and_recreated_cluster_is_refused_and_names_both_uids", () => {
  const before = [cluster({ uid: "uid-a", name: "orders-prod" })];
  const after = [cluster({ uid: "uid-b", name: "orders-prod", servers: ["elsewhere:9092"] })];

  const recreated = resolveClusterSelection(after, { uid: "uid-a", name: "orders-prod" });
  assert.equal(recreated.state, "recreated", "not `selected`, and not the newest one instead");
  assert.equal(recreated.cluster, null, "nothing is handed back to build a run from");
  assert.equal(recreated.uid, "uid-a", "the uid that was chosen");
  assert.equal(recreated.recreatedUid, "uid-b", "and the uid now under that name");

  const refusal = renderClusterSelector({
    id: "t", name: "t", clusters: after, selection: { uid: "uid-a", name: "orders-prod" }, now: NOW,
  });
  assert.match(refusal, /id="t-refusal"/, "the selector renders the refusal");
  assert.match(refusal, /role="alert"/);
  assert.ok(refusal.includes("uid-a") && refusal.includes("uid-b"), "naming both: " + refusal);
  assert.match(refusal, /different set of brokers reached with a different credential/);
  assert.equal(
    refusal.indexOf("value=\"uid-b\" selected"),
    -1,
    "and the impostor is NOT preselected: a refusal that quietly picks the replacement is the " +
      "defect this rule exists to prevent",
  );

  // A CLUSTER SIMPLY GONE is `missing`, which is a different sentence.
  const gone = resolveClusterSelection([], { uid: "uid-a", name: "orders-prod" });
  assert.equal(gone.state, "missing");
  assert.equal(gone.recreatedUid, "");

  // AND THE PRE-CONDITION: before the delete, the same selection resolved.
  assert.equal(resolveClusterSelection(before, { uid: "uid-a", name: "orders-prod" }).state, "selected");
});

test("an_existing_name_only_reference_still_resolves_and_is_pinned_from_then_on", () => {
  // MIGRATION. `BackupSchedule.spec.sourceRef.name` was written before saved
  // connections had identities in this page, and a draft kept before PLAT-07.2
  // carries a name and no uid. Both must keep working, and both become
  // identities the moment they resolve.
  const clusters = [cluster({ uid: "uid-a", name: "orders-prod" })];
  const legacy = resolveClusterSelection(clusters, { uid: "", name: "orders-prod" });
  assert.equal(legacy.state, "selected");
  assert.equal(legacy.uid, "uid-a", "the uid it resolved to is pinned");
  assert.equal(legacy.pinned, true);

  const rendered = renderClusterSelector({
    id: "s", name: "source", clusters: clusters, selection: { uid: "", name: "orders-prod" }, now: NOW,
  });
  assert.match(rendered, /data-selection-pinned="true"/, "and the page says so: " + rendered);
  assert.ok(rendered.includes("value=\"uid-a\" selected"));

  // A name-only reference can never be `recreated`: there was no recorded
  // identity for anything to have changed.
  const other = [cluster({ uid: "uid-b", name: "orders-prod" })];
  assert.equal(resolveClusterSelection(other, { uid: "", name: "orders-prod" }).state, "selected");
});

test("nothing_chosen_is_none_and_the_default_is_a_visible_preselect_not_a_decision", () => {
  const clusters = [
    cluster({ uid: "uid-a", name: "b-source", role: "source" }),
    cluster({ uid: "uid-b", name: "a-target", role: "target" }),
  ];
  assert.equal(resolveClusterSelection(clusters, { uid: "", name: "" }).state, "none");
  assert.equal(clusterUid(preferredCluster(clusters, "target")), "uid-b");
  assert.equal(clusterUid(preferredCluster(clusters, "source")), "uid-a");
  assert.equal(clusterUid(preferredCluster(clusters, undefined)), "uid-b", "else the first by name");
  assert.equal(preferredCluster([], "target"), null);

  // ORDERED BY NAME, and every cluster is offered whatever its role: the CRD
  // documents `role` as a label the adopter picks, and the runner's own guard
  // is the gate. A selector that filtered by role rendered an empty select for
  // this product's own demo (Task 28a).
  assert.deepEqual(savedClusters(clusters).map((c) => c.metadata.name), ["a-target", "b-source"]);
  const rendered = renderClusterSelector({
    id: "t", name: "t", clusters: clusters, selection: {}, prefer: "target", now: NOW,
  });
  assert.ok(rendered.includes("value=\"uid-a\""), "the source cluster is offered too");
  assert.ok(rendered.includes("value=\"uid-b\" selected"), "and the preferred role is preselected");
});

// ===========================================================================
// 2. FRESHNESS. `status.reachable` is a probe, and an old one says so.
// ===========================================================================

test("a_probe_older_than_the_freshness_budget_is_stale_and_says_it_is", () => {
  const fresh = probeState(cluster({}), NOW);
  assert.equal(fresh.verdict, "reachable");
  assert.equal(fresh.stale, false);
  assert.equal(fresh.freshSeconds, PROBE_FRESH_SECONDS);

  // ONE SECOND PAST THE BUDGET, and not before: the boundary is asserted from
  // both sides, because a `>=` here would flag a working installation and a
  // `>` too far out would keep presenting a dead probe as current.
  const at = Date.parse("2026-09-16T12:00:00Z");
  const edge = probeState(cluster({}), at + PROBE_FRESH_SECONDS * 1000);
  assert.equal(edge.stale, false, "exactly at the budget is still fresh");
  const over = probeState(cluster({}), at + PROBE_FRESH_SECONDS * 1000 + 1);
  assert.equal(over.stale, true, "and one millisecond past it is not");
  assert.equal(over.verdict, "reachable", "the verdict is unchanged: two facts, not one");

  const line = probeLine(over);
  assert.match(line, /badge-warn">stale</, "the stale badge is rendered: " + line);
  assert.match(line, /older than the 630s freshness budget/);
  assert.ok(line.includes("observed"), "with the instant it describes");

  // AN OBSERVATION IN THE FUTURE beyond the tolerated skew is stale too: the
  // arithmetic itself cannot be trusted, and an untrustworthy number presented
  // as current is the same defect wearing a different hat.
  assert.equal(probeState(cluster({}), at - 30 * 1000).stale, false, "a minute of skew is not news");
  assert.equal(probeState(cluster({}), at - 120 * 1000).stale, true);

  // THE MUTANT this row kills: `stale` hard-wired to `false` (or the age
  // comparison removed) so every observation renders as current.
  assert.equal(staleBadge(over).length > 0, true);
  assert.equal(staleBadge(fresh), "");
});

test("every_absent_reachable_case_gets_its_own_words_and_the_controllers_own_reason", () => {
  const table = [
    [{ reachable: true, observedAt: "2026-09-16T12:00:00Z", reason: "Reachable" }, "reachable"],
    [
      { reachable: false, observedAt: "2026-09-16T12:00:00Z", reason: "ProbeReportedUnreachable" },
      "unreachable",
    ],
    [{ reason: "ProbeRunning" }, "probing"],
    [{ reason: "ProbeOutputUnreadable", observedAt: "2026-09-16T12:00:00Z" }, "unknown"],
    [{ reason: "PodUnschedulable", observedAt: "2026-09-16T12:00:00Z" }, "unknown"],
    [{}, "never"],
  ];
  for (const [status, verdict] of table) {
    assert.equal(probeState(cluster({ status: status }), NOW).verdict, verdict, JSON.stringify(status));
  }
  // THE FOUR REFUSALS PLAT-07.1's RESOLVER WRITES. It CLEARS `reachable`, so
  // without this arm the page would render "never probed" over a connection
  // the controller has told it is broken.
  for (const reason of CONNECTION_REFUSAL_REASONS) {
    const state = probeState(cluster({ status: { reason: reason } }), NOW);
    assert.equal(state.verdict, "refused", reason);
    const line = probeLine(state);
    assert.ok(line.includes("<code>" + reason + "</code>"), "verbatim, never reworded: " + line);
    assert.match(probeBadge(state), /badge-danger/);
  }
  // A probe in flight is not stale: there is no observation to be old.
  assert.equal(probeState(cluster({ status: { reason: "ProbeRunning" } }), NOW).stale, false);
  // AND NEITHER IS A CONNECTION THAT WAS NEVER OBSERVED (review finding F2).
  // `stale` means "this reading describes the past" and is a statement ABOUT an
  // observation; with no `observedAt` there is none for it to be about, and
  // labelling it stale said two contradictory things at once and told an
  // operator to wait for a refresh that a refused connection never gets.
  for (const status of [
    { reachable: false },
    { reason: "ConnectionConfigInvalid" },
    { reason: "ProbeOutputUnreadable" },
  ]) {
    const state = probeState(cluster({ status: status }), NOW);
    assert.equal(state.stale, false, "never observed is not stale: " + JSON.stringify(status));
    assert.equal(state.observed, false, "and it says so: " + JSON.stringify(status));
    const line = probeLine(state);
    assert.match(line, /never observed/, "with its own word: " + line);
    assert.equal(
      line.indexOf("older than the"),
      -1,
      "and NOT the freshness clause, which is about an observation that exists: " + line,
    );
    assert.equal(staleBadge(state), "", "and no stale badge");
    assert.match(line, /no observation recorded/);
  }
  // The `never probed` verdict already carries the words, so the badge does not
  // repeat them; a probe in flight has not observed anything YET, which is not
  // the same statement.
  assert.equal(observedBadge(probeState(cluster({ status: {} }), NOW)), "");
  assert.equal(observedBadge(probeState(cluster({ status: { reason: "ProbeRunning" } }), NOW)), "");
  // And an observation that EXISTS and is old is still stale, with the clause.
  const old = probeState(cluster({}), NOW + 3600 * 1000);
  assert.equal(old.stale, true);
  assert.equal(old.observed, true);
  assert.equal(observedBadge(old), "", "the two badges are exclusive");
  assert.match(probeLine(old), /older than the 630s freshness budget/);
});

test("no_probe_surface_anywhere_says_ready", () => {
  const clusters = [
    cluster({ uid: "uid-a" }),
    cluster({ uid: "uid-b", name: "scratch", role: "target", status: { reason: "ProbeRunning" } }),
    cluster({ uid: "uid-c", name: "broken", status: { reason: "ConnectionConfigInvalid" } }),
  ];
  const wizard = initialState(
    "ns",
    { items: clusters },
    fixture("wizard-backups.json"),
    { uid: recoveryPoints(fixture("wizard-backups.json"))[0].metadata.uid, backup: "" },
  );
  const surfaces = [
    ["renderClusterList", renderClusterList({ items: clusters }, "ns", NOW)],
    ["renderClusterDetail", renderClusterDetail(clusters[0], NOW)],
    ["renderProbePanel", renderProbePanel(clusters[2], { now: NOW })],
    ["renderClusterSelector", renderClusterSelector({
      id: "s", name: "source", clusters: clusters, selection: {}, now: NOW,
    })],
    ["renderScheduleForm", renderScheduleForm({ clusters: { items: clusters }, now: NOW })],
    ["renderTargetStep", renderTargetStep(wizard)],
  ];
  for (const [label, html] of surfaces) {
    // THE WORD, MATCHED BARE, so `already` in a neighbouring sentence is not a
    // false positive and `Ready` in a heading is not a pass.
    assert.doesNotMatch(
      html,
      new RegExp("\\b" + FORBIDDEN_PROBE_WORD + "\\b", "i"),
      label + " must not describe a connection probe as ready (D2 section 9): " + html,
    );
    assert.ok(
      html.indexOf(PROBE_NOUN) !== -1,
      label + " calls the reading by its name, `" + PROBE_NOUN + "`",
    );
  }
  // THE MUTANT this row kills: `PROBE_NOUN` or any verdict caption changed to
  // the word `ready`. It fails on every surface at once, which is the point of
  // the noun living in one module.
});

// ===========================================================================
// 3. CONTRACT v1's REFERENCES, and the absence of a credential.
// ===========================================================================

test("the_cluster_form_carries_contract_v1s_references_and_never_a_value", () => {
  const form = renderClusterForm();
  // THE TWO NEW REFERENCES ARE INPUTS.
  assert.match(form, /id="cluster-password-key" name="passwordKey"/);
  assert.match(form, /id="cluster-tls-ca-kind" name="tlsCaKind"/);
  assert.match(form, /id="cluster-tls-ca-name" name="tlsCaName"/);
  assert.match(form, /id="cluster-tls-ca-key" name="tlsCaKey"/);
  assert.deepEqual(TLS_CA_KINDS, ["none", "secret", "configMap"]);
  for (const kind of TLS_CA_KINDS) {
    assert.ok(form.includes("<option value=\"" + kind + "\""), kind + " is offered");
  }

  // AND NOTHING A CREDENTIAL COULD BE TYPED INTO.
  assert.equal(form.indexOf("type=\"password\""), -1, "no password input, in any state");
  for (const forbidden of FORBIDDEN_CLUSTER_FIELDS) {
    assert.equal(
      form.indexOf("name=\"" + forbidden + "\""),
      -1,
      "the form has no field named " + forbidden,
    );
    assert.equal(CLUSTER_DRAFT_FIELDS.indexOf(forbidden), -1, "and no draft keeps one");
  }
  // THE MUTANT this row kills: adding `password` to the form and to
  // `CLUSTER_DRAFT_FIELDS`, which is the "password field kept in a draft"
  // mutant. `keepDraft` also drops key material from a DECLARED field, so the
  // two halves are asserted together.
  const key = formKey("mutant-ns", CLUSTER_FORM);
  keepDraft(key, { name: "a", password: "hunter2", passwordKey: "pw" }, CLUSTER_DRAFT_FIELDS);
  assert.equal(readDraft(key).password, undefined, "an undeclared field is never captured");
  assert.equal(readDraft(key).passwordKey, "pw", "and a data KEY's name is, because it is a name");
});

test("the_request_body_spells_contract_v1_and_omits_what_was_left_blank", () => {
  const base = {
    name: "orders-prod", servers: "kafka-0:9093", role: "source",
    mode: "scramSha512", username: "u", secret: "s", tls: true,
  };
  // ABSENT IS A MEANING. A blank key is LEFT OUT rather than sent as the
  // default: an object that omits it is byte-for-byte what every release
  // before contract v1 wrote, and `spec` is immutable, so the difference is
  // permanent and lands in the frozen execution inputs.
  const legacyShape = clusterBody(Object.assign({}, base, { passwordKey: "", tlsCaKind: "none" }));
  assert.deepEqual(legacyShape.spec.auth, { mode: "scramSha512", tls: true, username: "u", secretRef: { name: "s" } });

  const withKey = clusterBody(Object.assign({}, base, { passwordKey: "sasl-pw" }));
  assert.deepEqual(withKey.spec.auth.secretRef, { name: "s", passwordKey: "sasl-pw" });

  const caSecret = clusterBody(Object.assign({}, base, {
    tlsCaKind: "secret", tlsCaName: "ca-bundle", tlsCaKey: "ca.crt",
  }));
  assert.deepEqual(caSecret.spec.auth.tlsCa, { secretKeyRef: { name: "ca-bundle", key: "ca.crt" } });

  const caMap = clusterBody(Object.assign({}, base, {
    tlsCaKind: "configMap", tlsCaName: "ca-bundle", tlsCaKey: "ca.crt",
  }));
  assert.deepEqual(caMap.spec.auth.tlsCa, { configMapKeyRef: { name: "ca-bundle", key: "ca.crt" } });
  assert.equal(
    JSON.stringify(caMap).indexOf("secretKeyRef"),
    -1,
    "exactly one source, which is what the CRD's own CEL rule requires",
  );

  // THE TLS SWITCH IS INDEPENDENT OF THE MODE, both ways round.
  assert.equal(clusterBody(Object.assign({}, base, { mode: "plaintext", tls: false })).spec.auth.tls, false);
  assert.equal(clusterBody(Object.assign({}, base, { tls: true })).spec.auth.tls, true);
});

test("the_form_refuses_the_shapes_the_resolver_and_the_cel_rules_refuse", () => {
  const ok = {
    name: "orders-prod", servers: "kafka-0:9093", role: "source",
    mode: "scramSha512", username: "u", secret: "s", tls: true,
    passwordKey: "", tlsCaKind: "none", tlsCaName: "", tlsCaKey: "",
  };
  assert.deepEqual(Object.keys(validateCluster(ok)), [], "the accepted shape is accepted");

  // `plaintext` + `tls: true` -- TLS without SASL -- is refused rather than
  // dialled in the clear (`weirkeeper::connection::resolve`).
  const plainTls = validateCluster(Object.assign({}, ok, { mode: "plaintext", username: "", secret: "", tls: true }));
  assert.match(plainTls.tls, /refused rather than dialled without TLS/);

  // A CA requires TLS on (the CRD's `x-kubernetes-validations` on `auth`).
  const caNoTls = validateCluster(Object.assign({}, ok, {
    tls: false, tlsCaKind: "secret", tlsCaName: "ca", tlsCaKey: "ca.crt",
  }));
  assert.match(caNoTls.tlsCaName, /requires TLS on/);

  // A data key is `^[-._a-zA-Z0-9]+$`, and a key with no Secret is nothing.
  assert.ok(validateCluster(Object.assign({}, ok, { passwordKey: "not a key" })).passwordKey);
  assert.match(
    validateCluster(Object.assign({}, ok, { secret: "", mode: "plaintext", username: "", passwordKey: "pw" })).passwordKey,
    /name the Secret first/,
  );
  assert.ok(validateCluster(Object.assign({}, ok, {
    tlsCaKind: "configMap", tlsCaName: "Not_A_Name", tlsCaKey: "ca.crt",
  })).tlsCaName);
  assert.ok(validateCluster(Object.assign({}, ok, {
    tlsCaKind: "configMap", tlsCaName: "ca", tlsCaKey: "",
  })).tlsCaKey);
});

test("the_rendered_cluster_views_name_both_references_and_no_value", () => {
  const object = cluster({
    auth: {
      mode: "scramSha512", username: "u", tls: true,
      secretRef: { name: "orders-scram", passwordKey: "sasl-pw" },
      tlsCa: { configMapKeyRef: { name: "ca-bundle", key: "ca.crt" } },
    },
  });
  assert.equal(
    authCell(object.spec),
    "scramSha512 as u via Secret orders-scram key sasl-pw TLS CA from ConfigMap ca-bundle key ca.crt",
  );
  assert.equal(caWords(undefined), "", "a connection with no private CA says nothing about one");
  assert.equal(caWords({ secretKeyRef: { name: "s", key: "k" } }), "CA from Secret s key k");

  const detail = renderClusterDetail(object, NOW);
  assert.ok(detail.includes("orders-scram") && detail.includes("sasl-pw"));
  assert.ok(detail.includes("ca-bundle") && detail.includes("ca.crt"));
  assert.ok(detail.includes("<code id=\"cluster-uid\">uid-a</code>"), "and the identity is shown");

  // The LIST still names no credential word at all, which is what
  // `no_secret_value_is_rendered` has asserted since Task 26: the key's own
  // name travels as `key <name>` and the list never spells the field.
  const list = renderClusterList({ items: [object] }, "ns", NOW);
  assert.ok(list.includes("key sasl-pw"));
});

// ===========================================================================
// 4. THE FIVE TRACKER CASES, end to end, on the forms that use the selector.
// ===========================================================================

test("the_schedule_form_selects_a_source_by_uid_and_refuses_a_recreated_one", async () => {
  const before = { items: [cluster({ uid: "uid-a", name: "orders-prod" })] };
  const after = { items: [cluster({ uid: "uid-b", name: "orders-prod" })] };
  const values = {
    name: "hourly", cron: "0 * * * *", source: "orders-prod", sourceUid: "uid-a",
    topics: "orders", archive: "s3://b/p", archiveSecret: "logweir-s3",
    keepLast: "", keepDays: "",
  };
  assert.ok(SCHEDULE_DRAFT_FIELDS.indexOf("sourceUid") !== -1, "the draft keeps the identity");

  // Resolved: the create goes out with the resolved object's CURRENT name.
  const sent = [];
  await submitSchedule("team-a", values, { create: async (...a) => { sent.push(a); return { metadata: { name: "hourly" } }; } }, before);
  assert.equal(sent[0][2].spec.sourceRef.name, "orders-prod");

  // Recreated: refused, and nothing is sent.
  const none = [];
  await assert.rejects(
    () => submitSchedule("team-a", values, { create: async (...a) => { none.push(a); } }, after),
    (error) => {
      assert.equal(error.kind, "invalid");
      assert.match(error.fields.source, /uid-a/);
      assert.match(error.fields.source, /uid-b/);
      assert.match(error.fields.source, /recreated connection is a different set of brokers/);
      return true;
    },
  );
  assert.equal(none.length, 0, "a refusal sends nothing");
  assert.match(sourceRefusal({ state: "missing", uid: "uid-a", name: "orders-prod" }), /not in this namespace any more/);

  // A RENAME is followed, not refused: the body carries the new name.
  const renamed = { items: [cluster({ uid: "uid-a", name: "orders-prod-2" })] };
  const followed = [];
  await submitSchedule("team-a", values, { create: async (...a) => { followed.push(a); return { metadata: {} }; } }, renamed);
  assert.equal(
    followed[0][2].spec.sourceRef.name,
    "orders-prod-2",
    "the reference sent is the name the chosen object carries NOW",
  );

  // And the body builder itself is unchanged for an existing caller that has
  // no clusters to resolve against.
  assert.equal(scheduleBody(values).spec.sourceRef.name, "orders-prod");
});

test("the_wizard_target_is_bound_by_uid_across_a_rename_and_refuses_a_recreation", async () => {
  const backups = fixture("wizard-backups.json");
  const point = { uid: recoveryPoints(backups)[0].metadata.uid, backup: recoveryPoints(backups)[0].metadata.name };
  const clusters = fixture("wizard-clusters.json");
  const targetUid = clusters.items.find((c) => c.spec.role === "target").metadata.uid;

  const state = initialState("logweir-t27", clusters, backups, point);
  assert.equal(state.targetClusterUid, targetUid, "the default preselect is an identity");
  assert.equal(resolveTarget(state).state, "selected");
  assert.deepEqual(Object.keys(validateRestore(state)).indexOf("targetCluster"), -1);

  // A RENAME mid-wizard. The same object under a new label: the selection
  // holds and the create body carries the current name.
  const renamed = JSON.parse(JSON.stringify(clusters));
  renamed.items.find((c) => c.metadata.uid === targetUid).metadata.name = "orders-recovery-2";
  const afterRename = initialState("logweir-t27", renamed, backups, point);
  afterRename.targetClusterUid = targetUid;
  afterRename.targetClusterName = "orders-recovery";
  assert.equal(resolveTarget(afterRename).state, "selected");
  assert.equal(resolveTarget(afterRename).renamedFrom, "orders-recovery");
  selectTarget(afterRename, targetUid, "orders-recovery");
  const api = { create: async (ns, plural, body) => ({ metadata: { name: body.metadata.name, uid: "r" } }), get: async () => { throw new Error("none"); } };
  const calls = [];
  await submitRestore(afterRename, {
    create: async (ns, plural, body) => { calls.push(body); return api.create(ns, plural, body); },
    get: api.get,
  });
  assert.equal(calls[0].spec.target.clusterRef.name, "orders-recovery-2");

  // A RECREATION mid-wizard. The uid is gone and the name is taken: refused,
  // no plan and no request.
  const recreated = JSON.parse(JSON.stringify(clusters));
  recreated.items.find((c) => c.metadata.uid === targetUid).metadata.uid = "uid-new";
  const afterRecreate = initialState("logweir-t27", recreated, backups, point);
  afterRecreate.targetClusterUid = targetUid;
  afterRecreate.targetClusterName = "orders-recovery";
  assert.equal(resolveTarget(afterRecreate).state, "recreated");
  const problems = validateRestore(afterRecreate);
  assert.match(problems.targetCluster, /different object now answers to the name/);
  const silent = [];
  await assert.rejects(
    () => submitRestore(afterRecreate, { create: async (...a) => { silent.push(a); } }),
    (error) => {
      assert.equal(error.kind, "invalid");
      assert.match(error.fields.targetCluster, /uid-new/);
      return true;
    },
  );
  assert.equal(silent.length, 0, "nothing is sent for a target nobody chose");
  const step = renderTargetStep(afterRecreate);
  assert.match(step, /id="target-cluster-refusal"/, "and the step renders the refusal: " + step);

  // THE MUTANT this row kills: `resolveTarget` falling back to `firstTarget`
  // (or to a name match) when the uid does not answer, which is the "recreated
  // cluster silently re-selected" mutant. Under it the refusal disappears and
  // `submitRestore` sends a restore into an object nobody picked.
});

test("a_wizard_draft_carries_the_identity_and_an_older_draft_still_applies", () => {
  const backups = fixture("wizard-backups.json");
  const point = { uid: recoveryPoints(backups)[0].metadata.uid, backup: "" };
  const clusters = fixture("wizard-clusters.json");
  const sourceUid = clusters.items.find((c) => c.spec.role === "source").metadata.uid;

  const state = initialState("logweir-t27", clusters, backups, point);
  selectTarget(state, sourceUid, "orders-prod");
  const draft = wizardDraftValues(state);
  assert.equal(draft.targetClusterUid, sourceUid);
  assert.equal(draft.targetCluster, "orders-prod");

  const fresh = initialState("logweir-t27", clusters, backups, point);
  assert.equal(applyWizardDraft(fresh, draft), true);
  assert.equal(fresh.targetClusterUid, sourceUid, "the identity is restored, not the label");

  // A DRAFT FROM BEFORE PLAT-07.2 has a name and no uid: it resolves by name
  // and is pinned.
  const older = Object.assign({}, draft);
  delete older.targetClusterUid;
  const legacyApply = initialState("logweir-t27", clusters, backups, point);
  assert.equal(applyWizardDraft(legacyApply, older), true);
  assert.equal(legacyApply.targetClusterUid, sourceUid, "and it becomes an identity");

  // A DRAFT WHOSE TARGET WAS RECREATED is applied and then REFUSED, rather
  // than dropped: dropping it would restore the default target silently.
  const recreated = JSON.parse(JSON.stringify(clusters));
  recreated.items.find((c) => c.metadata.uid === sourceUid).metadata.uid = "uid-new";
  const refusing = initialState("logweir-t27", recreated, backups, point);
  assert.equal(applyWizardDraft(refusing, draft), true);
  assert.equal(resolveTarget(refusing).state, "recreated");
  assert.ok(validateRestore(refusing).targetCluster);
});

test("namespace_navigation_clears_the_selection", () => {
  // PLAT-13.1's rule, stated for this selector: a draft belongs to its
  // namespace. The key is `{ns, form}`, so nothing chosen in one namespace is
  // visible in another -- and the route the namespace picker writes
  // (`#/<route>?ns=<name>`) carries no identity, so a deep link into the
  // wizard does not survive a namespace change either.
  const a = formKey("team-a", "schedule-form");
  const b = formKey("team-b", "schedule-form");
  keepDraft(a, { source: "orders-prod", sourceUid: "uid-a" }, SCHEDULE_DRAFT_FIELDS);
  assert.equal(readDraft(a).sourceUid, "uid-a");
  assert.equal(readDraft(b), null, "the other namespace has no selection at all");

  // And the form rendered for the other namespace preselects the DEFAULT, with
  // no refusal and no memory of the first namespace's choice.
  const clusters = { items: [cluster({ uid: "uid-z", name: "other-ns-cluster" })] };
  const rendered = renderScheduleForm({ draft: readDraft(b), clusters: clusters, now: NOW });
  assert.equal(rendered.indexOf("uid-a"), -1, "no trace of the other namespace's selection");
  assert.ok(rendered.includes("value=\"uid-z\" selected"));
});

test("a_delayed_refresh_after_the_route_left_paints_nothing", async () => {
  // The tracker's "delayed refresh". `mountClusterDetail`'s Test connection
  // control is a READ, and a read that answers after the view is gone is
  // dropped -- the PLAT-13.1 rule, asserted for the one control PLAT-07.2
  // adds.
  const routes = createRouteLifecycle();
  const view = routes.begin();
  let resolveRead;
  const slow = new Promise((done) => { resolveRead = done; });
  const painted = [];
  const node = {
    children: [],
    appendChild(child) { this.children.push(child); return child; },
    removeChild() { return this.children.shift(); },
    get firstChild() { return this.children.length === 0 ? null : this.children[0]; },
    querySelector: () => null,
    querySelectorAll: () => [],
  };
  const api = { get: async () => slow };
  const mounted = mountClusterDetail(node, "team-a", "orders-prod", (html) => {
    painted.push(html);
    return [];
  }, view, api);
  routes.begin();
  resolveRead(cluster({}));
  await mounted;
  assert.deepEqual(painted, [], "an answer for a view that has left renders nothing");
});

test("a_credential_rotation_moves_the_observation_and_not_the_reference", () => {
  // THE TRACKER'S "credential rotation". Rotating the password means writing
  // the Secret; the `KafkaCluster` is untouched (`spec` is immutable and the
  // controller resolves the reference at run time), so what the page must show
  // changing is the PROBE and nothing else.
  const before = cluster({
    auth: { mode: "scramSha512", username: "u", tls: true, secretRef: { name: "s", passwordKey: "k" } },
    status: { reachable: false, observedAt: "2026-09-16T11:00:00Z", reason: "ProbeReportedUnreachable" },
  });
  const after = JSON.parse(JSON.stringify(before));
  after.status = { reachable: true, clusterId: "CID", observedAt: "2026-09-16T12:00:30Z", reason: "Reachable" };

  assert.equal(probeState(before, NOW).verdict, "unreachable");
  assert.equal(probeState(before, NOW).stale, true, "an hour-old failure is stale as well as failed");
  assert.equal(probeState(after, NOW).verdict, "reachable");
  assert.equal(probeState(after, NOW).stale, false);
  assert.equal(
    authCell(before.spec),
    authCell(after.spec),
    "and the reference the page shows is unchanged: a rotation is a Secret edit, not a spec edit",
  );
  assert.notEqual(probeCell(before, NOW), probeCell(after, NOW), "the cell a reader looks at moved");
});

test("a_role_change_keeps_the_selection_and_updates_what_the_option_says", () => {
  const before = [cluster({ uid: "uid-a", name: "orders-prod", role: "source" })];
  const after = [cluster({ uid: "uid-a", name: "orders-prod", role: "target" })];
  const selection = { uid: "uid-a", name: "orders-prod" };
  assert.equal(resolveClusterSelection(before, selection).role, "source");
  assert.equal(resolveClusterSelection(after, selection).state, "selected", "the identity is unchanged");
  assert.equal(resolveClusterSelection(after, selection).role, "target");
  assert.match(optionCaption(before[0], probeState(before[0], NOW)), /\(role: source\)/);
  assert.match(optionCaption(after[0], probeState(after[0], NOW)), /\(role: target\)/);
  assert.match(optionCaption(cluster({ role: "" }), probeState(cluster({}), NOW)), /\(role: unset\)/);
  const rendered = renderClusterSelector({ id: "s", name: "s", clusters: after, selection: selection, now: NOW });
  assert.ok(rendered.includes("data-role=\"target\""));
  assert.ok(rendered.includes("value=\"uid-a\" selected"));
});

// ===========================================================================
// 5. THE SEARCH, and the DOM half.
// ===========================================================================

test("the_search_filters_options_and_never_changes_what_would_be_submitted", () => {
  const nodes = selectNode([
    { uid: "uid-a", name: "orders-prod", search: "orders-prod source tls" },
    { uid: "uid-b", name: "scratch", search: "scratch target plaintext" },
  ], 0);
  assert.equal(filterSelectorOptions(nodes.select, "scratch"), 2,
    "the SELECTED option is never hidden, even when it does not match");
  assert.equal(nodes.select.options[0].hidden, false, "because hiding it would change the answer");
  assert.equal(nodes.select.options[1].hidden, false);
  assert.equal(filterSelectorOptions(nodes.select, "target"), 2);
  assert.equal(filterSelectorOptions(nodes.select, "nothing-matches"), 1, "only the selected one");
  assert.equal(nodes.select.options[1].hidden, true);
  assert.equal(filterSelectorOptions(nodes.select, ""), 2, "an empty query shows everything");
  assert.equal(filterSelectorOptions(null, "x"), 0, "and no select is no rows, not a throw");

  // EVERY WORD MUST MATCH, as the recovery-point selector already required.
  assert.equal(haystackMatches("orders-prod source tls", "orders tls"), true);
  assert.equal(haystackMatches("orders-prod source tls", "orders nope"), false);
  assert.equal(haystackMatches("orders-prod", ""), true);

  // The haystack carries the NAME of the credential Secret and never a value,
  // because there is none on the object to carry.
  const hay = clusterHaystack(cluster({
    auth: { mode: "scramSha512", username: "logweir-reader", tls: true, secretRef: { name: "orders-scram" } },
  }));
  assert.ok(hay.includes("orders-scram") && hay.includes("logweir-reader") && hay.includes("source"));
});

test("reading_a_selection_back_gives_the_uid_and_the_name_the_option_carried", () => {
  const nodes = selectNode([
    { uid: "uid-a", name: "orders-prod" },
    { uid: "uid-b", name: "scratch" },
  ], 1);
  assert.deepEqual(readClusterSelection(nodes.form, "s"), { uid: "uid-b", name: "scratch" });
  // A form rendered with no saved cluster has no select; the hidden inputs are
  // what answer, so a draft is still readable.
  const empty = { querySelector: (sel) => sel === "#s-uid" ? { value: "u" } : (sel === "#s-name" ? { value: "n" } : null) };
  assert.deepEqual(readClusterSelection(empty, "s"), { uid: "u", name: "n" });
});

// ===========================================================================
// 6. BOTH CLIENT MODES.
// ===========================================================================

test("both_client_modes_produce_the_same_selection_and_the_same_probe_words", async () => {
  // The legacy mode reads a `KafkaCluster`; the console mode reads a
  // `Connection` and `ui/client.js` projects it. The selector must say the
  // same thing about both, or the rule is a rule of one mode.
  const originalFetch = globalThis.fetch;
  resetMode();
  const record = await selectMode({
    probe: async () => ({ ok: true, status: 200, body: fixture("console/session.json") }),
  });
  assert.equal(record.mode, CONSOLE);
  globalThis.fetch = (u) => Promise.resolve({
    ok: true,
    status: 200,
    text: () => Promise.resolve(JSON.stringify(fixture("console/connections-list.json"))),
  });
  let projected;
  try {
    projected = await apiClient().list("team-a", "kafkaclusters");
  } finally {
    globalThis.fetch = originalFetch;
    resetMode();
  }
  const consoleAt = Date.parse("2026-09-12T09:15:00Z");
  const items = savedClusters(projected);
  assert.deepEqual(items.map((c) => c.metadata.uid), [
    "1b2c3d4e-5f60-4718-8293-a4b5c6d7e8f9",
    "2b2c3d4e-5f60-4718-8293-a4b5c6d7e8fa",
  ]);
  assert.equal(probeState(items[0], consoleAt).verdict, "reachable");
  assert.equal(probeState(items[0], consoleAt).stale, false);
  assert.equal(probeState(items[1], consoleAt).verdict, "never", "`unknown` with no reason is never probed");

  // THE SAME OBJECT AS A RAW CUSTOM RESOURCE, through the legacy mode's shape.
  const asCr = cluster({
    uid: "1b2c3d4e-5f60-4718-8293-a4b5c6d7e8f9",
    name: "orders-prod",
    servers: ["kafka-0.orders.svc:9093", "kafka-1.orders.svc:9093"],
    auth: { mode: "scramSha512", username: "logweir-reader", tls: true, secretRef: { name: "orders-scram" } },
    status: { reachable: true, clusterId: "MkU3NEVCTTlSM0FCQVRMQQ", observedAt: "2026-09-12T09:14:31Z", reason: "Reachable" },
  });
  assert.deepEqual(probeState(asCr, consoleAt), probeState(items[0], consoleAt));
  assert.equal(optionCaption(asCr, probeState(asCr, consoleAt)), optionCaption(items[0], probeState(items[0], consoleAt)));
  const both = [
    renderClusterSelector({ id: "s", name: "source", clusters: [asCr], selection: { uid: asCr.metadata.uid, name: "orders-prod" }, now: consoleAt }),
    renderClusterSelector({ id: "s", name: "source", clusters: [items[0]], selection: { uid: asCr.metadata.uid, name: "orders-prod" }, now: consoleAt }),
  ];
  assert.equal(both[0], both[1], "the two modes render the same selector for the same connection");

  // AND THE STALE CASE AGREES TOO, which is the one that matters: a console
  // projection that lost `observedAt` would render as fresh.
  const late = Date.parse("2026-09-12T09:40:00Z");
  assert.equal(probeState(items[0], late).stale, true);
  assert.equal(probeState(asCr, late).stale, true);
});

test("console_mode_names_the_two_contract_v1_fields_it_cannot_carry_and_refuses_them", async () => {
  const originalFetch = globalThis.fetch;
  resetMode();
  await selectMode({ probe: async () => ({ ok: true, status: 200, body: fixture("console/session.json") }) });
  const sent = [];
  globalThis.fetch = (u, init) => {
    sent.push({ url: u, init: init });
    return Promise.resolve({
      ok: true,
      status: 201,
      text: () => Promise.resolve(JSON.stringify(fixture("console/connection.json"))),
    });
  };
  try {
    const api = apiClient();
    const base = {
      name: "orders-prod", servers: "kafka-0:9093", role: "source",
      mode: "scramSha512", username: "u", secret: "s", tls: true,
      passwordKey: "", tlsCaKind: "none", tlsCaName: "", tlsCaKey: "",
    };
    // A contract-v1-free connection goes through, as it did before.
    await api.create("team-a", "kafkaclusters", clusterBody(base));
    assert.equal(sent.length, 1);

    // A `passwordKey` is REFUSED BY NAME rather than dropped: dropping it
    // would create a connection that projects a different entry of the Secret.
    await assert.rejects(
      () => api.create("team-a", "kafkaclusters", clusterBody(Object.assign({}, base, { passwordKey: "sasl-pw" }))),
      (error) => {
        assert.equal(error.reason, "NoConsoleRoute");
        assert.equal(error.kind, "refused");
        assert.match(error.message, /spec\.auth\.secretRef\.passwordKey/);
        return true;
      },
    );
    // And a `tlsCa` likewise: a connection created without the CA it named
    // would dial trusting the image's own store.
    await assert.rejects(
      () => api.create("team-a", "kafkaclusters", clusterBody(Object.assign({}, base, {
        tlsCaKind: "configMap", tlsCaName: "ca", tlsCaKey: "ca.crt",
      }))),
      (error) => {
        assert.match(error.message, /spec\.auth\.tlsCa/);
        return true;
      },
    );
    assert.equal(sent.length, 1, "neither refusal reached the network");
  } finally {
    globalThis.fetch = originalFetch;
    resetMode();
  }
});

test("the_legacy_decoder_hands_back_contract_v1s_fields_untouched", async () => {
  const { decodeLegacyObject } = await import("../contract.js");
  const object = cluster({
    auth: {
      mode: "scramSha512", username: "u", tls: true,
      secretRef: { name: "s", passwordKey: "sasl-pw" },
      tlsCa: { configMapKeyRef: { name: "ca", key: "ca.crt" } },
    },
  });
  const decoded = decodeLegacyObject("kafkaclusters", object);
  assert.equal(decoded.value, object, "the legacy decoder returns the API server's own object");
  assert.equal(decoded.value.spec.auth.secretRef.passwordKey, "sasl-pw");
  assert.deepEqual(decoded.value.spec.auth.tlsCa, { configMapKeyRef: { name: "ca", key: "ca.crt" } });
  assert.deepEqual(decoded.unknown, [], "and neither reference is an unknown field");
});

// ===========================================================================
// 7. THE LIST's OWN Test connection control, and its UID check.
// ===========================================================================

test("the_clusters_list_offers_a_test_connection_per_row_and_names_the_row_by_uid", async () => {
  const clusters = { items: [cluster({ uid: "uid-a" }), cluster({ uid: "uid-b", name: "scratch" })] };
  const html = renderClusterList(clusters, "team-a", NOW);
  assert.ok(html.includes("data-cluster-uid=\"uid-a\""), "each row carries its identity: " + html);
  assert.ok(html.includes("data-probe-uid=\"uid-b\""));
  // THE LIST's CONTROL IS A RE-READ AND NOW SAYS SO. It used to be labelled
  // "Test connection", which is a sentence about a broker; it reads a
  // `KafkaCluster` and shows what the controller recorded. The control that
  // really dials is the cluster page's, which starts a `sourceConnection`
  // `Preflight` -- and two controls under one label would have been the same
  // lie in a second place.
  assert.ok(html.includes("<button type=\"submit\">Re-read probe</button>"));
  assert.ok(!html.includes(">Test connection</button>"), "the label moved to the dial");
  assert.match(html, /It dials nothing/, "and says what the control actually does");
  assert.match(html, /The control that really dials is Test connection/);

  // AND THE MOUNT HALF WIRES ONE PER ROW, against the list it just read.
  const wired = [];
  const node = {
    children: [],
    appendChild(c) { this.children.push(c); return c; },
    removeChild() { return this.children.shift(); },
    get firstChild() { return this.children.length === 0 ? null : this.children[0]; },
    querySelector: () => null,
    querySelectorAll: (selector) => {
      if (selector !== "form.probe-test") {
        return [];
      }
      return ["uid-a", "uid-b"].map((uid) => ({
        getAttribute: (name) => (name === "probe-uid" || name === "data-probe-uid" ? uid : "orders-prod"),
        querySelector: () => null,
        addEventListener: (type) => { wired.push(uid + ":" + type); },
      }));
    },
  };
  const routes = createRouteLifecycle();
  await mountClusters(node, "team-a", () => [], routes.begin(), {
    list: async () => clusters,
  });
  assert.deepEqual(wired, ["uid-a:submit", "uid-b:submit"], "one control per row, and no more");
});

// ===========================================================================
// 8. THE WINDOW BETWEEN OPENING A FORM AND SUBMITTING IT.
// ===========================================================================

test("both_submits_read_the_connections_again_and_refuse_what_changed_under_them", async () => {
  // Resolving against the list the view read at mount only catches a
  // connection that was already gone when the form opened. The window that
  // matters is the one between opening the form and submitting it -- which is
  // exactly when somebody rebuilds a cluster -- so both submits read again.
  const values = {
    name: "hourly", cron: "0 * * * *", source: "orders-prod", sourceUid: "uid-a",
    topics: "orders", archive: "s3://b/p", archiveSecret: "logweir-s3",
    keepLast: "", keepDays: "",
  };
  const created = [];
  const fresh = { items: [cluster({ uid: "uid-a", name: "orders-prod" })] };
  await confirmThenCreate("team-a", values, {
    list: async () => fresh,
    create: async (...a) => { created.push(a); return { metadata: { name: "hourly" } }; },
  });
  assert.equal(created.length, 1);

  // The SAME draft, submitted after the cluster was recreated under its name:
  // the re-read finds the impostor and the create never happens.
  const none = [];
  await assert.rejects(
    () => confirmThenCreate("team-a", values, {
      list: async () => ({ items: [cluster({ uid: "uid-b", name: "orders-prod" })] }),
      create: async (...a) => { none.push(a); },
    }),
    (error) => {
      assert.match(error.fields.source, /uid-b/);
      return true;
    },
  );
  assert.equal(none.length, 0);

  // AND A FAILED RE-READ IS A REFUSAL, not a shrug: "I could not find out" is
  // not "it is still there", and the draft is kept either way.
  await assert.rejects(
    () => confirmThenCreate("team-a", values, {
      list: async () => { throw new Error("connection reset"); },
      create: async (...a) => { none.push(a); },
    }),
    (error) => {
      assert.match(error.fields.source, /could not be read again/);
      assert.match(error.fields.source, /connection reset/);
      return true;
    },
  );
  assert.equal(none.length, 0, "nothing is created when the confirmation failed");

  // THE WIZARD'S HALF puts the fresh list on the state and touches no plan
  // field, so the reviewed hash still compares the bytes that were on screen.
  const backups = fixture("wizard-backups.json");
  const point = { uid: recoveryPoints(backups)[0].metadata.uid, backup: "" };
  const state = initialState("logweir-t27", fixture("wizard-clusters.json"), backups, point);
  const before = JSON.stringify(state.fields);
  const replacement = { items: [cluster({ uid: "uid-z", name: "elsewhere" })] };
  await confirmClusters(state, { list: async () => replacement });
  assert.equal(state.clusters, replacement, "the state now resolves against what exists now");
  assert.equal(JSON.stringify(state.fields), before, "and no field of the plan moved");
  assert.equal(resolveTarget(state).state, "missing");
  assert.ok(validateRestore(state).targetCluster, "so the submit that follows refuses");

  await assert.rejects(
    () => confirmClusters(state, { list: async () => { throw new Error("gateway timeout"); } }),
    (error) => {
      assert.match(error.fields.targetCluster, /could not be read again/);
      return true;
    },
  );
});

// ===========================================================================
// 9. A REFUSAL SELECTS NOTHING (review finding F1).
// ===========================================================================

test("a_refused_selector_selects_nothing_and_a_further_create_click_sends_nothing", async () => {
  // THE DEFECT THIS ROW IS ABOUT. In the two refusal states the selector
  // marked no option `selected` while filling the hidden inputs from
  // `preferredCluster` -- which can be a THIRD cluster. A browser defaults an
  // unselected `<select>` to its first option, so one more click on Create,
  // without touching anything, submitted against whatever sorted first. A form
  // that has just said "the connection you chose is gone" must not be one click
  // from creating against a connection nobody chose.
  const clusters = [
    cluster({ uid: "uid-A", name: "aaa-target", role: "target" }),
    cluster({ uid: "uid-B", name: "zzz-source", role: "source" }),
  ];
  for (const [label, selection, expectName] of [
    ["recreated", { uid: "uid-gone", name: "aaa-target" }, "aaa-target"],
    ["missing", { uid: "uid-gone", name: "nowhere-at-all" }, "nowhere-at-all"],
  ]) {
    const html = renderClusterSelector({
      id: "schedule-source", name: "source", clusters: clusters, selection: selection,
      prefer: "source", now: NOW,
    });
    const resolved = resolveClusterSelection(clusters, selection);
    assert.equal(resolved.state, label);
    assert.ok(html.includes(EMPTY_OPTION), label + ": the empty option is rendered selected: " + html);
    // COUNTED OVER `<option>` TAGS, not over the word: the surrounding prose
    // says "selected" three times and a bare count would pass on anything.
    const marked = html.match(/<option [^>]*\sselected[^>]*>/g) || [];
    assert.equal(marked.length, 1, label + ": exactly one option is marked selected: " + html);
    assert.ok(
      marked[0].includes("value=\"\""),
      label + ": and it is the empty one: " + marked[0],
    );
    assert.equal(
      html.indexOf("value=\"uid-A\" selected"),
      -1,
      label + ": the first option by name is NOT preselected under a refusal",
    );
    assert.equal(html.indexOf("value=\"uid-B\" selected"), -1, label + ": nor the preferred role");
    // THE HIDDEN PAIR IS THE REFUSED PAIR -- what the refusal is about -- and
    // never a third cluster's identity.
    assert.ok(
      html.includes("id=\"schedule-source-uid\" name=\"sourceUid\" value=\"uid-gone\""),
      label + ": the hidden uid is the refused one: " + html,
    );
    assert.ok(
      html.includes("id=\"schedule-source-name\" name=\"sourceName\" value=\"" + expectName + "\""),
      label + ": and the hidden name is the refused one: " + html,
    );
    assert.equal(html.indexOf("uid-B\">"), -1, label + ": the fallback's uid is nowhere in a hidden input");
    assert.ok(
      html.includes("id=\"schedule-source-probe\">no saved connection is selected."),
      label + ": and no third cluster's probe is presented as the selection's: " + html,
    );
  }

  // THE EMPTY VALUE IS WHAT THE SELECT READS BACK, and both forms refuse it.
  const empty = { querySelector: () => null };
  assert.deepEqual(readClusterSelection(empty, "schedule-source"), { uid: "", name: "" });
  const refusedValues = {
    name: "hourly", cron: "0 * * * *", source: "", sourceUid: "",
    topics: "orders", archive: "s3://b/p", archiveSecret: "logweir-s3",
    keepLast: "", keepDays: "",
  };
  assert.match(
    validateSchedule(refusedValues).source,
    /nothing is selected/,
    "the schedule form refuses the empty selection by name",
  );
  const sent = [];
  await assert.rejects(
    () => confirmThenCreate("team-a", refusedValues, {
      list: async () => ({ items: clusters }),
      create: async (...a) => { sent.push(a); },
    }),
    (error) => {
      assert.equal(error.kind, "invalid");
      assert.match(error.fields.source, /nothing is selected/);
      return true;
    },
  );
  assert.equal(sent.length, 0, "a further Create click under a standing refusal sends NOTHING");

  // AND THE SAME ON THE WIZARD'S SIDE. Picking the empty option clears the
  // NAME as well as the uid: leaving the refused name behind would let the
  // selection resolve BY NAME onto the very object the refusal is about.
  const backups = fixture("wizard-backups.json");
  const point = { uid: recoveryPoints(backups)[0].metadata.uid, backup: "" };
  const wizard = initialState("logweir-t27", { items: clusters }, backups, point);
  wizard.targetClusterUid = "uid-gone";
  wizard.targetClusterName = "aaa-target";
  assert.equal(resolveTarget(wizard).state, "recreated");
  const step = renderTargetStep(wizard);
  assert.ok(step.includes(EMPTY_OPTION), "step 4's selector opens on the empty option: " + step);
  selectTarget(wizard, "", "");
  assert.equal(wizard.targetClusterUid, "");
  assert.equal(wizard.targetClusterName, "", "the refused NAME is cleared with the uid");
  assert.equal(resolveTarget(wizard).state, "none", "so nothing resolves by name onto the impostor");
  assert.match(validateRestore(wizard).targetCluster, /nothing is selected/);
  const nothing = [];
  await assert.rejects(
    () => submitRestore(wizard, { create: async (...a) => { nothing.push(a); } }),
    (error) => error.kind === "invalid",
  );
  assert.equal(nothing.length, 0, "and the wizard sends nothing either");
});

test("the_re_read_before_a_restore_submit_follows_a_rename_into_the_create_body", async () => {
  // REVIEW FINDING F4. `restoreBody` spells `target.clusterRef.name` from the
  // state, and `confirmClusters` used to refresh only `state.clusters` -- so a
  // connection renamed between opening the wizard and submitting it was sent
  // under the name it no longer has. This row NEVER CALLS `selectTarget`,
  // because the submit path does not: the guard has to hold on the product's
  // own sequence.
  const backups = fixture("wizard-backups.json");
  const point = { uid: recoveryPoints(backups)[0].metadata.uid, backup: "" };
  const before = { items: [cluster({ uid: "uid-t", name: "old-target", role: "target" })] };
  const after = { items: [cluster({ uid: "uid-t", name: "new-target", role: "target" })] };
  const state = initialState("logweir-t27", before, backups, point);
  assert.equal(state.targetClusterName, "old-target");

  const fields = JSON.stringify(state.fields);
  await confirmClusters(state, { list: async () => after });
  assert.equal(state.targetClusterName, "new-target", "the label follows the identity");
  assert.equal(state.targetClusterUid, "uid-t", "and the identity did not move");
  assert.equal(JSON.stringify(state.fields), fields, "no field of the plan moved");

  const bodies = [];
  await submitRestore(state, {
    create: async (ns, plural, body) => { bodies.push(body); return { metadata: { name: body.metadata.name, uid: "r" } }; },
    get: async () => { throw new Error("no approval"); },
  });
  assert.equal(
    bodies[0].spec.target.clusterRef.name,
    "new-target",
    "the create body spells the name the chosen object carries NOW",
  );
});
