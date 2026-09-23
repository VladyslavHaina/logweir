// datagrid.spec.js -- PLAT-18.2: the datagrid's arithmetic, the lists that
// declare one, and focus surviving a re-render.
//
// WHAT IS HERE AND WHAT IS NOT. `render.js`'s datagrid is two halves: a pure
// function from rows and a state to the rows one page shows
// (`datagridView`), and the DOM enhancement that puts Clarity's filter, sort
// buttons and pager around a parsed table (`enhanceDatagrids`). Node has no
// DOM, so the first half, the strings every list page emits, and the focus
// bookkeeping of `replace` (driven through a small fake document) are
// asserted here; the enhancement itself is driven in Chromium by
// `scripts/plat18-2-ui-e2e.mjs`, keyboard only, against the live API.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  enhanceDatagrids,
  DATAGRID_DEFAULT_PAGE_SIZE,
  DATAGRID_PAGE_SIZES,
  datagridCompare,
  datagridState,
  datagridSummary,
  datagridTerms,
  datagridView,
  focusWithin,
  replace,
  restoreFocus,
  table,
} from "../render.js";
import { build, fakeDocument as fakeDom } from "./fake-dom.js";
import { renderBackupList } from "../pages/backups.js";
import { renderHistoryList } from "../pages/history.js";
import { renderClusterList } from "../pages/clusters.js";
import { renderScheduleList } from "../pages/schedules.js";
import { renderApprovalList } from "../pages/approvals.js";
import {
  initialState,
  recoveryPoints,
  renderTopicSubset,
  selectedTopics,
} from "../pages/restore-wizard.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));

function fixture(name) {
  return JSON.parse(readFileSync(FIXTURES + name, "utf8"));
}

function rows(texts) {
  return texts.map((cells) => ({ text: cells.join(" "), cells: cells }));
}

// ------------------------------------------------------------ the arithmetic

test("a_filter_keeps_rows_holding_every_term_case_insensitively", () => {
  assert.deepEqual(datagridTerms("  Orders   DLQ "), ["orders", "dlq"]);
  const list = rows([["orders-hourly", "Succeeded"], ["orders-dlq", "Failed"], ["payments", "Succeeded"]]);
  const view = datagridView(list, { query: "ORDERS succeeded" });
  assert.deepEqual(view.indices, [0], "both terms, in any case, in any column");
  assert.equal(view.matched, 1);
  assert.equal(view.total, 3);
  assert.equal(view.filtered, true);
  // Negative control: a term no row holds matches nothing, and says so.
  const none = datagridView(list, { query: "restores" });
  assert.deepEqual(none.indices, []);
  assert.equal(none.first, 0);
  assert.equal(datagridSummary(none, "backups"), "No backups match the filter (3 in all).");
});

test("sorting_is_numeric_for_numbers_textual_otherwise_stable_and_puts_absence_last", () => {
  const list = rows([
    ["b", "10", "2026-09-02T00:00:00Z"],
    ["a", "9", "-"],
    ["c", "-", "2026-09-01T00:00:00Z"],
    ["a", "100", "2026-09-03T00:00:00Z"],
  ]);
  const byNumber = (dir) => datagridView(list, { sortColumn: 1, sortDirection: dir }).indices;
  assert.deepEqual(byNumber("ascending"), [1, 0, 3, 2], "9 < 10 < 100, and `-` last");
  assert.deepEqual(byNumber("descending"), [3, 0, 1, 2], "100 > 10 > 9, and `-` STILL last");
  assert.deepEqual(
    datagridView(list, { sortColumn: 2 }).indices,
    [2, 0, 3, 1],
    "an RFC 3339 instant sorts as text, and the absent one is last",
  );
  assert.deepEqual(
    datagridView(list, { sortColumn: 0 }).indices,
    [1, 3, 0, 2],
    "equal keys keep the page's own order (stable)",
  );
  assert.deepEqual(datagridView(list, { sortColumn: -1 }).indices, [0, 1, 2, 3], "-1 is the page order");
  assert.ok(datagridCompare("9", "10") < 0, "numbers compare as numbers");
  assert.ok(datagridCompare("b", "a") > 0, "text compares as text");
});

test("pagination_uses_clarity_page_sizes_and_clamps_a_page_that_no_longer_exists", () => {
  assert.deepEqual(DATAGRID_PAGE_SIZES, [10, 20, 50, 100]);
  assert.equal(DATAGRID_DEFAULT_PAGE_SIZE, 20);
  const list = rows(Array.from({ length: 1000 }, (_, i) => ["row-" + String(i)]));
  const first = datagridView(list, {});
  assert.equal(first.indices.length, 20);
  assert.equal(first.pages, 50);
  assert.equal(datagridSummary(first, "runs"), "1-20 of 1000 runs.");
  const page3 = datagridView(list, { page: 3, pageSize: 50 });
  assert.deepEqual([page3.first, page3.last], [101, 150]);
  assert.equal(page3.indices[0], 100);
  const beyond = datagridView(list, { page: 999, pageSize: 100 });
  assert.equal(beyond.page, 10, "a page past the end is the last page, never an empty screen");
  assert.equal(beyond.indices.length, 100);
  const odd = datagridView(list, { pageSize: 37 });
  assert.equal(odd.pageSize, 20, "a size Clarity does not offer falls back to the default");
  const filtered = datagridView(list, { query: "row-99", page: 1 });
  assert.equal(filtered.matched, 11, "row-99 and row-990..row-999");
  assert.equal(datagridSummary(filtered, "runs"), "1-11 of 11 runs matching the filter (1000 in all).");
  assert.equal(datagridSummary(datagridView([], {}), "runs"), "No runs.");
});

test("a_grid_state_is_kept_per_id_for_the_life_of_the_page", () => {
  const one = datagridState("spec-grid-a");
  one.query = "orders";
  one.page = 4;
  assert.equal(datagridState("spec-grid-a"), one, "the same id is the same state object");
  assert.equal(datagridState("spec-grid-a").query, "orders");
  assert.notEqual(datagridState("spec-grid-b"), one, "another id is another state");
  assert.equal(datagridState("spec-grid-b").query, "");
});

// --------------------------------------------------------------- the strings

test("a_declared_datagrid_gains_one_wrapper_and_an_undeclared_table_is_unchanged", () => {
  const plain = table(["A", "B"], [["1", "2"]], "none");
  assert.equal(
    plain,
    "<div class=\"table-wrap\"><table class=\"grid\"><thead><tr><th scope=\"col\">A</th>" +
      "<th scope=\"col\">B</th></tr></thead><tbody><tr><td>1</td><td>2</td></tr></tbody></table></div>",
    "no grid argument: the exact bytes every table had before PLAT-18.2",
  );
  const grid = table(["A", "B"], [["1", "2"]], "none", undefined, { id: "things", label: "things" });
  assert.equal(
    grid,
    "<div class=\"datagrid\" data-datagrid=\"things\" data-datagrid-label=\"things\">" + plain + "</div>",
    "a grid argument: the same table inside one wrapper, and nothing else changed",
  );
  const hostile = table(["A"], [], "none", undefined, { id: "x\"><script>", label: "<b>" });
  assert.ok(!hostile.includes("<script>") && !hostile.includes("<b>"), "the id and label are escaped");
});

test("every_dense_list_declares_itself_a_datagrid", () => {
  const backups = { items: [fixture("backup-valid-exit0.json"), fixture("backup-valid-exit2.json")] };
  const lists = [
    ["backups", "backups", "ns", renderBackupList(backups, "ns")],
    ["history", "runs", "ns", renderHistoryList(fixture("restore-valid-pass.json"), backups, "ns")],
    ["clusters", "connections", null, renderClusterList(fixture("cluster-scram.json"))],
    ["schedules", "schedules", null, renderScheduleList(fixture("schedule-retention.json"))],
    ["approvals", "approvals", null, renderApprovalList(fixture("approvals-selfattested.json"), 0)],
  ];
  for (const [id, label, scope, html] of lists) {
    assert.ok(
      html.includes("<div class=\"datagrid\" data-datagrid=\"" + id + "\" data-datagrid-label=\"" +
        label + "\"" + (scope === null ? "" : " data-datagrid-scope=\"" + scope + "\"") +
        "><div class=\"table-wrap\">"),
      id + " declares the datagrid `" + id + "` over its table" +
        (scope === null ? "" : ", scoped to its namespace") + ":\n" + html.slice(0, 400),
    );
  }
});

test("the_wizard_topic_subset_is_a_list_datagrid_and_keeps_every_box", () => {
  const point = fixture("wizard-backups.json");
  const topics = Array.from({ length: 2000 }, (_, i) => "orders.stream-" + String(i).padStart(5, "0"));
  point.items[0].spec.topics = topics;
  const newest = recoveryPoints(point)[0];
  const state = initialState("ns", fixture("wizard-clusters.json"), point,
    { uid: newest.metadata.uid, backup: newest.metadata.name });
  const html = renderTopicSubset(state);
  assert.ok(
    html.includes("<div class=\"datagrid\" data-datagrid=\"subset-topics\" " +
      "data-datagrid-label=\"topics\" data-datagrid-scope=\"" + newest.metadata.uid +
      "\"><ul class=\"topic-subset\">"),
    "the subset list is wrapped as a list datagrid scoped to the point's uid",
  );
  assert.equal((html.match(/class="topic-box"/g) || []).length, 2000,
    "every frozen topic still has its box in the string: pagination hides, it never drops");
  assert.equal(selectedTopics(state).length, 2000, "and the selection is the whole frozen list");
  // The mapping table beneath is a datagrid too.
  assert.ok(html.includes("data-datagrid=\"topic-mapping\""), "the mapping table is a datagrid");
});

// ------------------------------------------------------ focus across replace

/** A document just large enough for `replace`'s focus bookkeeping. */
function fakeDocument() {
  const doc = { activeElement: null, body: null, root: null };
  class Node {
    constructor(id, tabindex) {
      this.ownerDocument = doc;
      this.id = id || null;
      this.tabindex = tabindex === true;
      this.children = [];
      this.parent = null;
      this.selectionStart = undefined;
      this.selectionEnd = undefined;
    }
    get firstChild() {
      return this.children[0] || null;
    }
    removeChild(child) {
      this.children.splice(this.children.indexOf(child), 1);
      child.parent = null;
      return child;
    }
    appendChild(child) {
      this.children.push(child);
      child.parent = this;
      return child;
    }
    contains(other) {
      for (let at = other; at; at = at.parent) {
        if (at === this) {
          return true;
        }
      }
      return false;
    }
    getAttribute(name) {
      return name === "id" ? this.id : null;
    }
    hasAttribute(name) {
      return name === "tabindex" && this.tabindex;
    }
    focus() {
      doc.activeElement = this;
    }
    setSelectionRange(start, end) {
      this.selectionStart = start;
      this.selectionEnd = end;
    }
  }
  doc.Node = Node;
  doc.body = new Node("body");
  doc.getElementById = (id) => {
    const walk = (n) => {
      if (n.id === id) {
        return n;
      }
      for (const c of n.children) {
        const found = walk(c);
        if (found !== null) {
          return found;
        }
      }
      return null;
    };
    return walk(doc.body);
  };
  return doc;
}

test("replace_returns_focus_to_the_control_with_the_same_id_after_a_re_render", () => {
  const doc = fakeDocument();
  const view = doc.body.appendChild(new doc.Node("view-slot", true));
  const before = view.appendChild(new doc.Node("schedule-suspend-toggle"));
  view.appendChild(new doc.Node("other"));
  const input = view.appendChild(new doc.Node("topic-q"));
  input.selectionStart = 3;
  input.selectionEnd = 5;

  // The control a keyboard reader used, then the detail re-rendered.
  before.focus();
  const after = new doc.Node("schedule-suspend-toggle");
  replace(view, [new doc.Node("heading"), after]);
  assert.equal(doc.activeElement, after, "focus is on the NEW control with the same id");
  assert.notEqual(doc.activeElement, before);

  // A text field keeps its caret.
  const detached = view.appendChild(input);
  detached.focus();
  const fresh = new doc.Node("topic-q");
  replace(view, [fresh]);
  assert.equal(doc.activeElement, fresh);
  assert.deepEqual([fresh.selectionStart, fresh.selectionEnd], [3, 5], "caret and selection kept");

  // The control is gone: focus goes to the view itself, never to a guess.
  replace(view, [new doc.Node("something-else")]);
  assert.equal(doc.activeElement, view, "a vanished control hands focus to the tabindex=-1 view");
});

test("replace_leaves_focus_alone_when_it_was_not_inside_the_replaced_node", () => {
  const doc = fakeDocument();
  const nav = doc.body.appendChild(new doc.Node("nav-slot"));
  const link = nav.appendChild(new doc.Node("nav-link"));
  const view = doc.body.appendChild(new doc.Node("view-slot", true));
  view.appendChild(new doc.Node("row"));
  link.focus();
  replace(view, [new doc.Node("row")]);
  assert.equal(doc.activeElement, link, "focus outside the replaced subtree is not touched");
  assert.equal(focusWithin(view), null);
  // Negative control: without the bookkeeping, the focused control is lost.
  const lost = fakeDocument();
  const slot = lost.body.appendChild(new lost.Node("view-slot", true));
  const control = slot.appendChild(new lost.Node("toggle"));
  control.focus();
  const kept = focusWithin(slot);
  while (slot.firstChild !== null) {
    slot.removeChild(slot.firstChild);
  }
  const again = slot.appendChild(new lost.Node("toggle"));
  assert.equal(lost.activeElement, control, "a bare replace leaves focus on a detached node");
  assert.equal(slot.contains(lost.activeElement), false, "which is no longer in the view at all");
  assert.equal(restoreFocus(slot, kept), true, "and restoreFocus is what brings it back");
  assert.equal(lost.activeElement, again);
});

test("replace_without_a_document_is_the_plain_replace", () => {
  // The fake nodes the page suites drive have no ownerDocument: nothing about
  // focus is attempted, and nothing throws.
  const children = [];
  const node = {
    get firstChild() {
      return children[0] || null;
    },
    removeChild(c) {
      children.splice(children.indexOf(c), 1);
    },
    appendChild(c) {
      children.push(c);
    },
  };
  const child = { tag: "p" };
  assert.equal(replace(node, [child]), node);
  assert.deepEqual(children, [child]);
  assert.equal(focusWithin(node), null);
});

test("a_success_line_never_prints_an_empty_name_or_uid", async () => {
  const { mutationStatus } = await import("../render.js");
  // The manual-backup answer carries no metadata (the PLAT-18.2 live pass saw
  // "Created Backup  (uid )."). The line now says only what it knows.
  const bare = mutationStatus({ phase: "succeeded", result: { object: {} } },
    { kind: "Backup", name: "", idempotencyKey: true }, null);
  assert.ok(bare.includes("<p>Created Backup.</p>"), bare);
  assert.ok(!bare.includes("(uid )"), "no empty uid parenthesis");
  const full = mutationStatus(
    { phase: "succeeded", result: { object: { metadata: { name: "b1", uid: "u-1" } } } },
    { kind: "Backup", name: "" }, null);
  assert.ok(full.includes("<p>Created Backup b1 (uid u-1).</p>"), "a known identity is still printed: " + full);
});

test("a_region_that_scrolls_becomes_reachable_and_one_that_fits_adds_no_tab_stop", async () => {
  const { markScrollRegions } = await import("../render.js");
  const region = (wide, preset) => {
    const attrs = Object.assign(Object.create(null), preset || {});
    return {
      scrollWidth: wide ? 900 : 300, clientWidth: 300, scrollHeight: 50, clientHeight: 50,
      attrs: attrs,
      getAttribute: (n) => (n in attrs ? attrs[n] : null),
      setAttribute: (n, v) => { attrs[n] = String(v); },
      removeAttribute: (n) => { delete attrs[n]; },
      hasAttribute: (n) => n in attrs,
      closest: () => ({ querySelector: () => ({ textContent: " Integrity " }) }),
    };
  };
  const wide = region(true);
  const fits = region(false);
  const authored = region(true, { tabindex: "-1" });
  const root = { querySelectorAll: () => [wide, fits, authored] };
  assert.equal(markScrollRegions(root), 1, "only the overflowing, unauthored region is marked");
  assert.deepEqual(
    [wide.attrs.tabindex, wide.attrs.role, wide.attrs["aria-label"]],
    ["0", "region", "Integrity (scrolls)"],
    "reachable by Tab, a region, named by its section's heading",
  );
  assert.equal(fits.attrs.tabindex, undefined, "a region that fits adds no Tab stop");
  assert.equal(authored.attrs.tabindex, "-1", "an attribute the page wrote is never overwritten");
  // It stops overflowing (a wider window): the attributes this set go away.
  wide.scrollWidth = 300;
  markScrollRegions(root);
  assert.equal(wide.attrs.tabindex, undefined);
  assert.equal(wide.attrs.role, undefined);
  assert.equal(authored.attrs.tabindex, "-1", "and the page's own is still left alone");
});

test("an_operation_announces_its_state_stage_and_reason_in_one_sentence", async () => {
  const { operationAnnouncement } = await import("../pages/operation.js");
  assert.equal(
    operationAnnouncement({ state: "running", stage: "copying", reason: "Admitted" }, "b-1"),
    "Operation b-1: running, stage copying (Admitted).",
  );
  assert.equal(operationAnnouncement({ phase: "Succeeded" }, "b-2"), "Operation b-2: Succeeded.",
    "a custom resource's phase stands in for a state");
  assert.equal(operationAnnouncement({}, "b-3"), "Operation b-3: unknown.",
    "no state is said as unknown, never as a success");
  const { announce } = await import("../render.js");
  assert.equal(announce("anything"), false, "without a document nothing is announced, and nothing throws");
});

test("a_control_disabled_by_a_pending_fieldset_hands_focus_to_its_form_status", () => {
  // Found by the PLAT-18.2 live pass: a form going pending re-renders with its
  // controls inside a disabled fieldset; the button there has `disabled ===
  // false` and still refuses focus, so focus fell to the body. It must land on
  // the form's status region, where the outcome is announced.
  const doc = { activeElement: null, body: null };
  const FOCUSABLE_TAGS = ["BUTTON", "INPUT", "SELECT", "TEXTAREA", "SUMMARY"];
  class El {
    constructor(tag, opts) {
      const o = opts || {};
      this.ownerDocument = doc;
      this.tagName = tag;
      this.id = o.id || null;
      this.cls = o.cls || "";
      this.tabindex = o.tabindex === true;
      this.fieldsetDisabled = o.fieldsetDisabled === true;
      this.disabled = false;
      this.children = [];
      this.parent = null;
    }
    get firstChild() { return this.children[0] || null; }
    removeChild(c) { this.children.splice(this.children.indexOf(c), 1); c.parent = null; return c; }
    appendChild(c) { this.children.push(c); c.parent = this; return c; }
    contains(o) { for (let a = o; a; a = a.parent) { if (a === this) { return true; } } return false; }
    getAttribute(n) { return n === "id" ? this.id : null; }
    hasAttribute(n) { return n === "tabindex" && this.tabindex; }
    walk(out) { for (const c of this.children) { out.push(c); c.walk(out); } return out; }
    matches(sel) {
      if (sel === ":disabled") { return this.fieldsetDisabled; }
      if (sel === ".form-status") { return this.cls === "form-status"; }
      if (sel === "form") { return this.tagName === "FORM"; }
      if (sel === ".form-status[tabindex]") { return this.cls === "form-status" && this.tabindex; }
      return FOCUSABLE_TAGS.indexOf(this.tagName) !== -1 || this.tabindex;
    }
    querySelectorAll(sel) { return this.walk([]).filter((e) => e.matches(sel)); }
    querySelector(sel) { return this.querySelectorAll(sel)[0] || null; }
    closest(sel) { for (let a = this; a; a = a.parent) { if (a.matches(sel)) { return a; } } return null; }
    focus() {
      // A browser ignores focus() on a control a disabled fieldset disables.
      if (!this.fieldsetDisabled) { doc.activeElement = this; }
    }
  }
  doc.body = new El("BODY", { id: "body" });
  doc.getElementById = (id) => doc.body.walk([]).find((e) => e.id === id) || null;
  const slot = doc.body.appendChild(new El("DIV", { id: "cluster-form-slot" }));
  const build = (pending) => {
    const form = new El("FORM", { id: "cluster-form" });
    form.appendChild(new El("INPUT", { id: "cluster-name", fieldsetDisabled: pending }));
    form.appendChild(new El("BUTTON", { fieldsetDisabled: pending }));
    form.appendChild(new El("DIV", { id: "cluster-form-status", cls: "form-status", tabindex: true }));
    return form;
  };
  slot.appendChild(build(false));
  slot.querySelectorAll("form")[0].children[1].focus();
  assert.equal(doc.activeElement.tagName, "BUTTON", "a keyboard reader is on Create");
  replace(slot, [build(true)]);
  assert.equal(doc.activeElement.id, "cluster-form-status",
    "the pending re-render hands focus to the form's status region, not to the body");
  // Negative control (run by hand at this commit): with `canTakeFocus` no
  // longer asking `:disabled`, restoreFocus picks the disabled button, the
  // browser ignores the focus() call, and this row fails with focus still on
  // the detached old button -- the body, in a browser.
  assert.notEqual(doc.activeElement, doc.body);
});

test("disabling_the_focused_control_in_place_moves_focus_to_the_status_first", async () => {
  const { disableKeepingFocus } = await import("../render.js");
  const doc = { activeElement: null };
  const mk = (id) => ({
    id: id, ownerDocument: doc, disabled: false, kids: [],
    contains(o) { return o === this || this.kids.indexOf(o) !== -1; },
    closest: () => null,
    focus() { doc.activeElement = this; },
  });
  const view = mk("view-slot");
  doc.getElementById = (id) => (id === "view-slot" ? view : null);
  const status = mk("discovery-status");
  const cancel = mk("discovery-cancel");
  cancel.focus();
  disableKeepingFocus(cancel, true, status);
  assert.equal(cancel.disabled, true);
  assert.equal(doc.activeElement, status, "focus moved to the status region before disabling");
  // No status given and no form: the view takes it, never the body.
  const again = mk("re-read");
  again.focus();
  disableKeepingFocus(again, true);
  assert.equal(doc.activeElement, view);
  // Re-enabling does not move focus, and a control without focus is just disabled.
  disableKeepingFocus(again, false);
  assert.equal(again.disabled, false);
  assert.equal(doc.activeElement, view);
  // Negative control: the bare assignment this replaced leaves focus on a
  // disabled control, which a browser then drops to the body.
  const bare = mk("bare");
  bare.focus();
  bare.disabled = true;
  assert.equal(doc.activeElement, bare, "the old way: focus left on the disabled control");
});

// ------------------------------------- the enhancement, in a fake document
//
// Review HIGH-1: a filter kept per grid id and applied to every list of that
// id -- with the box only rendered over ten rows -- hid all five runs of the
// next namespace, a Missing archive among them, behind a filter nobody could
// see or clear. These rows drive `enhanceDatagrids` itself.


function historyGrid(doc, count, missingAt) {
  const rows = [];
  for (let i = 0; i < count; i += 1) {
    rows.push(["tr", {},
      ["td", {}, (i < 2 ? "prod-" : "run-") + String(i)],
      ["td", {}, i === missingAt ? ["span", { class: "badge badge-danger" }, "Missing"] : "Available"],
    ]);
  }
  return build(doc, ["div", { class: "datagrid", "data-datagrid": "history",
    "data-datagrid-label": "runs" },
  ["div", { class: "table-wrap" }, ["table", { class: "grid" },
    ["thead", {}, ["tr", {}, ["th", { scope: "col" }, "NAME"], ["th", { scope: "col" }, "ARCHIVE"]]],
    ["tbody", {}, ...rows]]]]);
}

function mount(doc, grid) {
  while (doc.body.firstChild !== null) {
    doc.body.removeChild(doc.body.firstChild);
  }
  doc.body.appendChild(grid);
  enhanceDatagrids(grid);
  return grid;
}

function shownRows(grid) {
  return grid.querySelectorAll("tbody tr").filter((tr) => tr.shown() &&
    tr.querySelector("td.empty") === null);
}


test("a_filter_typed_over_one_namespace_does_not_hide_the_next_namespaces_short_list", () => {
  const doc = fakeDom("#/history?ns=team-a");
  const a = mount(doc, historyGrid(doc, 15));
  const filter = a.querySelector("#history-filter");
  assert.ok(filter !== null, "15 rows render the filter box");
  filter.value = "prod";
  filter.dispatch("input");
  assert.equal(shownRows(a).length, 2, "the filter narrows team-a to its two prod runs");

  // The next namespace: 5 runs, one of them a Missing archive.
  doc.defaultView.location.hash = "#/history?ns=team-b";
  const b = mount(doc, historyGrid(doc, 5, 3));
  assert.equal(shownRows(b).length, 5, "every run of team-b is shown");
  assert.ok(b.querySelector(".badge-danger").shown(), "including the Missing archive");
  assert.equal(b.querySelector("#history-filter"), null, "and a short list with no filter has no box");
  assert.equal(b.querySelector("#history-count").textContent, "1-5 of 5 runs.");
});

test("a_filter_in_force_on_a_short_list_always_renders_its_box_and_a_clear_control", () => {
  // NEGATIVE CONTROL for the row above: the SAME route and list, shrunk under
  // a live filter (a re-render after runs were deleted). The filter still
  // applies -- and so the box, the hidden count and Clear must be on screen.
  const doc = fakeDom("#/history?ns=team-a");
  const a = mount(doc, historyGrid(doc, 15));
  const filter = a.querySelector("#history-filter");
  filter.value = "zzz-matches-nothing";
  filter.dispatch("input");
  const shrunk = mount(doc, historyGrid(doc, 5, 3));
  const box = shrunk.querySelector("#history-filter");
  assert.ok(box !== null && box.shown(), "the box is rendered over 5 rows because a filter is in force");
  assert.equal(box.value, "zzz-matches-nothing", "showing the filter that applies");
  assert.equal(shownRows(shrunk).length, 0, "the filter does hide every row");
  const hidden = shrunk.querySelector("#history-hidden");
  assert.ok(hidden.shown(), "the hidden line is shown");
  assert.match(hidden.textContent, /^5 runs hidden by the filter\. Clear filter$/);
  shrunk.querySelector("#history-clear").click();
  assert.equal(shownRows(shrunk).length, 5, "Clear shows all five");
  assert.equal(hidden.shown(), false, "and the hidden line goes away");
  assert.equal(doc.activeElement, box, "focus returns to the emptied filter");
});

test("a_topic_filter_for_one_point_does_not_hide_the_next_points_topics", () => {
  const topicsGrid = (doc, uid, count) => build(doc, ["div", { class: "datagrid",
    "data-datagrid": "subset-topics", "data-datagrid-label": "topics", "data-datagrid-scope": uid },
  ["ul", { class: "topic-subset" }, ...Array.from({ length: count }, (_, i) =>
    ["li", {}, ["label", {}, ["input", { type: "checkbox", class: "topic-box",
      id: "topic-" + String(i) }], "orders.stream-" + String(i)]])]]);
  // The same wizard hash shape, a different point: scope is the point's uid.
  const doc = fakeDom("#/restore?ns=team-a&backup=b1&uid=u-1");
  const one = mount(doc, topicsGrid(doc, "u-1", 15));
  const filter = one.querySelector("#subset-topics-filter");
  filter.value = "stream-1";
  filter.dispatch("input");
  const visible = (grid) => grid.querySelectorAll("li").filter((li) => li.shown()).length;
  assert.equal(visible(one), 6, "stream-1 and stream-10..14");
  doc.defaultView.location.hash = "#/restore?ns=team-a&backup=b2&uid=u-2";
  const two = mount(doc, topicsGrid(doc, "u-2", 3));
  assert.equal(visible(two), 3, "all three topics of the next point are visible and tickable");
  assert.equal(two.querySelector("#subset-topics-filter"), null);
  // And every box of the first point stayed in its form while filtered.
  assert.equal(one.querySelectorAll(".topic-box").length, 15);
});

test("a_row_needing_attention_on_another_page_is_counted_on_this_one", () => {
  const doc = fakeDom("#/history?ns=team-c");
  const grid = mount(doc, historyGrid(doc, 25, 22));
  assert.equal(shownRows(grid).length, 20, "page 1 of 25");
  const line = grid.querySelector("#history-attention");
  assert.ok(line.shown(), "the attention line is shown");
  assert.equal(line.textContent,
    "1 needing attention (failed, unverified, refused or unavailable) is not on this page: page 2.");
  grid.querySelector("#history-next").click();
  assert.equal(line.shown(), false, "on page 2 it is on the page, and the line goes away");
});
