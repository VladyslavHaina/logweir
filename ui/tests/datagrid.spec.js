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
    ["backups", "backups", renderBackupList(backups, "ns")],
    ["history", "runs", renderHistoryList(fixture("restore-valid-pass.json"), backups, "ns")],
    ["clusters", "connections", renderClusterList(fixture("cluster-scram.json"))],
    ["schedules", "schedules", renderScheduleList(fixture("schedule-retention.json"))],
    ["approvals", "approvals", renderApprovalList(fixture("approvals-selfattested.json"), 0)],
  ];
  for (const [id, label, html] of lists) {
    assert.ok(
      html.includes("<div class=\"datagrid\" data-datagrid=\"" + id + "\" data-datagrid-label=\"" +
        label + "\"><div class=\"table-wrap\">"),
      id + " declares the datagrid `" + id + "` over its table:\n" + html.slice(0, 400),
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
      "data-datagrid-label=\"topics\"><ul class=\"topic-subset\">"),
    "the subset list is wrapped as a list datagrid",
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
