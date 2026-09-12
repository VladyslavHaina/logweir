// design.spec.js -- the design system's own guarantees, under node --test.
// Task 36.
//
// Run with `node --test 'ui/tests/*.spec.js'` from `logweir/`, which is what
// `scripts/check-ui-behaviour.sh` runs on every `just lint`.
//
// WHY MOST OF THESE READ BYTES. A stylesheet has no runtime under node, and
// the properties that matter -- a token layer defined once for light and once
// for dark, a reduced-motion block, a focus ring, no resource named outside
// the directory -- are properties of the bytes of `style.css`, so they are
// asserted over those bytes the way `crates/logweir/tests/ui_lint.rs` asserts
// over the bytes of every other shipped file. The badge row and the stepper
// row are different: they call the page modules' own pure functions and read
// what comes back, because "every badge carries words" is a claim about what
// is rendered and not about how it is coloured.
//
// NOTHING HERE REACHES THE NETWORK. `preview-server.js` is read as TEXT and
// never imported: it is a development tool, and this row only asserts what
// its source says about the address it binds and the writes it refuses.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { badge, phaseBadge } from "../render.js";
import { backupBadge, renderBackupList } from "../pages/backups.js";
import { renderHistoryList, restoreBadge } from "../pages/history.js";
import { reachableBadge, renderClusterDetail, renderClusterList } from "../pages/clusters.js";
import { renderScheduleList, suspendBadge } from "../pages/schedules.js";
import { renderApprovalsPage } from "../pages/approvals.js";
import {
  STEPS,
  initialState,
  renderRestoreWizard,
  stepStates,
} from "../pages/restore-wizard.js";

const UI = fileURLToPath(new URL("../", import.meta.url));
const TESTS = fileURLToPath(new URL("./", import.meta.url));
const REPO = fileURLToPath(new URL("../../", import.meta.url));
const FIXTURES = TESTS + "fixtures/";

function fixture(name) {
  return JSON.parse(readFileSync(FIXTURES + name, "utf8"));
}

const css = readFileSync(UI + "style.css", "utf8");

/** The ten semantic colour roles the token layer must name, in both schemes. */
const SEMANTIC = [
  "--surface",
  "--surface-raised",
  "--border",
  "--text",
  "--text-muted",
  "--accent",
  "--success",
  "--warning",
  "--danger",
  "--info",
];

/** The first brace-balanced block that follows `opener` in `text`, from
 *  `from`: `{start, body, end}` or `null`. */
function blockAfter(text, opener, from) {
  const at = text.indexOf(opener, from || 0);
  if (at === -1) {
    return null;
  }
  const open = text.indexOf("{", at);
  let depth = 0;
  for (let i = open; i < text.length; i += 1) {
    if (text[i] === "{") {
      depth += 1;
    } else if (text[i] === "}") {
      depth -= 1;
      if (depth === 0) {
        return { start: at, body: text.slice(open + 1, i), end: i };
      }
    }
  }
  return null;
}

/** The value a block declares for a custom property, or `null`. The colon
 *  has to follow the name directly, so `--surface` does not match
 *  `--surface-raised`. */
function declared(body, name) {
  const pattern = new RegExp("(^|[\\s;{])" + name.replace(/-/g, "\\-") + "\\s*:\\s*([^;]+);");
  const match = body.match(pattern);
  return match === null ? null : match[2].trim();
}

const BADGE = /<span class="badge badge-([a-z0-9-]+)">([^<]*)<\/span>/g;

/** Every badge in a rendered string: its kind and its caption. */
function badgesIn(html) {
  const out = [];
  let match;
  BADGE.lastIndex = 0;
  while ((match = BADGE.exec(html)) !== null) {
    out.push({ kind: match[1], text: match[2].trim() });
  }
  return out;
}

// -------------------------------------------------------------------- rows

test("the_stylesheet_defines_the_token_layer_for_light_and_dark", () => {
  const light = blockAfter(css, ":root");
  assert.ok(light !== null, "style.css opens with a :root block that holds the tokens");
  for (const name of SEMANTIC) {
    assert.ok(
      declared(light.body, name) !== null,
      "the light :root block declares " + name + "; the block was:\n" + light.body,
    );
  }
  for (const scale of ["--font-sans", "--font-mono", "--text-base", "--space-4", "--radius-m"]) {
    assert.ok(declared(light.body, scale) !== null, "the token layer carries " + scale);
  }
  assert.match(
    declared(light.body, "--font-sans"),
    /^-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif$/,
    "the sans stack is the system stack and nothing is fetched",
  );

  const dark = blockAfter(css, "@media (prefers-color-scheme: dark)");
  assert.ok(dark !== null, "a prefers-color-scheme: dark block exists");
  assert.ok(
    dark.start > light.end,
    "the dark block comes AFTER the light one, so its redefinitions win the cascade",
  );
  const darkRoot = blockAfter(dark.body, ":root");
  assert.ok(darkRoot !== null, "the dark block redefines the tokens on :root");
  for (const name of SEMANTIC) {
    const before = declared(light.body, name);
    const after = declared(darkRoot.body, name);
    assert.ok(after !== null, "the dark block redefines " + name + "; it was:\n" + darkRoot.body);
    assert.notEqual(
      after,
      before,
      name + " has a different value in the dark scheme -- a dark block that repeats the " +
        "light values is not a dark scheme",
    );
  }
  assert.ok(
    /color-scheme:\s*light dark/.test(light.body),
    "color-scheme: light dark, so form controls and scrollbars follow the scheme too",
  );
});

test("the_stylesheet_honours_reduced_motion_and_shows_focus", () => {
  const reduce = blockAfter(css, "@media (prefers-reduced-motion: reduce)");
  assert.ok(reduce !== null, "a prefers-reduced-motion: reduce block exists");
  for (const rule of ["animation: none !important", "transition: none !important"]) {
    assert.ok(
      reduce.body.includes(rule),
      "the reduced-motion block carries `" + rule + "`, so every animation and every " +
        "transition in the file is switched off at once; the block was:\n" + reduce.body,
    );
  }
  assert.ok(
    /\*,\s*\*::before,\s*\*::after\s*\{/.test(reduce.body),
    "and it applies to every element and pseudo-element, not to a list somebody maintains",
  );

  const focus = blockAfter(css, ":focus-visible");
  assert.ok(focus !== null, "a :focus-visible rule exists");
  assert.ok(
    /outline:\s*2px solid/.test(focus.body),
    "and it draws a visible ring; the rule was:\n" + focus.body,
  );
  const selector = css.slice(css.lastIndexOf("\n\n", focus.start), focus.start + ":focus-visible".length);
  for (const element of ["a", "button", "input", "select", "textarea"]) {
    assert.ok(
      new RegExp("(^|\\s|,)" + element + ":focus-visible").test(css),
      element + ":focus-visible is covered; the first focus rule's selector list was:" + selector,
    );
  }
});

test("the_stylesheet_names_no_resource_outside_the_directory", () => {
  // The offline gate's rule 1, restated for CSS: no import at-rule of any
  // spelling (the gate's own token is the url() form only), no url() that
  // does not begin with a dot or a data scheme, and no scheme at all.
  assert.equal(css.indexOf("@import"), -1, "style.css carries no @import of any spelling");
  let from = 0;
  let urls = 0;
  while (true) {
    const at = css.indexOf("url(", from);
    if (at === -1) {
      break;
    }
    urls += 1;
    const argument = css.slice(at + 4, css.indexOf(")", at)).trim().replace(/^["']|["']$/g, "");
    assert.ok(
      argument.startsWith("./") || argument.startsWith("data:"),
      "url(" + argument + ") names a resource outside the directory the page was served from",
    );
    from = at + 4;
  }
  // (A source-map annotation is not listed here: `no_build_step_exists` in
  // ui_lint.rs forbids it over every file under ui/, this one included.)
  for (const token of ["http:", "https:", "@font-face"]) {
    assert.equal(css.indexOf(token), -1, "style.css carries " + JSON.stringify(token));
  }
  assert.equal(urls, 0, "and, today, no url() at all: the one graphic is inline SVG in index.html");
});

test("the_tables_collapse_to_cards_below_the_breakpoint", () => {
  const narrow = blockAfter(css, "@media (max-width: 719.98px)");
  assert.ok(narrow !== null, "a max-width: 719.98px block exists -- the stacked-card breakpoint");
  assert.ok(
    narrow.body.includes(".grid td::before") && narrow.body.includes("content: attr(data-label)"),
    "below it every cell shows its column caption from data-label; the block was:\n" + narrow.body,
  );
  assert.ok(
    /\.grid,\s*\.grid tbody,\s*\.grid tr,\s*\.grid td\s*\{\s*display:\s*block/.test(narrow.body),
    "and the table's own display is dropped so rows stack",
  );

  // The caption is copied at adoption time, with setAttribute and never
  // innerHTML, so the string the suite asserts on is the string a browser
  // receives.
  const app = readFileSync(UI + "app.js", "utf8");
  assert.ok(app.includes("function labelTableCells("), "app.js labels the cells");
  assert.ok(app.includes('setAttribute("data-label"'), "with setAttribute");
  assert.equal(app.indexOf("innerHTML ="), -1, "and never by assigning innerHTML");
  assert.ok(app.includes("labelTableCells(parsed)"), "and it runs inside parseFragment");

  // And the string half puts a table where the stylesheet expects one.
  const out = renderClusterList(fixture("cluster-scram.json"));
  assert.ok(out.includes('<div class="table-wrap"><table class="grid"><thead><tr><th scope="col">'));
});

// Measured at 390 px before these two rules existed (the review of Task 36):
// a stacked cell holding an unbreakable id widened the page to 417 px, and a
// two-column fact list rendered a topic list one character per line. The
// narrow block must keep both: the value column of a stacked cell allowed to
// shrink to nothing (`minmax(0, 1fr)`), and a fact list in one column.
test("the_narrow_block_keeps_long_values_inside_their_cards", () => {
  const narrow = blockAfter(css, "@media (max-width: 719.98px)");
  assert.ok(narrow !== null, "a max-width: 719.98px block exists -- the stacked-card breakpoint");
  // The stacked cell's own rule -- not the multi-selector one above it that
  // also ends in `.grid td {` -- is the one whose display is grid.
  const cell = blockAfter(narrow.body, ".grid td {\n    display: grid;");
  assert.ok(cell !== null, "the stacked cell rule (display: grid) exists in the narrow block");
  assert.ok(
    /grid-template-columns:\s*minmax\(6\.5rem, 34%\) minmax\(0, 1fr\)/.test(cell.body),
    "a stacked cell's value column may shrink to nothing, so an id cannot widen the card; the rule was:\n" +
      cell.body,
  );
  const facts = blockAfter(narrow.body, ".facts {");
  assert.ok(facts !== null, "the narrow block restyles .facts");
  assert.ok(
    /grid-template-columns:\s*1fr\s*;/.test(facts.body),
    "a fact list is one column on a phone -- the caption over its value; the rule was:\n" + facts.body,
  );
});

test("every_badge_carries_a_text_label_and_not_only_a_colour", () => {
  // The primitive: a class for the colour, a caption for the words.
  assert.equal(badge("green", "verified"), '<span class="badge badge-green">verified</span>');

  // The producers, each over the states it can be in. Every one renders
  // exactly one badge, with a kind and with words, and the words differ
  // between the states -- so a reader who cannot tell the hues apart still
  // reads two different captions.
  const pairs = [
    ["backupBadge green", backupBadge(fixture("backup-valid-exit0.json").status)],
    ["backupBadge unverified", backupBadge(fixture("backup-invalid-exit0.json").status)],
    ["restoreBadge green", restoreBadge(fixture("restore-valid-pass.json").status)],
    ["restoreBadge unverified", restoreBadge(fixture("restore-valid-failintegrity.json").status)],
    ["reachableBadge true", reachableBadge({ reachable: true })],
    ["reachableBadge false", reachableBadge({ reachable: false })],
    ["suspendBadge true", suspendBadge({ suspend: true })],
    ["suspendBadge false", suspendBadge({ suspend: false })],
    ["phaseBadge Succeeded", phaseBadge("Succeeded")],
    ["phaseBadge Running", phaseBadge("Running")],
    ["phaseBadge Failed", phaseBadge("Failed")],
    ["phaseBadge unknown", phaseBadge("SomethingNew")],
  ];
  const captions = {};
  for (const [label, html] of pairs) {
    const found = badgesIn(html);
    assert.equal(found.length, 1, label + " renders exactly one badge: " + html);
    assert.ok(found[0].kind.length > 0, label + " has a kind");
    assert.ok(found[0].text.length > 0, label + " CARRIES WORDS, not only a colour: " + html);
    captions[label] = found[0].text;
  }
  assert.notEqual(captions["backupBadge green"], captions["backupBadge unverified"]);
  assert.notEqual(captions["restoreBadge green"], captions["restoreBadge unverified"]);
  assert.notEqual(captions["reachableBadge true"], captions["reachableBadge false"]);
  assert.notEqual(captions["suspendBadge true"], captions["suspendBadge false"]);
  assert.equal(captions["phaseBadge unknown"], "SomethingNew", "a phase's caption is the phase itself");
  assert.equal(phaseBadge(undefined), "-", "and an absent phase is the absent marker, not an empty badge");

  // The rendered pages: every badge on them has words too.
  const backups = { items: [
    fixture("backup-valid-exit0.json"),
    fixture("backup-valid-exit2.json"),
    fixture("backup-invalid-exit0.json"),
    fixture("backup-notattempted-exit0.json"),
  ] };
  const route = { ns: "logweir-t27", subject: "", hash: "", name: "" };
  const pages = [
    ["renderBackupList", renderBackupList(backups, "ns")],
    ["renderHistoryList", renderHistoryList(fixture("restore-valid-pass.json"), backups, "ns")],
    ["renderClusterList", renderClusterList(fixture("cluster-scram.json"))],
    ["renderClusterDetail", renderClusterDetail(fixture("cluster-scram.json"))],
    ["renderScheduleList", renderScheduleList(fixture("schedule-retention.json"))],
    ["renderApprovalsPage", renderApprovalsPage(fixture("approvals-selfattested.json"), route, 0)],
  ];
  for (const [label, html] of pages) {
    const found = badgesIn(html);
    assert.ok(found.length > 0, label + " renders at least one badge");
    for (const one of found) {
      assert.ok(one.text.length > 0, label + ": a badge of kind " + one.kind + " has no words");
    }
  }

  // And the stylesheet knows every kind the pages produce, so none of them
  // falls back to the neutral look by accident.
  for (const kind of ["green", "unverified", "ok", "flat", "warn", "phase-succeeded", "phase-failed", "phase-running"]) {
    assert.ok(css.includes(".badge-" + kind), "style.css styles .badge-" + kind);
  }
  assert.ok(css.includes(".badge::before"), "and draws the dot that is the badge's second channel");
});

test("the_wizard_stepper_says_which_step_is_current", async () => {
  const state = initialState(
    "logweir-t27",
    fixture("wizard-clusters.json"),
    fixture("wizard-backups.json"),
  );
  const steps = stepStates(state);
  assert.equal(steps.length, 6, "six steps");
  assert.deepEqual(
    steps.map((s) => s.title),
    STEPS.map((s) => s.title),
    "in the order the sections render",
  );
  assert.deepEqual(
    steps.map((s) => s.status),
    ["done", "done", "done", "done", "done", "ready"],
    "with everything prefilled from the cluster, the five input steps are done and the " +
      "plan step is where you are",
  );
  assert.deepEqual(steps.map((s) => s.current), [false, false, false, false, false, true]);

  // A point outside the covered window: step 3 needs attention and is the
  // step you are on; the plan step is no longer ready.
  const outside = initialState(
    "logweir-t27",
    fixture("wizard-clusters.json"),
    fixture("wizard-backups.json"),
  );
  outside.fields.pointInTime = "2026-09-08T00:00:00Z";
  const complained = stepStates(outside);
  assert.equal(complained[2].status, "attention");
  assert.equal(complained[2].current, true);
  assert.equal(complained[5].status, "todo");
  assert.equal(complained.filter((s) => s.current).length, 1, "exactly one current step");

  // mode scratch against a cluster with no markerTopic: step 4 is whole but
  // flagged, and you are still at the plan, because a warning is not a refusal.
  const scratch = initialState(
    "logweir-t28",
    fixture("wizard-clusters-source-only.json"),
    fixture("wizard-backups-schedule-running.json"),
  );
  scratch.fields.target.mode = "scratch";
  const warned = stepStates(scratch);
  assert.equal(warned[3].status, "attention");
  assert.equal(warned[3].current, false);
  assert.equal(warned[5].status, "ready");

  // The rendered stepper: one list, one aria-current, a button per step that
  // names a section which is actually on the page, and the state in words.
  const html = await renderRestoreWizard(state);
  assert.ok(html.includes('<ol class="stepper"'), "the stepper is rendered");
  assert.equal((html.match(/aria-current="step"/g) || []).length, 1, "one current step");
  for (const step of STEPS) {
    assert.ok(html.includes('data-target="' + step.id + '"'), "a button for " + step.id);
    assert.ok(html.includes('id="' + step.id + '"'), "and the section it scrolls to");
  }
  assert.ok(html.includes('<span class="stepper-status">done</span>'), "done, in words");
  assert.ok(html.includes('<span class="stepper-status">review and create</span>'), "ready, in words");
  const complaintHtml = await renderRestoreWizard(outside);
  assert.ok(
    complaintHtml.includes('<span class="stepper-status">needs attention</span>'),
    "attention, in words",
  );
  for (let i = 0; i < 6; i += 1) {
    assert.ok(
      html.includes("<h3>" + String(i + 1) + ". " + STEPS[i].title + "</h3>"),
      "the stepper's title for step " + String(i + 1) + " is the section heading's own",
    );
  }
});

test("the_preview_server_binds_loopback_and_refuses_writes", () => {
  // Read as text, never imported: the tool opens a socket on purpose and the
  // suite must not.
  const source = readFileSync(TESTS + "preview-server.js", "utf8");
  assert.ok(source.includes('const HOST = "127.0.0.1";'), "the bind address is the loopback literal");
  assert.ok(source.includes(".listen(PORT, HOST"), "and listen is handed it by name");
  for (const token of ["0.0.0.0", '"::"', "[::]"]) {
    assert.equal(source.indexOf(token), -1, "no wildcard address anywhere: " + token);
  }
  assert.ok(source.includes("const METHOD_NOT_ALLOWED = 405;"), "every write is a 405");
  assert.ok(
    source.includes('method !== "GET" && method !== "HEAD"'),
    "refused before anything is looked up",
  );
  for (const token of ["writeFileSync", "appendFileSync", "createWriteStream"]) {
    assert.equal(source.indexOf(token), -1, "the preview writes nothing, not even a file: " + token);
  }

  // It is a tool: the gate's glob never runs it and the gate never names it.
  const gate = readFileSync(REPO + "scripts/check-ui-behaviour.sh", "utf8");
  assert.equal(gate.indexOf("preview-server"), -1, "check-ui-behaviour.sh does not run the preview");

  // And ui/README.md documents it under its own heading, with the command.
  const readme = readFileSync(UI + "README.md", "utf8");
  assert.ok(readme.includes("## Previewing with fixtures"), "the README heading");
  assert.ok(readme.includes("node ui/tests/preview-server.js"), "the command");
});
