// manual-run-queue.spec.js -- P10: a manual run waiting for a slot in its
// namespace's manual-run pool reads "Queued (limit N active)", in both modes,
// from the object's own `status.queue.limit`.
//
// THE DEFECT THIS FOLLOWS. Nothing bounded "Back up now": one operator's
// hundred accepted runs became a hundred runner pods. The controller now
// queues the runs above `runs.maxManualBackupsActivePerNamespace` with
// `phase: Queued` and `status.queue.limit`, and the API publishes the same
// ceiling as the item's `queue.limit`. These rows hold the page to it: the
// number is COPIED, never computed, and a run that is not queued is rendered
// exactly as before. Each behaviour has its negative control beside it.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { apiClient, resetMode, selectMode } from "../client.js";
import { QUEUED_RUN_SENTENCE, phaseBadge, runPhaseBadge } from "../render.js";
import { renderBackupDetail, renderBackupList } from "../pages/backups.js";
import { renderHistoryList } from "../pages/history.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const fixture = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));

function transport(answer) {
  const original = globalThis.fetch;
  globalThis.fetch = (u) => {
    const reply = answer(String(u));
    return Promise.resolve({
      ok: reply.status >= 200 && reply.status < 300,
      status: reply.status,
      text: () => Promise.resolve(JSON.stringify(reply.body)),
    });
  };
  return () => { globalThis.fetch = original; };
}

async function consoleMode() {
  resetMode();
  await selectMode({ probe: async () => ({ ok: true, status: 200, body: fixture("console/session.json") }) });
}

const QUEUED_CAPTION = "Queued (limit 4 active)";

test("a_queued_run_names_its_ceiling_and_every_other_phase_is_unchanged", () => {
  assert.equal(
    runPhaseBadge({ phase: "Queued", queue: { limit: 4 } }),
    "<span class=\"badge badge-phase-queued\">" + QUEUED_CAPTION + "</span>",
  );
  // NEGATIVE CONTROLS. No block (an older controller) is the plain word; a
  // block beside another phase is not believed; a nonsense limit is ignored;
  // every other phase is exactly `phaseBadge`'s.
  assert.equal(runPhaseBadge({ phase: "Queued" }), phaseBadge("Queued"));
  assert.equal(runPhaseBadge({ phase: "Running", queue: { limit: 4 } }), phaseBadge("Running"));
  assert.equal(runPhaseBadge({ phase: "Queued", queue: { limit: "4" } }), phaseBadge("Queued"));
  assert.equal(runPhaseBadge({ phase: "Queued", queue: { limit: 0 } }), phaseBadge("Queued"));
  for (const phase of ["Pending", "Resolving", "Running", "Succeeded", "Failed"]) {
    assert.equal(runPhaseBadge({ phase }), phaseBadge(phase), phase);
  }
  assert.equal(runPhaseBadge(undefined), phaseBadge(undefined));
});

test("the_legacy_backups_page_reads_the_custom_resources_own_queue_block", () => {
  const queued = {
    metadata: { name: "logweir-manual-q", namespace: "team-a", uid: "u-q" },
    spec: { triggeredBy: "manual", trigger: { kind: "Manual", attempt: 0 } },
    status: {
      phase: "Queued",
      queue: { limit: 4 },
      conditions: [{ type: "Admitted", status: "False", reason: "ConcurrencyLimited" }],
    },
  };
  const running = JSON.parse(JSON.stringify(queued));
  running.metadata.name = "logweir-manual-r";
  running.status = { phase: "Running", jobRef: { name: "logweir-manual-r" } };
  const page = renderBackupList({ items: [queued, running] }, "team-a");
  assert.equal((page.match(/Queued \(limit 4 active\)/g) || []).length, 1, page);
  assert.match(page, /badge-phase-running">Running</);
  // The detail says what a queued run is -- and only for a queued run.
  assert.ok(renderBackupDetail(queued).indexOf(QUEUED_RUN_SENTENCE) !== -1);
  assert.equal(renderBackupDetail(running).indexOf(QUEUED_RUN_SENTENCE), -1);
});

test("the_console_projection_carries_the_items_queue_block_under_the_resources_name", async () => {
  await consoleMode();
  const list = fixture("console/backups-list.json");
  const template = list.items[0];
  const queued = Object.assign(JSON.parse(JSON.stringify(template)), {
    name: "logweir-manual-q",
    uid: "00000000-0000-4000-8000-0000000000q1",
    triggeredBy: "manual",
    queue: { limit: 4 },
    operation: { state: "queued", stateReason: "ConcurrencyLimited", terminal: false,
      verificationState: "pending", verifiedSuccess: false },
  });
  const done = JSON.parse(JSON.stringify(template));
  list.items = [queued, done];
  const restore = transport(() => ({ status: 200, body: list }));
  try {
    const got = await apiClient().list("team-a", "backups");
    const [q, d] = got.items;
    assert.equal(q.status.phase, "Queued", "the API's `queued` is the resource's own word");
    assert.deepEqual(q.status.queue, { limit: 4 });
    // NEGATIVE CONTROL: an item without the block projects none.
    assert.equal(d.status.queue, undefined);
    const page = renderBackupList(got, "team-a");
    assert.equal((page.match(/Queued \(limit 4 active\)/g) || []).length, 1, page);
    const history = renderHistoryList({ items: [] }, got, "team-a");
    assert.equal((history.match(/Queued \(limit 4 active\)/g) || []).length, 1);
  } finally {
    restore();
  }
});
