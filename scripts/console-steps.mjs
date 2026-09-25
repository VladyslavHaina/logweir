// The steps a person takes on the console pages that console-ux-1 redesigned, shared by the live
// browser harnesses (scripts/*-ui-*.mjs, scripts/live/poc/*.mjs). Nothing here reads a cluster;
// every helper drives the page the way a person does and THROWS when the page does not answer
// the way the helper requires, so a journey never goes on from a step it did not reach.
//
// ONE WIZARD STEP AT A TIME (MCP-29). The restore wizard renders all six steps and hides every
// one but the step on screen (`<div class="wizard-page" data-wizard-step="0..5" hidden>`), with
// Back / Next below and "Step N of 6: <title>" between them. A control on a hidden step can be
// neither filled nor clicked (Playwright waits for it to be visible), and `body.innerText`
// carries only the step on screen. So a journey walks to the step it drives: Next forward, Back
// backward, ONE step per click, and every click is required to land on the next step -- the
// position line names it, with its own title, and its page is the only one not hidden.
//
// Which control is on which step is ui/pages/restore-wizard.js's own `FIELD_STEP` and render
// functions; the offline guard (scripts/live/wizard_steps.py, run by
// scripts/live/poc/test_poc_harness.py and e2e/journeys/test_catalogue.py) holds every wizard
// control in the harnesses behind a walk to its step, and reads the titles below against the
// page's `STEPS`.

/** The six steps' titles, in order, exactly as `ui/pages/restore-wizard.js` `STEPS` names them. */
export const WIZARD_STEPS = Object.freeze(["Archive", "Recovery point", "Point in time",
  "Target, topic subset and naming", "Operation readiness", "Plan, hash and names"]);

/** What the wizard shows now: the position line's step and title, and which pages are visible
 *  (1-based). `position` 0 when there is no wizard on screen. */
export async function wizardShows(page) {
  return page.evaluate(() => {
    const line = document.querySelector("#wizard-position");
    const m = line ? /^Step ([1-6]) of 6: (.+)$/.exec(line.textContent.trim()) : null;
    const shown = Array.from(document.querySelectorAll("[data-wizard-step]"))
      .filter((p) => !p.hidden).map((p) => Number(p.getAttribute("data-wizard-step")) + 1);
    return { position: m ? Number(m[1]) : 0, title: m ? m[2] : "", shown: shown };
  });
}

/** Waits until step `n` (1-6) is the one on screen and returns `n`; throws after `seconds`
 *  (default 30) with what the page showed instead. */
export async function wizardAt(page, n, seconds) {
  const budget = (seconds || 30) * 1000;
  const end = Date.now() + budget;
  let seen = null;
  while (Date.now() < end) {
    seen = await wizardShows(page);
    if (seen.position === n && seen.title === WIZARD_STEPS[n - 1] && seen.shown.length === 1 &&
      seen.shown[0] === n) {
      return n;
    }
    await page.waitForTimeout(250);
  }
  throw new Error("the restore wizard is not on step " + n + " (" + WIZARD_STEPS[n - 1] + ") after " +
    (budget / 1000) + " s; it shows " + JSON.stringify(seen));
}

/** Walks to step `n` (1-6) with Next / Back, one step per click, each click required to land on
 *  the step after (or before) it. Returns the steps passed through, first to last, for a row's
 *  evidence: `[1, 2, 3, 4]` for a walk from step 1 to step 4. */
export async function wizardStep(page, n) {
  if (!(Number.isInteger(n) && n >= 1 && n <= WIZARD_STEPS.length)) {
    throw new Error("there is no wizard step " + n);
  }
  let at = (await wizardShows(page)).position;
  if (at === 0) {
    throw new Error("the restore wizard is not on screen, so step " + n + " cannot be reached");
  }
  const walked = [at];
  while (at !== n) {
    const forward = at < n;
    await page.click(forward ? "#wizard-next" : "#wizard-back");
    at = await wizardAt(page, forward ? at + 1 : at - 1, 30);
    walked.push(at);
  }
  return walked;
}

/** THE WHOLE WIZARD'S TEXT, as a person reads it step by step: the page on step 1 (the chrome,
 *  the banners above the stepper and step 1), then each of steps 2-6's own page, walked with
 *  Next, and finally the walk back to the step it started on. For a check that read the whole
 *  wizard from `body.innerText` when all six steps were on screen at once -- an absence ("no
 *  sentence claims ...") must still be about every step, not only the one on screen. */
export async function wizardText(page) {
  const start = (await wizardShows(page)).position;
  await wizardStep(page, 1);
  const parts = [await page.evaluate(() => document.body.innerText)];
  for (let n = 2; n <= WIZARD_STEPS.length; n += 1) {
    await wizardStep(page, n);
    parts.push(await page.evaluate((i) =>
      document.querySelector("[data-wizard-step=\"" + i + "\"]").innerText, n - 1));
  }
  await wizardStep(page, start);
  return parts.join("\n");
}

/** The same walk BY KEYBOARD ALONE, for the journeys that prove a keyboard user reaches every
 *  control: Next / Back are reached with `tabTo(selector, label)` -- the caller's own Tab walk,
 *  which records its focus trail -- and pressed with Enter. Returns the walk and the trails. */
export async function wizardStepByKeyboard(page, n, tabTo) {
  if (!(Number.isInteger(n) && n >= 1 && n <= WIZARD_STEPS.length)) {
    throw new Error("there is no wizard step " + n);
  }
  let at = (await wizardShows(page)).position;
  if (at === 0) {
    throw new Error("the restore wizard is not on screen, so step " + n + " cannot be reached");
  }
  const walked = [at];
  const trails = [];
  while (at !== n) {
    const forward = at < n;
    const button = forward ? "#wizard-next" : "#wizard-back";
    trails.push(await tabTo(button, (forward ? "Next" : "Back") + " from step " + at));
    await page.keyboard.press("Enter");
    at = await wizardAt(page, forward ? at + 1 : at - 1, 30);
    walked.push(at);
  }
  return { walked: walked, trails: trails };
}

/** THE ONE TIMESTAMP FORMAT (MCP-7, MCP-15), written here independently of ui/render.js, so a
 *  row that compares a page with it checks the page rather than the page's own formatter: an
 *  RFC 3339 instant read as `YYYY-MM-DD HH:MM:SS UTC`, whole seconds, in UTC (a value already
 *  in UTC digit for digit). The page puts the exact recorded value in the `<time>`'s `datetime`
 *  and `title`; `timeInstants` reads those. Throws on a value that is not an instant. */
export function humanUtc(value) {
  const m = /^(\d{4}-\d{2}-\d{2})[Tt ](\d{2}:\d{2}:\d{2})(\.\d+)?([Zz]|[+-]\d{2}:\d{2})$/
    .exec(String(value).trim());
  if (m === null) {
    throw new Error("not an RFC 3339 instant: " + JSON.stringify(value));
  }
  if (/^([Zz]|[+-]00:00)$/.test(m[4])) {
    return m[1] + " " + m[2] + " UTC";
  }
  const iso = new Date(Date.parse(m[1] + "T" + m[2] + m[4])).toISOString();
  return iso.slice(0, 10) + " " + iso.slice(11, 19) + " UTC";
}

/** The exact instants (`datetime`) of every `<time>` under `selector`, in document order. */
export async function timeInstants(page, selector) {
  return page.locator(selector).first().locator("time")
    .evaluateAll((ts) => ts.map((t) => t.getAttribute("datetime")));
}

/** THE CREATE-DESTINATION FORM SITS BEHIND A "Create destination" DISCLOSURE (MCP-10), closed
 *  unless a draft, a refusal or a pending create is in flight. Opens it with a click on its
 *  summary, as a person does, and requires the form on screen after. Returns whether it had to
 *  be opened. */
export async function openDestinationCreate(page, seconds) {
  const timeout = (seconds || 30) * 1000;
  const disclosure = page.locator("#destination-create-disclosure");
  await disclosure.waitFor({ state: "attached", timeout: timeout });
  const wasOpen = await disclosure.evaluate((d) => d.open === true);
  if (!wasOpen) {
    await disclosure.locator("summary").click();
  }
  await page.waitForSelector("#destination-form", { state: "visible", timeout: timeout });
  return !wasOpen;
}
