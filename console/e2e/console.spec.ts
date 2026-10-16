// The console as an operator meets it, served by logweir-api: the page and its headers,
// the session's landing, every page the navigation offers, the restore wizard from a
// recovery point to its target, and the refusals. Role and text locators only. Each
// screen is checked for console errors, CSP violations, requests off the origin and axe
// violations (WCAG 2.2 AA). Read-only: nothing here creates anything.
//
// It replaces the old console's live harnesses (scripts/prod16-1-ui-e2e.mjs and the
// wizard walk in scripts/console-steps.mjs); the journeys that need a cluster (sign-in
// with Dex, Back up now, a restore to Verified, approvals) are the PoC pass's rows.
import { expect, test, type Page } from "@playwright/test";

declare const process: { env: Record<string, string | undefined> };

test.skip(!process.env.LOGWEIR_CONSOLE_URL, "set LOGWEIR_CONSOLE_URL to the console's origin");

const AXE = "node_modules/axe-core/axe.min.js";
const WCAG = ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"];

interface Watch {
  errors: string[];
  offOrigin: string[];
}

/** Console errors, page errors, CSP violations and requests to another origin, from the first byte. */
async function watch(page: Page): Promise<Watch> {
  const w: Watch = { errors: [], offOrigin: [] };
  const origin = new URL(process.env.LOGWEIR_CONSOLE_URL!).origin;
  page.on("console", (m) => m.type() === "error" && w.errors.push(`console: ${m.text()}`));
  page.on("pageerror", (e) => w.errors.push(`page: ${e.message}`));
  page.on("request", (r) => !r.url().startsWith(origin) && !r.url().startsWith("data:") && w.offOrigin.push(r.url()));
  // Injected by the browser before the page's own scripts, which the page's policy does
  // not govern: axe-core, and a listener for the policy's violations.
  await page.addInitScript({ path: AXE });
  await page.addInitScript(() => {
    document.addEventListener("securitypolicyviolation", (e) => {
      (window as unknown as { __csp: string[] }).__csp ??= [];
      (window as unknown as { __csp: string[] }).__csp.push(`${e.violatedDirective} ${e.blockedURI}`);
    });
  });
  return w;
}

/** The screen as it stands: no error, no violation, no request off the origin, and no axe
 *  finding, since the previous check. */
async function clean(page: Page, w: Watch, what: string) {
  const csp = await page.evaluate(() => {
    const seen = (window as unknown as { __csp?: string[] }).__csp ?? [];
    (window as unknown as { __csp?: string[] }).__csp = [];
    return seen;
  });
  const axe = await page.evaluate(async (tags) => {
    const run = (window as unknown as { axe: { run: (c: unknown, o: unknown) => Promise<{ violations: { id: string; nodes: unknown[] }[] }> } }).axe;
    const r = await run.run(document, { runOnly: { type: "tag", values: tags } });
    return r.violations.map((v) => `${v.id} (${v.nodes.length})`);
  }, WCAG);
  // Soft, so one screen's finding does not hide the next screen's: the test still fails.
  expect.soft({ what, errors: w.errors, csp, offOrigin: w.offOrigin, axe }).toEqual({ what, errors: [], csp: [], offOrigin: [], axe: [] });
  w.errors.length = 0;
  w.offOrigin.length = 0;
}

/** The workspace the session lands on: the address after `#/`. */
async function landing(page: Page): Promise<string> {
  await page.goto("/ui/");
  await expect(page).toHaveURL(/#\/[a-z0-9-]+/);
  return new URL(page.url()).hash.split("/")[1]!;
}

test("the page is served at /ui/ under the policy, its hashed files are immutable, and nothing else is", async ({ request }) => {
  const page = await request.get("/ui/");
  expect(page.status()).toBe(200);
  expect(page.headers()["content-security-policy"]).toContain("default-src 'self'");
  expect(page.headers()["cache-control"]).toBe("no-cache");
  const html = await page.text();
  const script = /src="\.\/(assets\/index-[A-Za-z0-9_-]+\.js)"/.exec(html)?.[1];
  expect(script, "the page names its script under assets/").toBeTruthy();
  const asset = await request.get(`/ui/${script}`);
  expect(asset.status()).toBe(200);
  expect(asset.headers()["cache-control"]).toBe("public, max-age=31536000, immutable");
  // The old console's paths and the preview are gone (negative controls for the above).
  for (const gone of ["/ui/next/", "/ui/runtime.js", "/ui/app.js", "/ui/pages/backups.js", "/ui/.vite/license.md"]) {
    expect((await request.get(gone)).status(), gone).toBe(404);
  }
});

test("the session lands on its workspace's Overview, with the navigation its role allows", async ({ page }) => {
  const w = await watch(page);
  const ws = await landing(page);
  await expect(page.getByRole("heading", { level: 1, name: "Overview" })).toBeVisible();
  const nav = page.getByRole("navigation").first();
  await expect(nav.getByRole("link", { name: /Overview/ })).toBeVisible();
  // The switcher shows only with more than one workspace; the brand link names this one either way.
  await expect(page.getByRole("link", { name: "Logweir console, overview" })).toHaveAttribute("href", `#/${ws}`);
  // A settled screen: while its reads are in flight, a control is disabled, and a
  // disabled control's contrast is not held (WCAG 1.4.3) though axe may report it.
  await page.waitForLoadState("networkidle");
  await clean(page, w, "overview");
});

test("every page the navigation offers opens on its own heading", async ({ page }) => {
  const w = await watch(page);
  await landing(page);
  await expect(page.getByRole("heading", { level: 1, name: "Overview" })).toBeVisible();
  const nav = page.getByRole("navigation").first();
  const names = (await nav.getByRole("link").allInnerTexts()).map((t) => t.split("\n")[0]!.trim()).filter(Boolean);
  expect(names.length, "the navigation lists pages").toBeGreaterThan(3);
  for (const name of names) {
    await nav.getByRole("link", { name: new RegExp(`^${name}`) }).click();
    await expect(page.getByRole("heading", { level: 1, name })).toBeVisible();
    await expect(page.getByText("This page does not exist")).toHaveCount(0);
    await page.waitForLoadState("networkidle");
    await clean(page, w, name);
  }
});

test("the restore wizard walks from a recovery point to its target", async ({ page }) => {
  const w = await watch(page);
  const ws = await landing(page);
  await page.goto(`/ui/#/${ws}/restores/new`);
  await expect(page.getByRole("heading", { name: "Which recovery point?" })).toBeVisible();
  // A table on a wide screen, a radio group on a phone: either way, labelled.
  const points = page.getByLabel("Recovery points").getByRole("radio");
  test.skip((await points.count()) === 0, "this workspace has no verified recovery point to start from");
  await points.first().check();
  await clean(page, w, "wizard: point");
  const next = page.getByRole("button", { name: "Next" });
  await expect(next).toBeEnabled();
  await next.click();
  await expect(page.getByRole("heading", { name: "What to restore" })).toBeVisible();
  await expect(next).toBeEnabled();
  await next.click();
  await expect(page.getByRole("heading", { name: "Where to restore" })).toBeVisible();
  await expect(page.getByRole("radiogroup", { name: "Target connection" })).toBeVisible();
  await clean(page, w, "wizard: target");
});

test("a workspace the session does not hold, and an address with no page, each say so", async ({ page }) => {
  const w = await watch(page);
  const ws = await landing(page);
  await page.goto(`/ui/#/${ws}/no-such-page`);
  await expect(page.getByText("This page does not exist")).toBeVisible();
  await clean(page, w, "not found");
  await page.goto("/ui/#/lw-e2e-not-granted");
  await expect(page.getByText("You hold no role in lw-e2e-not-granted")).toBeVisible();
  await clean(page, w, "not granted");
});
