// The console's end-to-end rows (console/e2e/), role-based Playwright against a console
// that logweir-api serves: on the host (`cargo run -p logweir-api` with a kubeconfig), a
// compose stack or the PoC. Nothing here starts a server.
//
//   LOGWEIR_CONSOLE_URL=http://127.0.0.1:8484 npx playwright test
//
// LOGWEIR_CONSOLE_STORAGE_STATE names a signed-in browser state for a shared-mode console
// (written by `npx playwright codegen --save-storage`); a localAdmin console needs none.
// LOGWEIR_E2E_CHROMIUM names a Chromium to launch instead of the one `npx playwright
// install` downloads.
import { defineConfig, devices } from "@playwright/test";

declare const process: { env: Record<string, string | undefined> };

export default defineConfig({
  testDir: "e2e",
  outputDir: "e2e-results",
  workers: 1,
  retries: 0,
  timeout: 60_000,
  reporter: [["list"]],
  use: {
    baseURL: process.env.LOGWEIR_CONSOLE_URL,
    ...(process.env.LOGWEIR_CONSOLE_STORAGE_STATE ? { storageState: process.env.LOGWEIR_CONSOLE_STORAGE_STATE } : {}),
    ...(process.env.LOGWEIR_E2E_CHROMIUM ? { launchOptions: { executablePath: process.env.LOGWEIR_E2E_CHROMIUM } } : {}),
    screenshot: "on",
    trace: "retain-on-failure",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
});
