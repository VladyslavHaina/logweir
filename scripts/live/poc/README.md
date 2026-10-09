# PoC live harness

The Playwright journeys (`*.mjs`) and Python rows (`*.py`) here drive the PoC
install ([deploy/poc/README.md](../../../deploy/poc/README.md), steps 9 and 10)
the way a person does: a real Chromium signs in through Traefik and Dex and
uses the shared console. Each file's header says what it proves and how to run
it, for example:

```bash
NODE_PATH="$(npm root -g)" node scripts/live/poc/journey.mjs /tmp/poc-journey
```

`LOGWEIR_POC_CREDENTIALS` names the credentials file (default
`~/.logweir-poc/credentials.txt`), `CONSOLE_HOST` the console's host name and
`POC_NAMESPACE` the namespace (default `logweir-poc`). Every `kubectl` call
names `--context docker-desktop`.

## The browser

Every journey launches its browser through `launchBrowser()` in
[console.mjs](console.mjs). By default that is Playwright's own
`chromium.launch()`, which runs the headless shell the installed Playwright
version pins. On a host whose browser cache holds a different build, it fails
before the first page:

```text
browserType.launch: Executable doesn't exist at
  …/ms-playwright/chromium_headless_shell-1208/chrome-headless-shell-mac-arm64/chrome-headless-shell
```

Either fix works:

1. Install the build that Playwright pins, with the same Playwright the
   journeys load (`npx --no-install playwright --version` prints the version of
   `$(npm root -g)/playwright`):

   ```bash
   npx --no-install playwright install chromium-headless-shell
   ```

2. Point the journeys at a Chromium or Chrome you already have:

   ```bash
   export LOGWEIR_POC_CHROMIUM="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
   ```

   `UI_E2E_CHROMIUM`, the same override in the `scripts/*-ui-e2e.mjs`
   harnesses, is read when `LOGWEIR_POC_CHROMIUM` is not set. A path that does
   not exist is refused before anything is launched.

## Offline guards

```bash
python3 -m pytest scripts/live/poc/test_poc_harness.py
```

No cluster and no browser: the rows read the files' text (every `kubectl`
names its context, no credential-shaped literal, no secret printed, every
journey launches through `launchBrowser()`), and one row runs `console.mjs`
under `node` with a stand-in `playwright` module to check what
`launchBrowser()` hands it.
