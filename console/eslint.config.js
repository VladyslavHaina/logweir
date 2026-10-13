// The console's lint rules: the recommended JavaScript and TypeScript sets, and the three
// rules that replaced the old console's ui_lint.rs and check-ui-offline.sh gates
// (docs/to-do/decisions/PROD-18.1-console.md §3.3):
//   - no `dangerouslySetInnerHTML`: text only, never markup from data;
//   - `fetch` and `EventSource` only in src/api/: one place issues requests;
//   - no browser storage (`localStorage`, `sessionStorage`, `indexedDB`, `document.cookie`):
//     the session cookie is the browser's, the CSRF token lives in memory.
import js from "@eslint/js";
import tseslint from "typescript-eslint";

const STORAGE = ["localStorage", "sessionStorage", "indexedDB"];
const REQUESTS = ["fetch", "EventSource", "XMLHttpRequest", "WebSocket"];

const noMarkup = {
  selector: "JSXAttribute[name.name='dangerouslySetInnerHTML']",
  message: "Render text, never markup: no dangerouslySetInnerHTML.",
};
const noStorage = [
  ...STORAGE.map((name) => ({
    selector: `MemberExpression[property.name='${name}']`,
    message: `No browser storage (${name}): the console keeps nothing in the browser.`,
  })),
  {
    selector: "MemberExpression[object.name='document'][property.name='cookie']",
    message: "The session cookie is the browser's; the console never reads or writes cookies.",
  },
];
const noRequests = REQUESTS.map((name) => ({
  selector: `MemberExpression[property.name='${name}']`,
  message: `Requests go through src/api/ only (${name}).`,
}));
const globals = (names, why) => names.map((name) => ({ name, message: why }));

export default tseslint.config(
  { ignores: ["dist/", "dist-single/", "node_modules/", "src/api/schema.d.ts"] },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    files: ["**/*.{ts,tsx}"],
    rules: {
      "no-restricted-syntax": ["error", noMarkup, ...noStorage, ...noRequests],
      "no-restricted-globals": [
        "error",
        ...globals(STORAGE, "No browser storage: the console keeps nothing in the browser."),
        ...globals(REQUESTS, "Requests go through src/api/ only."),
      ],
    },
  },
  {
    // The build scripts run on Node.
    files: ["scripts/**/*.mjs"],
    languageOptions: { globals: { console: "readonly", process: "readonly", URL: "readonly" } },
  },
  {
    // The one place that issues requests. Storage and markup stay banned here too.
    files: ["src/api/**/*.{ts,tsx}"],
    rules: {
      "no-restricted-syntax": ["error", noMarkup, ...noStorage],
      "no-restricted-globals": ["error", ...globals(STORAGE, "No browser storage: the console keeps nothing in the browser.")],
    },
  },
  {
    // Tests stand in for the network with fakes they install on the global object.
    files: ["src/**/*.test.{ts,tsx}"],
    rules: {
      "no-restricted-syntax": ["error", noMarkup, ...noStorage],
      "no-restricted-globals": ["error", ...globals(STORAGE, "No browser storage, in tests either.")],
    },
  },
);
