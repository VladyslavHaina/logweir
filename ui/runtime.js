// runtime.js -- an empty namespace context.
//
// The page starts with no namespace rather than silently assuming one;
// `GET /api/v1/session` names the namespaces granted to THIS ACTOR, and
// `ui/client.js` replaces whatever is written here with them.
//
// AND THE CONSOLE NEVER SERVES THIS FILE. `logweir-api` serves its own,
// compiled in (`crates/logweir-api/src/assets.rs` `CONSOLE_RUNTIME_JS`), which
// also sets the console marker `window.LOGWEIR_CONSOLE`, which this file must
// never set.
window.LOGWEIR_NAMESPACE_CONTEXT = Object.freeze({ allowed: [], selected: "" });
