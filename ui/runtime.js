// runtime.js -- standalone namespace context.
//
// A plain `kubectl proxy --www=./ui` has no installation configuration, so it
// starts with no namespace rather than silently assuming one. Helm mounts its
// immutable, content-addressed runtime.js over this file with the namespaces
// it actually bound for the UI ServiceAccount.
//
// THIS FILE IS THE LEGACY MODE'S ANSWER AND ONLY ITS ANSWER (PLAT-18.1,
// decision D0 stage 6). It is the INSTALLATION's list: what the chart bound
// for the one ServiceAccount the proxy runs as, the same list for every viewer
// of that proxy. In CONSOLE MODE there is a better answer and the page uses
// it instead -- `GET /api/v1/session` names the namespaces granted to THIS
// ACTOR, and `ui/client.js` replaces whatever is written here with them once
// the mode is decided. Nothing fetches this file in that mode for its
// contents; it is still served, because `index.html` asks for it before
// `app.js` and a missing script is an error in the console.
window.LOGWEIR_NAMESPACE_CONTEXT = Object.freeze({ allowed: [], selected: "" });
