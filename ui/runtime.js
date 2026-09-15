// runtime.js -- standalone namespace context.
//
// A plain `kubectl proxy --www=./ui` has no installation configuration, so it
// starts with no namespace rather than silently assuming one. Helm mounts its
// immutable, content-addressed runtime.js over this file with the namespaces
// it actually bound for the UI ServiceAccount.
window.LOGWEIR_NAMESPACE_CONTEXT = Object.freeze({ allowed: [], selected: "" });
