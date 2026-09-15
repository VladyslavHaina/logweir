// lifecycle.js -- the ownership token for one mounted route.
//
// Reads belong to a view and may be cancelled when that view leaves. Durable
// operations do not: a POST or PATCH accepted by the API server must be allowed
// to finish even when its originating view is no longer visible. Page modules
// use this token before rendering, binding, or starting a mutation.

export function active(lifecycle) {
  return lifecycle === undefined || lifecycle === null || lifecycle.isCurrent();
}

export function readOptions(lifecycle) {
  if (lifecycle === undefined || lifecycle === null || lifecycle.signal === undefined) {
    return undefined;
  }
  return { signal: lifecycle.signal };
}

export function cancelled(error, lifecycle) {
  return (
    (lifecycle !== undefined && lifecycle !== null && lifecycle.signal.aborted) ||
    (error !== null && error !== undefined && error.name === "AbortError")
  );
}

/** Route-bound DOM listeners are removed when navigation aborts the lifecycle.
 *  Detached nodes cannot retain an actionable subscription after exit. */
export function listen(target, type, handler, lifecycle) {
  if (lifecycle !== undefined && lifecycle !== null && lifecycle.signal !== undefined) {
    target.addEventListener(type, handler, { signal: lifecycle.signal });
    return;
  }
  target.addEventListener(type, handler);
}
