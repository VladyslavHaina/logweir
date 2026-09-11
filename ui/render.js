// render.js -- DOM helpers. This module issues no network request of any kind;
// `api.js` is the only module in this tree that does.
//
// WHAT IS DELIBERATELY NOT HERE. There is no function in this file that
// decides whether an object verified, and there must not be. The verdict a
// page shows is read from the object's own status -- weirkeeper writes
// `status.evidence.verification` and a `Verified` condition, and the badge
// rules are a conjunction over those recorded fields and the run's own outcome.
// A helper here that took a guess at that conjunction, or that defaulted a
// missing field to a favourable answer, would be this page inventing a claim
// about signed evidence it never read. The shell renders no verdict at all;
// the pages that render one arrive with their own tests.
//
// Everything below is structural: it builds elements and sets TEXT. Nothing
// here writes `innerHTML`, so a name, a reason or an API server message drawn
// from the cluster is inserted as text and can never become markup.

/** Creates an element. `attrs` are set as attributes; `children` may be
 *  strings (inserted as text) or nodes. */
export function el(tag, attrs, children) {
  const node = document.createElement(tag);
  if (attrs) {
    for (const key of Object.keys(attrs)) {
      const value = attrs[key];
      if (value !== null && value !== undefined) {
        node.setAttribute(key, String(value));
      }
    }
  }
  append(node, children);
  return node;
}

/** Appends children to a node. A string child becomes a text node -- never
 *  markup. */
export function append(node, children) {
  if (children === null || children === undefined) {
    return node;
  }
  const list = Array.isArray(children) ? children : [children];
  for (const child of list) {
    if (child === null || child === undefined) {
      continue;
    }
    if (typeof child === "string" || typeof child === "number") {
      node.appendChild(document.createTextNode(String(child)));
    } else {
      node.appendChild(child);
    }
  }
  return node;
}

/** Empties a node. */
export function clear(node) {
  while (node.firstChild !== null) {
    node.removeChild(node.firstChild);
  }
  return node;
}

/** Replaces a node's children in one step. */
export function replace(node, children) {
  return append(clear(node), children);
}

/** Renders an error from `api.js` as the API server reported it: its own
 *  status code, its own `reason`, its own `message`, and nothing added.
 *
 *  A 403 here is the API server's 403 about the viewer's own RBAC. This
 *  function neither softens it nor explains it away. */
export function errorBox(error) {
  const status = error && error.status ? String(error.status) : "error";
  const reason = error && error.reason ? String(error.reason) : "";
  const message = error && error.message ? String(error.message) : String(error);
  return el("div", { class: "error" }, [
    el("span", { class: "error-status" }, reason.length > 0 ? status + " " + reason : status),
    el("p", { class: "error-message" }, message),
  ]);
}
