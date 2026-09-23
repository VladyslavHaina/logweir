// fake-dom.js -- the smallest document `render.js`'s `enhanceDatagrids` can
// run in, so the datagrid's DOM behaviour is asserted under `node --test`
// (PLAT-18.2 review HIGH-1). Not a spec: the gate's glob does not collect it.
//
// It implements exactly what the enhancement touches -- elements with
// attributes, children and text, `hidden`/`disabled`/`value`, listeners and a
// dispatch, `tBodies`, `insertBefore`, focus -- and a selector matcher for the
// forms the enhancement uses: a tag, `#id`, `.class`, `[attr]`,
// `[attr="value"]`, any concatenation of those, one descendant space, and a
// comma list. A selector it cannot parse throws rather than guessing.

const VOID = new Set(["input", "br", "img", "meta", "link"]);

class Text {
  constructor(doc, text) {
    this.ownerDocument = doc;
    this.nodeType = 3;
    this.data = String(text);
    this.parentNode = null;
  }
  get textContent() {
    return this.data;
  }
  set textContent(value) {
    this.data = String(value);
  }
}

function parseCompound(one) {
  const out = { tag: null, id: null, classes: [], attrs: [] };
  const tag = /^[a-zA-Z][\w-]*/.exec(one);
  let rest = one;
  if (tag !== null) {
    out.tag = tag[0].toLowerCase();
    rest = one.slice(tag[0].length);
  }
  const pattern = /#([\w-]+)|\.([\w-]+)|\[([\w-]+)(?:="([^"]*)")?\]/g;
  let consumed = 0;
  let part;
  while ((part = pattern.exec(rest)) !== null) {
    consumed += part[0].length;
    if (part[1] !== undefined) {
      out.id = part[1];
    } else if (part[2] !== undefined) {
      out.classes.push(part[2]);
    } else {
      out.attrs.push([part[3], part[4]]);
    }
  }
  if (consumed !== rest.length) {
    throw new Error("fake-dom cannot parse the selector part `" + one + "`");
  }
  return out;
}

function matchesCompound(el, c) {
  if (c.tag !== null && el.tagName.toLowerCase() !== c.tag) {
    return false;
  }
  if (c.id !== null && el.getAttribute("id") !== c.id) {
    return false;
  }
  const classes = String(el.getAttribute("class") || "").split(/\s+/);
  if (c.classes.some((k) => classes.indexOf(k) === -1)) {
    return false;
  }
  return c.attrs.every(([name, value]) =>
    el.hasAttribute(name) && (value === undefined || el.getAttribute(name) === value));
}

export class Element {
  constructor(doc, tag) {
    this.ownerDocument = doc;
    this.nodeType = 1;
    this.tagName = tag.toUpperCase();
    this.attrs = new Map();
    this.childNodes = [];
    this.parentNode = null;
    this.listeners = new Map();
    this.hidden = false;
    this.disabled = false;
    this.value = "";
    this.checked = false;
  }
  get children() {
    return this.childNodes.filter((n) => n.nodeType === 1);
  }
  get firstChild() {
    return this.childNodes[0] || null;
  }
  get tBodies() {
    return this.children.filter((c) => c.tagName === "TBODY");
  }
  get textContent() {
    return this.childNodes.map((n) => n.textContent).join("");
  }
  set textContent(value) {
    for (const child of this.childNodes) {
      child.parentNode = null;
    }
    this.childNodes = [new Text(this.ownerDocument, value)];
    this.childNodes[0].parentNode = this;
  }
  setAttribute(name, value) {
    this.attrs.set(name, String(value));
  }
  getAttribute(name) {
    return this.attrs.has(name) ? this.attrs.get(name) : null;
  }
  hasAttribute(name) {
    return this.attrs.has(name);
  }
  removeAttribute(name) {
    this.attrs.delete(name);
  }
  appendChild(child) {
    if (child.parentNode !== null) {
      child.parentNode.removeChild(child);
    }
    this.childNodes.push(child);
    child.parentNode = this;
    return child;
  }
  removeChild(child) {
    const at = this.childNodes.indexOf(child);
    if (at !== -1) {
      this.childNodes.splice(at, 1);
      child.parentNode = null;
    }
    return child;
  }
  insertBefore(child, before) {
    if (before === null || before === undefined) {
      return this.appendChild(child);
    }
    if (child.parentNode !== null) {
      child.parentNode.removeChild(child);
    }
    this.childNodes.splice(this.childNodes.indexOf(before), 0, child);
    child.parentNode = this;
    return child;
  }
  contains(other) {
    for (let at = other; at; at = at.parentNode) {
      if (at === this) {
        return true;
      }
    }
    return false;
  }
  descendants() {
    const out = [];
    for (const child of this.children) {
      out.push(child);
      out.push(...child.descendants());
    }
    return out;
  }
  matches(selector) {
    return selector.split(",").map((s) => s.trim()).some((one) => {
      const parts = one.split(/\s+/).map(parseCompound);
      if (!matchesCompound(this, parts[parts.length - 1])) {
        return false;
      }
      let at = this.parentNode;
      for (let i = parts.length - 2; i >= 0; i -= 1) {
        while (at !== null && at.nodeType === 1 && !matchesCompound(at, parts[i])) {
          at = at.parentNode;
        }
        if (at === null || at.nodeType !== 1) {
          return false;
        }
        at = at.parentNode;
      }
      return true;
    });
  }
  querySelectorAll(selector) {
    return this.descendants().filter((el) => el.matches(selector));
  }
  querySelector(selector) {
    return this.querySelectorAll(selector)[0] || null;
  }
  closest(selector) {
    for (let at = this; at !== null && at.nodeType === 1; at = at.parentNode) {
      if (at.matches(selector)) {
        return at;
      }
    }
    return null;
  }
  addEventListener(type, fn) {
    if (!this.listeners.has(type)) {
      this.listeners.set(type, []);
    }
    this.listeners.get(type).push(fn);
  }
  dispatch(type, event) {
    for (const fn of this.listeners.get(type) || []) {
      fn(Object.assign({ type: type, target: this, preventDefault() {} }, event || {}));
    }
  }
  click() {
    if (!this.disabled) {
      this.dispatch("click");
    }
  }
  focus() {
    this.ownerDocument.activeElement = this;
  }
  /** Whether this element is shown: neither it nor an ancestor is hidden,
   *  and it is still attached under the document body. */
  shown() {
    for (let at = this; at !== null; at = at.parentNode) {
      if (at.hidden === true) {
        return false;
      }
      if (at === this.ownerDocument.body) {
        return true;
      }
    }
    return false;
  }
}

/** A document with a body and a `defaultView.location.hash` the test sets. */
export function fakeDocument(hash) {
  const doc = {
    activeElement: null,
    defaultView: { location: { hash: hash || "" } },
    createElement: (tag) => new Element(doc, tag),
    createTextNode: (text) => new Text(doc, text),
    getElementById: (id) => doc.body.querySelector("#" + id),
  };
  doc.body = new Element(doc, "body");
  return doc;
}

/** Builds elements from a nested `[tag, attrs, ...children]` description; a
 *  string child is text. Enough to write a table or a list in a test. */
export function build(doc, spec) {
  if (typeof spec === "string") {
    return new Text(doc, spec);
  }
  const [tag, attrs, ...children] = spec;
  const el = new Element(doc, tag);
  for (const key of Object.keys(attrs || {})) {
    el.setAttribute(key, attrs[key]);
  }
  if (!VOID.has(tag)) {
    for (const child of children) {
      el.appendChild(build(doc, child));
    }
  }
  return el;
}
