// The fake DOM the schedules page's mount-half rows drive: a node that keeps
// the markup the page adopted and answers selectors from it. Moved here from
// `d1.spec.js` so the schedule-detail rows (`schedules-detail.spec.js`) drive
// the SAME fake rather than a second one that could disagree with it.
//
// Not a spec: `node --test 'ui/tests/*.spec.js'` does not collect it.

export function attributesOf(tag) {
  const out = Object.create(null);
  const pattern = /([a-zA-Z][\w-]*)(?:="([^"]*)")?/g;
  const body = tag.replace(/^<\w+/, "").replace(/>$/, "");
  let match;
  while ((match = pattern.exec(body)) !== null) {
    out[match[1]] = match[2] === undefined
      ? ""
      : match[2].replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, "\"")
        .replace(/&#39;/g, "'").replace(/&amp;/g, "&");
  }
  return out;
}

/** `tag`, `#id`, `.class`, `[attr="value"]` and any concatenation of them,
 *  plus a comma list. Enough for every selector the schedules page uses, and
 *  no more: a matcher that guessed would make a row pass for the wrong reason. */
export function matches(element, selector) {
  for (const one of selector.split(",").map((t) => t.trim())) {
    const tag = /^[a-zA-Z][\w-]*/.exec(one);
    if (tag !== null && element.tagName !== tag[0].toUpperCase()) {
      continue;
    }
    let ok = true;
    const rest = one.slice(tag === null ? 0 : tag[0].length);
    const pattern = /#([\w-]+)|\.([\w-]+)|\[([\w-]+)="([^"]*)"\]/g;
    let part;
    while ((part = pattern.exec(rest)) !== null) {
      if (part[1] !== undefined && element.attributes.id !== part[1]) {
        ok = false;
      } else if (part[2] !== undefined &&
        String(element.attributes.class || "").split(/\s+/).indexOf(part[2]) === -1) {
        ok = false;
      } else if (part[3] !== undefined && element.attributes[part[3]] !== part[4]) {
        ok = false;
      }
    }
    if (ok) {
      return true;
    }
  }
  return false;
}

export class Fake {
  constructor(tag, attributes, view) {
    this.tagName = tag.toUpperCase();
    this.attributes = attributes;
    this.view = view;
    this.children = [];
    this.listeners = [];
    this.value = "";
    this.checked = false;
    this.disabled = "disabled" in attributes;
  }
  get firstChild() { return this.children.length === 0 ? null : this.children[0]; }
  removeChild(child) { this.children.splice(this.children.indexOf(child), 1); return child; }
  appendChild(child) {
    this.children.push(child);
    if (child && typeof child.html === "string") {
      this.view.adopt(child.html, this.isRoot === true, this);
    }
    return child;
  }
  addEventListener(type, handler) { this.listeners.push({ type: type, handler: handler }); }
  setAttribute(name, value) { this.attributes[name] = String(value); }
  getAttribute(name) { return name in this.attributes ? this.attributes[name] : null; }
  focus() {}
  querySelector(selector) { return this.view.find(selector); }
  querySelectorAll(selector) { return this.view.findAll(selector); }
  async dispatch(type) {
    for (const entry of this.listeners.slice()) {
      if (entry.type === type) {
        await entry.handler({ preventDefault() {} });
      }
    }
  }
}

export function fakeView() {
  const view = {
    chunks: [],
    adopt(html, fromRoot, owner) {
      if (fromRoot) {
        view.chunks = [];
      } else if (owner !== undefined && owner.slotOf !== undefined) {
        // A slot replace supersedes whatever that slot held before.
        view.chunks = view.chunks.filter((c) => c.slot !== owner.slotOf);
      }
      view.chunks.push({
        html: html, elements: null, slot: owner === undefined ? null : (owner.slotOf || null),
      });
    },
    html() { return view.chunks.map((c) => c.html).join(""); },
    elementsOf(chunk) {
      if (chunk.elements !== null) {
        return chunk.elements;
      }
      const elements = [];
      const tag = /<(input|select|button|form|div|section|p)\b[^>]*>/g;
      let match;
      while ((match = tag.exec(chunk.html)) !== null) {
        const element = new Fake(match[1], attributesOf(match[0]), view);
        element.start = match.index;
        if (match[1] === "input") {
          element.value = element.attributes.value || "";
          element.checked = "checked" in element.attributes;
        } else if (match[1] === "select") {
          const end = chunk.html.indexOf("</select>", match.index);
          const chosen = /<option value="([^"]*)" selected>/.exec(chunk.html.slice(match.index, end)) ||
            /<option value="([^"]*)"/.exec(chunk.html.slice(match.index, end));
          element.value = chosen === null ? "" : chosen[1];
        } else if (match[1] === "form") {
          element.end = chunk.html.indexOf("</form>", match.index);
        } else if (match[1] === "div" &&
          (element.attributes["data-policy-slot"] || element.attributes["data-run-now-slot"])) {
          element.slotOf = element.attributes["data-policy-slot"] === undefined
            ? "run:" + element.attributes["data-run-now-slot"]
            : "policy:" + element.attributes["data-policy-slot"];
        }
        elements.push(element);
      }
      for (const form of elements.filter((e) => e.tagName === "FORM")) {
        const inside = elements.filter((e) => e.start > form.start && e.start < form.end);
        form.elements = Object.create(null);
        for (const control of inside) {
          if (["INPUT", "SELECT"].indexOf(control.tagName) !== -1 && control.attributes.name) {
            form.elements[control.attributes.name] = control;
          }
        }
        form.querySelectorAll = (selector) => inside.filter((e) => matches(e, selector));
      }
      chunk.elements = elements;
      return elements;
    },
    find(selector) {
      for (let i = view.chunks.length - 1; i >= 0; i -= 1) {
        const found = view.elementsOf(view.chunks[i]).find((e) => matches(e, selector));
        if (found !== undefined) {
          return found;
        }
      }
      return null;
    },
    findAll(selector) {
      const all = [];
      for (const chunk of view.chunks) {
        for (const element of view.elementsOf(chunk)) {
          if (matches(element, selector)) {
            all.push(element);
          }
        }
      }
      return all;
    },
  };
  const root = new Fake("main", Object.create(null), view);
  root.isRoot = true;
  view.root = root;
  return view;
}

export const parse = (html) => [{ html: html }];
// The route token `createRouteLifecycle` hands a mount: a signal and the
// question "is this still the current route". Built by hand here rather than
// imported so these rows carry no navigation of their own.
export const LIFE = () => ({ generation: 1, signal: undefined, isCurrent: () => true });
