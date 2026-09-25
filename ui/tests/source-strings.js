// source-strings.js -- the string literals of a shipped source file, for the
// suites that lint what an operator reads: `console-ux.spec.js` (MCP-24, no
// internal roadmap reference) and `code-spans.spec.js` (O2, no literal
// backtick). Moved here from `console-ux.spec.js` so both read the SAME lexer.
// Not a spec: the gate's glob does not collect it.

/** The string literals of a source file -- `"..."`, `'...'` and template
 *  literals, a template across as many lines as it spans -- with the line each
 *  starts on; comments and regular-expression literals are skipped (review
 *  L2: a per-line scan lost a multi-line template and was thrown by a quote
 *  inside a regex). For the shell (`html`), every line with its HTML comments
 *  removed, TAGS INCLUDED, so an attribute -- a `title`, an `aria-label` -- is
 *  scanned as well as the text between tags (review L2). */
export function quotedSegments(text, html) {
  const out = [];
  const source = String(text);
  if (html) {
    const stripped = source.replace(/<!--[\s\S]*?-->/g, (m) => m.replace(/[^\n]/g, " "));
    stripped.split("\n").forEach((line, i) => {
      if (line.trim().length > 0) {
        out.push({ line: i + 1, text: line });
      }
    });
    return out;
  }
  let line = 1;
  let last = "";
  // The characters after which a `/` opens a regular expression, not a division.
  const REGEX_AFTER = "(,=:[!&|?{};+-*%<>~^";
  for (let k = 0; k < source.length; k += 1) {
    const c = source[k];
    const n = source[k + 1];
    if (c === "\n") {
      line += 1;
      continue;
    }
    if (c === "/" && n === "/") {
      while (k < source.length && source[k] !== "\n") {
        k += 1;
      }
      k -= 1;
      continue;
    }
    if (c === "/" && n === "*") {
      const end = source.indexOf("*/", k + 2);
      const stop = end === -1 ? source.length : end + 2;
      for (let m = k; m < stop; m += 1) {
        if (source[m] === "\n") {
          line += 1;
        }
      }
      k = stop - 1;
      continue;
    }
    if (c === "/" && (last === "" || REGEX_AFTER.indexOf(last) !== -1 ||
      /\b(?:return|typeof|case)$/.test(source.slice(Math.max(0, k - 8), k).trimEnd()))) {
      let inClass = false;
      for (k += 1; k < source.length; k += 1) {
        const r = source[k];
        if (r === "\\") {
          k += 1;
        } else if (r === "[") {
          inClass = true;
        } else if (r === "]") {
          inClass = false;
        } else if (r === "/" && !inClass) {
          break;
        } else if (r === "\n") {
          line += 1;
          break;
        }
      }
      last = "/";
      continue;
    }
    if (c === "\"" || c === "'" || c === "`") {
      const startLine = line;
      let segment = "";
      for (k += 1; k < source.length && source[k] !== c; k += 1) {
        if (source[k] === "\\") {
          segment += source.slice(k, k + 2);
          k += 1;
          continue;
        }
        if (source[k] === "\n") {
          line += 1;
          if (c !== "`") {
            break;
          }
        }
        segment += source[k];
      }
      out.push({ line: startLine, text: segment });
      last = c;
      continue;
    }
    if (!/\s/.test(c)) {
      last = c;
    }
  }
  return out;
}
