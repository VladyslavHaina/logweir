// The product API writes names in its problem details as Markdown code spans
// ("`primary` is already this namespace's default destination"). Shown verbatim
// the backticks read as noise, so each balanced span is a <code> element: text
// nodes only, never parsed HTML.

/** The text's segments: odd indexes were inside a balanced pair of backticks. */
export function proseSegments(text: string): { text: string; code: boolean }[] {
  const parts = text.split("`");
  // An odd number of backticks leaves one unpaired: the text is shown as it came.
  if (parts.length % 2 === 0) return [{ text, code: false }];
  return parts.map((part, i) => ({ text: i % 2 === 1 ? part.trim() : part, code: i % 2 === 1 })).filter((s) => s.code || s.text !== "");
}

export function Prose({ text }: { text: string }) {
  return (
    <span className="lw-prose">
      {proseSegments(text).map((s, i) => (s.code ? <code key={i}>{s.text}</code> : <span key={i}>{plainInstants(s.text)}</span>))}
    </span>
  );
}

/**
 * A check row's message with its machine suffix ` [detail: {"count":N,"sample":[…]}]`
 * said in words (DUI-22): "… (orders, payments, and 3 more)". A suffix this page cannot
 * read is left as it came.
 */
export function checkMessage(text: string): string {
  const at = text.lastIndexOf(" [detail: ");
  if (at < 0 || !text.endsWith("]")) return text;
  try {
    const d = JSON.parse(text.slice(at + " [detail: ".length, -1)) as { count?: unknown; sample?: unknown };
    const count = typeof d.count === "number" && Number.isInteger(d.count) ? d.count : null;
    const sample = Array.isArray(d.sample) ? d.sample.filter((s): s is string => typeof s === "string") : [];
    if (count === null && sample.length === 0) return text;
    const more = count !== null && count > sample.length ? `, and ${count - sample.length} more` : "";
    return `${text.slice(0, at)} (${sample.length ? `${sample.join(", ")}${more}` : String(count)})`;
  } catch {
    return text;
  }
}

const RFC3339 = /\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})/g;

/**
 * DUI-22: the API's sentences carry raw RFC 3339 instants, to the nanosecond
 * ("2026-10-14T14:06:44.284150630+00:00"). Each is written as the console writes an
 * instant, in UTC to the minute; one this page cannot read is left as it came.
 */
export function plainInstants(text: string): string {
  return text.replace(RFC3339, (raw) => {
    // Date reads milliseconds at most: the digits past them change nothing at minute precision.
    const ms = Date.parse(raw.replace(/(\.\d{3})\d+/, "$1"));
    return Number.isNaN(ms) ? raw : new Date(ms).toISOString().slice(0, 16).replace("T", " ") + " UTC";
  });
}

/** An error's message as prose: `ApiError` and every other `Error` carry one; anything else is stringified. */
export function ErrorText({ error }: { error: unknown }) {
  return <Prose text={error instanceof Error ? error.message : String(error)} />;
}
