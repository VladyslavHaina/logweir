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
      {proseSegments(text).map((s, i) => (s.code ? <code key={i}>{s.text}</code> : <span key={i}>{s.text}</span>))}
    </span>
  );
}

/** An error's message as prose: `ApiError` and every other `Error` carry one; anything else is stringified. */
export function ErrorText({ error }: { error: unknown }) {
  return <Prose text={error instanceof Error ? error.message : String(error)} />;
}
