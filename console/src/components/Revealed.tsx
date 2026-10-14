// DUI-33: a refusal that appears after a submit is brought to the reader. In a long
// dialog it lands below the fold, so the click seemed to do nothing; and a screen
// reader heard nothing of it. Each new refusal is scrolled into view and focused,
// once: a re-render of the same one moves nothing.
import { useEffect, useRef, type ReactNode } from "react";

/**
 * Wraps the alert a failed submit renders. `when` is what changed: the error object
 * of the submit (a new one per attempt). The wrapper takes focus (tabIndex -1, so it
 * is not a tab stop) and its alert is read from there.
 */
export function Revealed({ when, children }: { when: unknown; children: ReactNode }) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const el = ref.current;
    if (!el || when === null || when === undefined || when === false) return;
    // jsdom and some older engines have no scrollIntoView; focus alone still moves the reader.
    el.scrollIntoView?.({ block: "nearest" });
    el.focus({ preventScroll: true });
  }, [when]);
  return (
    <div ref={ref} tabIndex={-1} className="lw-revealed">
      {children}
    </div>
  );
}
