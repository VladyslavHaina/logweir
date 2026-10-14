// The phone view of a list: one row per item with its name, its status and one
// line of context. Everything else is on the item's own page.
import { useEffect, useState, type ReactNode } from "react";

/** True while the viewport is phone-sized (PatternFly's md breakpoint). */
export function useNarrow(maxWidth = 767): boolean {
  const query = `(max-width: ${maxWidth}px)`;
  const [narrow, setNarrow] = useState(() => window.matchMedia?.(query).matches ?? false);
  useEffect(() => {
    const mq = window.matchMedia?.(query);
    if (!mq) return undefined;
    const onChange = () => setNarrow(mq.matches);
    mq.addEventListener("change", onChange);
    return () => mq.removeEventListener("change", onChange);
  }, [query]);
  return narrow;
}

export interface CompactItem {
  key: string;
  /** The item's own page; without it the row is plain text. */
  href?: string;
  title: string;
  meta: ReactNode;
  status: ReactNode;
}

export function CompactList({ label, items, flush = false }: { label: string; items: CompactItem[]; flush?: boolean }) {
  return (
    <ul className={flush ? "lw-list lw-list--flush" : "lw-list"} aria-label={label}>
      {items.map((item) => (
        <li key={item.key}>
          <div className="lw-list__row">
            <div className="lw-list__main">
              {item.href ? (
                <a href={item.href} className="lw-list__title" title={item.title}>
                  {item.title}
                </a>
              ) : (
                <span className="lw-list__title">{item.title}</span>
              )}
              <span className="lw-list__meta">{item.meta}</span>
            </div>
            <div className="lw-list__end">{item.status}</div>
          </div>
        </li>
      ))}
    </ul>
  );
}
