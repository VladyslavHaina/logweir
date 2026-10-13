// Instants: a relative phrase for scanning, the exact UTC instant on hover and
// in the <time> element, and `exact` for detail pages.
import { useEffect, useState } from "react";

export function exactUtc(value: string | number): string {
  const d = new Date(value);
  return d.toISOString().replace("T", " ").replace(/:\d{2}\.\d{3}Z$/, " UTC");
}

export function relative(value: string | number, now = Date.now()): string {
  const ms = new Date(value).getTime() - now;
  const abs = Math.abs(ms);
  const future = ms > 0;
  const minutes = Math.round(abs / 60_000);
  const say = (n: number, unit: string) => {
    const text = `${n} ${unit}${n === 1 ? "" : "s"}`;
    return future ? `in ${text}` : `${text} ago`;
  };
  if (abs < 45_000) return future ? "in a moment" : "just now";
  if (minutes < 60) return say(minutes, "min").replace("mins", "min");
  const hours = Math.round(abs / 3_600_000);
  if (hours < 24) return say(hours, "hour");
  return say(Math.round(abs / 86_400_000), "day");
}

/** Re-renders every 30 s so "2 min ago" stays true while the page is open. */
function useMinuteTick(): number {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const id = window.setInterval(() => setNow(Date.now()), 30_000);
    return () => window.clearInterval(id);
  }, []);
  return now;
}

export function When({ value, exact = false }: { value: string | number | null | undefined; exact?: boolean }) {
  const now = useMinuteTick();
  if (value === null || value === undefined || value === "") return <span className="lw-subtle">Not recorded</span>;
  const iso = new Date(value).toISOString();
  return (
    <time dateTime={iso} title={exactUtc(value)}>
      {exact ? `${exactUtc(value)} (${relative(value, now)})` : relative(value, now)}
    </time>
  );
}

export function duration(seconds: number): string {
  if (seconds < 90) return `${Math.round(seconds)} s`;
  if (seconds < 5400) return `${Math.round(seconds / 60)} min`;
  if (seconds < 172_800) return `${Math.round(seconds / 3600)} h`;
  return `${Math.round(seconds / 86_400)} days`;
}

export function count(n: number | null | undefined): string {
  return typeof n === "number" ? n.toLocaleString("en-US") : "Not recorded";
}
