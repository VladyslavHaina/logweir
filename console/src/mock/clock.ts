// The prototype's clock: every mock instant is relative to the moment the page
// loaded, rounded down to the minute, so the demo always looks current.
export const LOADED_AT = Date.now();
export const NOW = Math.floor(LOADED_AT / 60_000) * 60_000;

export const MINUTE = 60_000;
export const HOUR = 60 * MINUTE;
export const DAY = 24 * HOUR;

export function iso(ms: number): string {
  return new Date(ms).toISOString().replace(/\.\d{3}Z$/, "Z");
}

/** Seconds since the page loaded: the mock's runs move forward with it. */
export function elapsedSeconds(): number {
  return (Date.now() - LOADED_AT) / 1000;
}

/** The start of the hour `hoursBack` hours before NOW. */
export function slot(hoursBack: number): number {
  return Math.floor(NOW / HOUR) * HOUR - hoursBack * HOUR;
}

export function slotName(ms: number): string {
  const d = new Date(ms);
  const p = (n: number) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}${p(d.getUTCMonth() + 1)}${p(d.getUTCDate())}-${p(d.getUTCHours())}${p(d.getUTCMinutes())}00`;
}
