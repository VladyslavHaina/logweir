// DUI-40.6: a count and its noun agree, in every duration this console words.
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render } from "@testing-library/react";
import { When, counted, duration, msValue } from "./When";

describe("DUI-40.6: singular and plural", () => {
  it("says one day, not one days, and seven days (the control)", () => {
    expect(msValue("delete.retention.ms", "86400000")).toBe("86400000 (1 day)");
    expect(msValue("retention.ms", "604800000")).toBe("604800000 (7 days)");
    expect(counted(1, "point")).toBe("1 point");
    expect(counted(2, "day")).toBe("2 days");
    expect(counted(1_000, "point")).toBe("1,000 points");
    // A duration's units are abbreviations or always plural (it says days from two upward).
    expect([duration(45), duration(2 * 3600), duration(2 * 86_400)]).toEqual(["45 s", "2 h", "2 days"]);
  });
});

describe("DUI-41.1: an instant recorded after the page's last tick", () => {
  afterEach(() => {
    cleanup();
    vi.useRealTimers();
  });

  it("reads as past, never \"in a moment\"; a real future instant still does (the control)", () => {
    vi.useFakeTimers();
    const t0 = Date.parse("2026-10-14T22:35:00Z");
    vi.setSystemTime(t0);
    const { container, rerender } = render(<When value={new Date(t0 - 60_000).toISOString()} />);
    // Twenty seconds pass without a tick; the verdict landed seventeen seconds ago.
    vi.setSystemTime(t0 + 20_000);
    rerender(<When value={new Date(t0 + 3_000).toISOString()} />);
    expect(container.textContent).toBe("just now");
    rerender(<When value={new Date(t0 + 20_000 + 10 * 60_000).toISOString()} />);
    expect(container.textContent).toBe("in 10 mins".replace("mins", "min"));
  });
});
