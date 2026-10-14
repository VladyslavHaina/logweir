// DUI-40.6: a count and its noun agree, in every duration this console words.
import { describe, expect, it } from "vitest";
import { counted, duration, msValue } from "./When";

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
