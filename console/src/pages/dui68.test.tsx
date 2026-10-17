// @vitest-environment jsdom
// DUI-68 (the PoC re-check's LOW slips) that have a cheap row here. 68.2 is in
// restores/model.test.ts, 68.6 in restores/flows.test.tsx, 68.4 in weirkeeper's
// restore_controller.rs. 68.1's layout (two even halves at 390 px, where `text-wrap: balance`
// under `word-break: break-all` left "…pihn / 5gr") is Chromium's, measured on the built
// console; jsdom has no layout, so this row holds the markup that layout relies on.
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render } from "@testing-library/react";
import { TokenTitle } from "./restores/RestoreDetail";
import { interruptionCause } from "./restores/words";

afterEach(cleanup);

describe("DUI-68.1: a restore's name as a title", () => {
  it("has one break opportunity, in its middle", () => {
    const { container } = render(<TokenTitle value="rst-gbcrxesmdxylpcs2rmtpihn5gr" />);
    const span = container.querySelector(".lw-token-title")!;
    expect(span.textContent).toBe("rst-gbcrxesmdxylpcs2rmtpihn5gr");
    expect(span.querySelectorAll("wbr")).toHaveLength(1);
    expect(Array.from(span.childNodes, (n) => n.textContent)).toEqual(["rst-gbcrxesmdxy", "", "lpcs2rmtpihn5gr"]);
  });
});

describe("DUI-68.3: who cancelled an interrupted restore", () => {
  // The PoC's v-dev-op, as the API's annotation names it: `<issuer>#<Dex subject>`.
  const BY = "https://dex.localtest.me#CiQxZjBjOWE0ZS0wNTAyLTRhMWUtOWUxYS0wMDAwMDAwMDA1MDISBWxvY2Fs";
  it("is shortened as the user menu shortens a subject", () => {
    expect(interruptionCause("Cancelled", BY)).toBe("It was cancelled by …000000000502@local.");
    // Controls: another cause names nobody; a short subject is kept whole.
    expect(interruptionCause("RunnerStopped", BY)).not.toContain("dex");
    expect(interruptionCause("Cancelled", "admin")).toBe("It was cancelled by admin.");
  });
});
