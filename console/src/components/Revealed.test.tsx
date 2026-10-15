// DUI-33: a refusal that appears after a submit is scrolled into view and takes focus,
// once per refusal: the shared write alerts (connections' WriteOutcome, schedules'
// WriteAlert) do it for every form that renders them.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { useState } from "react";
import { ApiError } from "../api/client";
import { WriteOutcome } from "../pages/connections/parts";
import { WriteAlert } from "../pages/schedules/parts";

const scrolled = vi.fn();
beforeEach(() => {
  scrolled.mockClear();
  Element.prototype.scrollIntoView = scrolled;
});
afterEach(() => {
  cleanup();
  delete (Element.prototype as { scrollIntoView?: unknown }).scrollIntoView;
});

const refusal = () =>
  new ApiError(409, { status: 409, code: "state_conflict" as never, detail: "`primary` is already this namespace's default destination.", requestId: "r", retryable: false });

/** A form's foot: a submit that fails anew on every click, and a re-render that changes nothing. */
function Form({ alert }: { alert: "outcome" | "schedules" }) {
  const [error, setError] = useState<unknown>(null);
  const [, setTick] = useState(0);
  return (
    <div>
      <button onClick={() => setError(refusal())}>Submit</button>
      <button onClick={() => setTick((n) => n + 1)}>Type</button>
      {error !== null &&
        (alert === "outcome" ? (
          <WriteOutcome error={error} subject="the destination" credentials unmatched={[]} />
        ) : (
          <WriteAlert error={error} kind="create" subject="the schedule" verb="created" />
        ))}
    </div>
  );
}

describe("DUI-33: a refusal is brought to the reader", () => {
  for (const alert of ["outcome", "schedules"] as const) {
    it(`${alert}: scrolls the refusal into view and focuses it, once per refusal`, () => {
      render(<Form alert={alert} />);
      // Control: nothing is moved before a submit fails.
      expect(scrolled).not.toHaveBeenCalled();
      fireEvent.click(screen.getByRole("button", { name: "Submit" }));
      const focused = document.activeElement as HTMLElement;
      expect(focused.className).toBe("lw-revealed");
      expect(focused.textContent).toContain("is already this namespace's default destination");
      expect(focused.getAttribute("tabindex")).toBe("-1");
      expect(scrolled).toHaveBeenCalledTimes(1);
      expect(scrolled).toHaveBeenCalledWith({ block: "nearest" });
      // Control: a re-render with the same refusal moves nothing.
      fireEvent.click(screen.getByRole("button", { name: "Type" }));
      expect(scrolled).toHaveBeenCalledTimes(1);
      // A new refusal is brought to the reader again.
      fireEvent.click(screen.getByRole("button", { name: "Submit" }));
      expect(scrolled).toHaveBeenCalledTimes(2);
    });
  }
});
