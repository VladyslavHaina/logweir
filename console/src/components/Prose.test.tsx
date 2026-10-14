// DUI-11: the API's problem details write names as Markdown code spans; the page
// shows them as code, never as raw backticks, and never parses HTML.
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render } from "@testing-library/react";
import { ApiError } from "../api/client";
import { ErrorText, Prose, proseSegments } from "./Prose";

afterEach(cleanup);

describe("problem details as prose", () => {
  it("renders each balanced code span as code, without its backticks", () => {
    const { container } = render(<Prose text="`primary` is already this namespace's default destination. Clear it there first." />);
    expect(container.querySelector("code")?.textContent).toBe("primary");
    expect(container.textContent).toBe("primary is already this namespace's default destination. Clear it there first.");
  });

  it("shows an unpaired backtick as it came, and HTML as text", () => {
    // The control: an odd number of backticks is not a code span.
    expect(proseSegments("one ` alone")).toEqual([{ text: "one ` alone", code: false }]);
    const { container } = render(<Prose text="`<b>x</b>` and <i>y</i>" />);
    expect(container.querySelector("b, i")).toBeNull();
    expect(container.textContent).toBe("<b>x</b> and <i>y</i>");
  });

  it("reads an API error's message", () => {
    const { container } = render(<ErrorText error={new ApiError(409, { code: "state_conflict", detail: "`primary` holds the flag." } as never)} />);
    expect(container.innerHTML).toContain("<code>primary</code>");
  });
});
