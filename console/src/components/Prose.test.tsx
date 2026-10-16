// DUI-11: the API's problem details write names as Markdown code spans; the page
// shows them as code, never as raw backticks, and never parses HTML.
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render } from "@testing-library/react";
import { ApiError } from "../api/client";
import { ErrorText, Prose, checkMessage, plainInstants, proseSegments } from "./Prose";

afterEach(cleanup);

describe("problem details as prose", () => {
  it("renders each balanced code span as code, without its backticks", () => {
    const { container } = render(<Prose text="`primary` is already this workspace's default destination. Clear it there first." />);
    expect(container.querySelector("code")?.textContent).toBe("primary");
    expect(container.textContent).toBe("primary is already this workspace's default destination. Clear it there first.");
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

describe("DUI-22: a check row's machine suffix", () => {
  it("is said in words, with the rest of the count", () => {
    expect(checkMessage('the mapped topics exist on the target [detail: {"count":4,"sample":["dui2-a-orders","dui2-a-payments"]}]')).toBe(
      "the mapped topics exist on the target (dui2-a-orders, dui2-a-payments, and 2 more)",
    );
  });
  it("is left as it came when it cannot be read, and a message without one is unchanged (the controls)", () => {
    expect(checkMessage("x [detail: not json]")).toBe("x [detail: not json]");
    expect(checkMessage("plain words")).toBe("plain words");
  });
});

describe("DUI-22: a server sentence's instants", () => {
  it("are written as the console writes an instant, nanoseconds and offset included", () => {
    expect(plainInstants("waits for a second person's approval until 2026-10-14T14:06:44.284150630+00:00. It expires in 15 min")).toBe(
      "waits for a second person's approval until 2026-10-14 14:06 UTC. It expires in 15 min",
    );
    expect(plainInstants("at 2026-10-14T16:06:44+02:00")).toBe("at 2026-10-14 14:06 UTC");
  });
  it("leave a code span and a sentence without one alone (the controls)", () => {
    expect(plainInstants("no instant here")).toBe("no instant here");
    const { container } = render(<Prose text="`2026-10-14T14:06:44Z` and 2026-10-14T14:06:44Z" />);
    expect(container.textContent).toBe("2026-10-14T14:06:44Z and 2026-10-14 14:06 UTC");
  });
});
