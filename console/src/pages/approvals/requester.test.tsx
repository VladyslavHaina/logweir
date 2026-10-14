// DUI-21: a second person sees who asked: the identity provider's name, the short
// subject beside it, and the signed principal whole for the audit. A request made
// before names were recorded shows the principal in full, as before.
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import type { ApprovalGrant } from "../../api/approvals";
import { Requester } from "./parts";

afterEach(cleanup);

const DEX = "https://dex.localtest.me#CiQxZjBjOWE0ZS0wMTA2LTRhMWUtOWUxYS0wMDAwMDAwMDAxMDYSBWxvY2Fs";
const bob = { known: true, approvalsRead: true, approvalSubmit: true, approvalPacketRead: true, restoresRead: true, actorId: "https://dex.localtest.me#bob", displayName: "Bob" } as ApprovalGrant;

describe("who asked", () => {
  it("reads the name, the short subject, and keeps the signed principal for the audit", () => {
    const { container } = render(<Requester id={DEX} name="Alice Martin" grant={bob} />);
    expect(container.textContent).toMatch(/^Alice Martin \(CiQxZjBjOW…BWxvY2Fs\)/);
    expect(screen.getByTitle(DEX)).toBeTruthy();
    expect(container.querySelector("code")?.textContent).toBe(DEX);
    expect(container.textContent).toContain("not part of the signed request");
  });

  it("shows an older request's principal in full, with no name invented (the control)", () => {
    const { container } = render(<Requester id={DEX} name={undefined} grant={bob} />);
    expect(container.textContent).toBe(DEX);
    expect(container.textContent).not.toContain("Alice");
  });

  it("still says it is you when it is", () => {
    const { container } = render(<Requester id={bob.actorId} name="Bob Novak" grant={bob} />);
    expect(container.textContent).toMatch(/^you \(Bob\) · Bob Novak/);
  });
});
