// DUI-21, after its security review: "Requested by" leads with the VERIFIED identifier
// (the email the identity provider verified, or the signed principal in full), a name
// from an opted-in claim follows labelled unverified and never bold, the signed
// principal stays visible, and "you" is decided on the signed principal alone.
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render } from "@testing-library/react";
import type { ApprovalGrant } from "../../api/approvals";
import { ApprovalStatus, DETAILS_NOT_VERIFIED, Requester, UNVERIFIED_NAME, principalText } from "./parts";

afterEach(cleanup);

const DEX = "https://dex.localtest.me#CiQxZjBjOWE0ZS0wMTA2LTRhMWUtOWUxYS0wMDAwMDAwMDAxMDYSBWxvY2Fs";
const grant = (actorId: string, displayName: string) =>
  ({ known: true, approvalsRead: true, approvalSubmit: true, approvalPacketRead: true, restoresRead: true, actorId, displayName }) as ApprovalGrant;
const bob = grant("https://dex.localtest.me#bob", "Bob");

describe("who asked", () => {
  it("orders the verified email, then the unverified name, then the signed principal", () => {
    const { container } = render(<Requester id={DEX} email="alice@example.com" name="The CEO" grant={bob} />);
    const text = container.textContent ?? "";
    const at = ["alice@example.com", `The CEO ${UNVERIFIED_NAME}`, `Signed as ${DEX}`].map((t) => text.indexOf(t));
    expect(at[0]).toBeGreaterThanOrEqual(0);
    expect(at).toEqual([...at].sort((x, y) => x - y));
    expect(container.querySelector("[data-verified-identifier]")?.textContent).toBe("alice@example.com");
    // The self-asserted name is never bold and never the identifier.
    expect(container.querySelector("strong")).toBeNull();
    expect(container.querySelector("[data-unverified-name]")?.textContent).toBe(`The CEO ${UNVERIFIED_NAME}`);
  });

  it("without a verified email leads with the signed principal in full, never shortened", () => {
    const { container } = render(<Requester id={DEX} name="The CEO" grant={bob} />);
    expect(container.querySelector("[data-verified-identifier]")?.textContent).toBe(DEX);
    expect((container.textContent ?? "").indexOf(DEX)).toBe(0);
    expect(container.textContent).toContain(UNVERIFIED_NAME);
  });

  it("shows an older request's principal in full, with no name invented (the control)", () => {
    const { container } = render(<Requester id={DEX} grant={bob} />);
    expect(container.textContent).toBe(DEX);
  });

  it("decides 'you' on the signed principal, never on a matching name", () => {
    // A requester who set their name to the viewer's is not the viewer.
    const { container } = render(<Requester id={DEX} name="Bob" grant={bob} />);
    expect(container.textContent).not.toMatch(/you/);
    cleanup();
    const mine = render(<Requester id={bob.actorId} email="bob@example.com" grant={bob} />);
    expect(mine.container.textContent).toMatch(/^you \(Bob\) · bob@example\.com/);
  });

  it("lists a principal in full", () => {
    expect(principalText(DEX, bob)).toBe(DEX);
    expect(principalText(bob.actorId, bob)).toBe(`you (Bob) · ${bob.actorId}`);
  });
});

describe("DUI-21, second review: details the API could not verify", () => {
  it("leads with the signed principal in full and says the details are not shown", () => {
    // The API served no email or name, and flagged them: a tampered or copied display document.
    const { container } = render(<Requester id={DEX} detailsNotVerified grant={bob} />);
    expect(container.querySelector("[data-verified-identifier]")?.textContent).toBe(DEX);
    expect((container.textContent ?? "").indexOf(DEX)).toBe(0);
    expect(container.querySelector("[data-details-not-verified]")?.textContent).toBe(DETAILS_NOT_VERIFIED);
  });

  it("leads with the email only when the API served it as verified (the control)", () => {
    const { container } = render(<Requester id={DEX} email="alice@example.com" grant={bob} />);
    expect(container.querySelector("[data-verified-identifier]")?.textContent).toBe("alice@example.com");
    expect(container.querySelector("[data-details-not-verified]")).toBeNull();
  });
});

describe("DUI-43: a consumed approval's verdict", () => {
  it("reads Verified · used by the run it admitted; one no run used reads Verified alone (the control)", () => {
    const { container } = render(<ApprovalStatus found={{ state: "verified", key: "k", consumedAt: "2026-10-14T22:17:00Z", usedBy: { kind: "Switchover", name: "swo-ttusskxkeb5i65k4gx4jd6davm" } }} />);
    expect(container.textContent).toBe("Verified · used by swo-ttusskxkeb5i65k4gx4jd6davm");
    cleanup();
    const plain = render(<ApprovalStatus found={{ state: "verified", key: "k" }} />);
    expect(plain.container.textContent).toBe("Verified");
  });
});
