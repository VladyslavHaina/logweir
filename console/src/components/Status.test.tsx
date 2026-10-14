// DUI-22: a run held for its approval says so; it is not "Starting".
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import type { OperationSummary } from "../api/types";
import { RunStatus } from "./Status";

afterEach(cleanup);

const op = (over: Partial<OperationSummary>): OperationSummary => ({ state: "pending", terminal: false, verificationState: "pending", verifiedSuccess: false, ...over }) as OperationSummary;

describe("a run's status", () => {
  it("reads Waiting for approval while the controller holds it for its approval", () => {
    render(<RunStatus op={op({ stateReason: "ApprovalNotVerified" })} />);
    expect(screen.getByText("Waiting for approval")).toBeTruthy();
  });
  it("reads Starting for any other pending run (the control)", () => {
    render(<RunStatus op={op({ stateReason: "JobCreating" })} />);
    expect(screen.getByText("Starting")).toBeTruthy();
  });
});
