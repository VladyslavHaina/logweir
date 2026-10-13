// The saved-connection selector: a refusal selects nothing, a fresh form starts on the preferred role.
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { Connection } from "../../api/connections";
import { connections } from "../../mock/data";
import { ConnectionSelect } from "./ConnectionSelect";

afterEach(cleanup);

const teamA = () => connections.filter((c) => c.namespace === "team-a" && !c.name.startsWith("conn-")) as Connection[];

describe("ConnectionSelect", () => {
  it("starts a fresh form on the preferred role and hands back the UID with the name", () => {
    const onSelect = vi.fn();
    render(<ConnectionSelect id="target" label="Target" connections={teamA()} selection={null} prefer="target" onSelect={onSelect} />);
    const select = document.getElementById("target") as HTMLSelectElement;
    expect(select.selectedOptions[0]?.textContent).toMatch(/^orders-dr \(target\): connection probe reachable/);
    const prod = teamA().find((c) => c.name === "orders-prod")!;
    fireEvent.change(select, { target: { value: prod.uid } });
    expect(onSelect).toHaveBeenCalledWith({ uid: prod.uid, name: "orders-prod" });
  });

  it("refuses a connection recreated under the same name and selects nothing", () => {
    const held = { uid: "00000000-dead-4000-9000-000000000000", name: "orders-prod" };
    render(<ConnectionSelect id="source" label="Source" connections={teamA()} selection={held} onSelect={() => {}} />);
    expect(screen.getByText("The connection this form had chosen was recreated")).toBeTruthy();
    expect((document.getElementById("source") as HTMLSelectElement).value).toBe("");
    // Control: the object that really holds the UID is selected without a refusal.
    cleanup();
    const prod = teamA().find((c) => c.name === "orders-prod")!;
    render(<ConnectionSelect id="source" label="Source" connections={teamA()} selection={{ uid: prod.uid, name: prod.name }} onSelect={() => {}} />);
    expect(screen.queryByText(/had chosen/)).toBeNull();
    expect((document.getElementById("source") as HTMLSelectElement).value).toBe(prod.uid);
  });
});
