// DUI-61: the plan's target registry, as the approval page reads it from the plan bytes.
import { describe, expect, it } from "vitest";
import registryPlan from "../../../fixtures/plan-registry.golden.yaml?raw";
import plainPlan from "../../../fixtures/plan.golden.yaml?raw";
import { planTargetRegistry } from "./words";

describe("DUI-61: planTargetRegistry", () => {
  it("reads the console's own plan, and the flow style a hand-written plan may use", () => {
    expect(planTargetRegistry(registryPlan)).toEqual({ url: "https://schema-registry.orders-dr.svc:8081", username: "logweir" });
    expect(planTargetRegistry('target:\n  schema_registry: {url: "http://r:8081"}\nrestore:\n  x: 1\n')).toEqual({ url: "http://r:8081", username: null });
  });
  it("control: a plan with no target registry names none", () => {
    expect(planTargetRegistry(plainPlan)).toBeNull();
    expect(planTargetRegistry(null)).toBeNull();
  });
});
