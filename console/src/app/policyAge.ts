// A protection policy's newest recovery point against its objective: the one
// derivation the Overview's policy card and the Protection pages both draw.
// It is the Overview's `ageVsObjective`, moved here so the two cannot disagree;
// the Overview imports it from here once the page groups are merged.
import { ProgressVariant } from "@patternfly/react-core";
import type { ProtectionPolicy } from "../api/types";
import { duration } from "../components/When";

export function ageVsObjective(p: ProtectionPolicy) {
  const age = p.lastAvailablePoint?.ageSeconds;
  const objective = p.objectives.maxRecoveryPointAgeSeconds;
  if (typeof age !== "number") return { percent: 100, variant: ProgressVariant.danger, text: "No recovery point" };
  const percent = Math.min(100, Math.round((age / objective) * 100));
  const variant = percent >= 100 ? ProgressVariant.danger : percent >= 75 ? ProgressVariant.warning : ProgressVariant.success;
  return { percent, variant, text: `${duration(age)} of ${duration(objective)}` };
}
