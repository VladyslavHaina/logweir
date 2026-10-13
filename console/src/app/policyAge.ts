// A protection policy's newest recovery point against its objective: the one
// derivation the Overview's policy card and the Protection pages both draw.
// The age is measured when the page reads it (attention.ts pointAgeSeconds):
// from the point's capture start when recorded, else the controller's own age.
import { ProgressVariant } from "@patternfly/react-core";
import type { ProtectionPolicy } from "../api/types";
import { duration } from "../components/When";
import { pointAgeSeconds } from "./attention";

export function ageVsObjective(p: ProtectionPolicy, now = Date.now()) {
  const age = pointAgeSeconds(p, now);
  const objective = p.objectives.maxRecoveryPointAgeSeconds;
  if (age === undefined) return { percent: 100, variant: ProgressVariant.danger, text: "No recovery point" };
  const percent = Math.min(100, Math.round((age / objective) * 100));
  const variant = percent >= 100 ? ProgressVariant.danger : percent >= 75 ? ProgressVariant.warning : ProgressVariant.success;
  return { percent, variant, text: `${duration(age)} of ${duration(objective)}` };
}
