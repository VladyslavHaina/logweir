// Every status the console shows, in one place and one style: an outline label
// with an icon, a word and a colour, never colour alone (styles.css "labels").
// The words are the ones operators read; the API's raw value goes in `title`.
import type { ReactNode } from "react";
import { Label, Spinner } from "@patternfly/react-core";
import BanIcon from "@patternfly/react-icons/dist/esm/icons/ban-icon";
import UnknownIcon from "@patternfly/react-icons/dist/esm/icons/unknown-icon";
import PendingIcon from "@patternfly/react-icons/dist/esm/icons/pending-icon";
import CheckDoubleIcon from "@patternfly/react-icons/dist/esm/icons/check-double-icon";
import OutlinedClockIcon from "@patternfly/react-icons/dist/esm/icons/outlined-clock-icon";
import RedoIcon from "@patternfly/react-icons/dist/esm/icons/redo-icon";
import UserIcon from "@patternfly/react-icons/dist/esm/icons/user-icon";
import HistoryIcon from "@patternfly/react-icons/dist/esm/icons/history-icon";
import type { Backup, OperationSummary } from "../api/types";
import type { MappingClass, Verdict } from "../api/switchovers";

export type Tone = "success" | "danger" | "warning" | "info" | "neutral" | "progress";

/** A status word as a label. Success, warning, danger and info take PatternFly's own status icons. */
export function StatusLabel({ tone, text, title, icon }: { tone: Tone; text: string; title?: string; icon?: ReactNode }) {
  const shown = icon ?? (tone === "progress" ? <Spinner size="sm" aria-hidden="true" /> : tone === "neutral" ? <PendingIcon /> : undefined);
  const status = tone === "success" || tone === "danger" || tone === "warning" || tone === "info" ? tone : undefined;
  return (
    <Label isCompact variant="outline" title={title} icon={shown} status={status} color={tone === "progress" ? "blue" : status ? undefined : "grey"}>
      {text}
    </Label>
  );
}

const IN_PROGRESS: Record<string, string> = {
  pending: "Starting",
  queued: "Queued",
  preparing: "Preparing",
  running: "Running",
  verifying: "Verifying",
};

/** A run's state. Green means the controller verified the signed evidence. */
export function RunStatus({ op }: { op: OperationSummary }) {
  const title = `state ${op.state}; verification ${op.verificationState}`;
  if (!op.terminal) return <StatusLabel tone="progress" text={IN_PROGRESS[op.state] ?? "In progress"} title={title} />;
  switch (op.state) {
    case "succeeded":
      return op.verifiedSuccess ? (
        <StatusLabel tone="success" text="Verified" title={title} />
      ) : (
        <StatusLabel tone="warning" text="Done, not verified" title={title} />
      );
    case "failed":
      return <StatusLabel tone="danger" text="Failed" title={title} />;
    case "refused":
      return <StatusLabel tone="danger" text="Refused" title={title} />;
    case "cancelled":
      return <StatusLabel tone="neutral" text="Cancelled" title={title} icon={<BanIcon />} />;
    default:
      return <StatusLabel tone="neutral" text="Unknown" title={title} icon={<UnknownIcon />} />;
  }
}

/** A protection policy's health (Healthy, AtRisk, Stale, Unprotected, Unknown). An absent health
 *  is "not evaluated yet"; `Unknown` is an evaluation that could not decide, never the same thing. */
export function HealthStatus({ health }: { health: string | null | undefined }) {
  switch (health) {
    case "Unknown":
      return <StatusLabel tone="neutral" text="Unknown" title="health Unknown: the evaluation could not decide" icon={<UnknownIcon />} />;
    case "Healthy":
      return <StatusLabel tone="success" text="Protected" />;
    case "AtRisk":
      return <StatusLabel tone="warning" text="At risk" />;
    case "Stale":
      return <StatusLabel tone="warning" text="Stale" />;
    case "Unprotected":
      return <StatusLabel tone="danger" text="Not protected" />;
    default:
      return <StatusLabel tone="neutral" text="Not evaluated yet" />;
  }
}

export function ReachStatus({ state }: { state: string }) {
  if (state === "reachable") return <StatusLabel tone="success" text="Reachable" />;
  if (state === "unreachable") return <StatusLabel tone="danger" text="Unreachable" />;
  return <StatusLabel tone="neutral" text="Not checked yet" />;
}

/** Captured with a recovery point, or not (and why). */
export function CaptureStatus({ captured, reason }: { captured: boolean; reason?: string | null }) {
  return captured ? <StatusLabel tone="success" text="Captured" /> : <StatusLabel tone="neutral" text="Not captured" title={reason ?? undefined} />;
}

// --------------------------------------------------------------- theme A

/** A position's translation class (PROD-04.2 §2.2). */
export function MappingStatus({ cls, title }: { cls: MappingClass; title?: string }) {
  if (cls === "exact") return <StatusLabel tone="success" text="Exact" title={title} />;
  if (cls === "approximate") return <StatusLabel tone="info" text="Approximate" title={title} />;
  return <StatusLabel tone="danger" text="Unavailable" title={title} />;
}

/** What the review concludes for one consumer group. */
export function VerdictStatus({ verdict, title }: { verdict: Verdict; title?: string }) {
  if (verdict === "ready") return <StatusLabel tone="success" text="Ready" title={title} />;
  if (verdict === "alreadyApplied") return <StatusLabel tone="info" text="Already switched" title={title} icon={<CheckDoubleIcon />} />;
  if (verdict === "stopConsumers") return <StatusLabel tone="warning" text="Stop consumers first" title={title} />;
  return <StatusLabel tone="danger" text="Blocked" title={title} />;
}


// ------------------------------------------------------------ plain facts

/** How a backup was started: plain text with an icon, so it never competes with status. */
export function TriggerText({ b }: { b: Backup }) {
  const kind = b.trigger?.kind ?? (b.triggeredBy === "manual" ? "Manual" : "Scheduled");
  const [Icon, text] =
    kind === "Retry"
      ? [RedoIcon, `Retry ${b.trigger?.attempt ?? ""}`.trim()]
      : kind === "Manual"
        ? [UserIcon, "Manual"]
        : kind === "CatchUp"
          ? [HistoryIcon, "Catch-up"]
          : [OutlinedClockIcon, "Scheduled"];
  return (
    <span className="lw-inline-icon">
      <Icon aria-hidden="true" />
      {text}
    </span>
  );
}
