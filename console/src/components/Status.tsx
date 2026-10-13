// Every status the console shows, in one place: a word, a colour and an icon,
// never colour alone. The words are the ones operators read; the API's raw value
// is kept in the tooltip-free `title` only where it helps support.
import { Label, Spinner } from "@patternfly/react-core";
import CheckCircleIcon from "@patternfly/react-icons/dist/esm/icons/check-circle-icon";
import ExclamationCircleIcon from "@patternfly/react-icons/dist/esm/icons/exclamation-circle-icon";
import ExclamationTriangleIcon from "@patternfly/react-icons/dist/esm/icons/exclamation-triangle-icon";
import BanIcon from "@patternfly/react-icons/dist/esm/icons/ban-icon";
import UnknownIcon from "@patternfly/react-icons/dist/esm/icons/unknown-icon";
import PendingIcon from "@patternfly/react-icons/dist/esm/icons/pending-icon";
import type { OperationSummary } from "../api/types";
import type { GroupVerdict, Mapping } from "../mock/cutover";

type Tone = "success" | "danger" | "warning" | "info" | "neutral" | "progress";

function StatusLabel({ tone, text, title }: { tone: Tone; text: string; title?: string }) {
  const common = { isCompact: true, title };
  switch (tone) {
    case "success":
      return <Label {...common} status="success" icon={<CheckCircleIcon />}>{text}</Label>;
    case "danger":
      return <Label {...common} status="danger" icon={<ExclamationCircleIcon />}>{text}</Label>;
    case "warning":
      return <Label {...common} status="warning" icon={<ExclamationTriangleIcon />}>{text}</Label>;
    case "info":
      return <Label {...common} status="info">{text}</Label>;
    case "progress":
      return (
        <Label {...common} color="blue" icon={<Spinner size="sm" aria-hidden="true" />}>
          {text}
        </Label>
      );
    default:
      return <Label {...common} color="grey">{text}</Label>;
  }
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
      return (
        <Label isCompact color="grey" icon={<BanIcon />} title={title}>
          Cancelled
        </Label>
      );
    default:
      return (
        <Label isCompact color="grey" icon={<UnknownIcon />} title={title}>
          Unknown
        </Label>
      );
  }
}

/** A protection policy's health (Healthy, AtRisk, Stale, Unprotected, Unknown). */
export function HealthStatus({ health }: { health: string | null | undefined }) {
  switch (health) {
    case "Healthy":
      return <StatusLabel tone="success" text="Protected" />;
    case "AtRisk":
      return <StatusLabel tone="warning" text="At risk" />;
    case "Stale":
      return <StatusLabel tone="warning" text="Stale" />;
    case "Unprotected":
      return <StatusLabel tone="danger" text="Not protected" />;
    default:
      return (
        <Label isCompact color="grey" icon={<PendingIcon />}>
          Not evaluated yet
        </Label>
      );
  }
}

export function ReachStatus({ state }: { state: string }) {
  if (state === "reachable") return <StatusLabel tone="success" text="Reachable" />;
  if (state === "unreachable") return <StatusLabel tone="danger" text="Unreachable" />;
  return <StatusLabel tone="neutral" text="Not checked yet" />;
}

export function MappingStatus({ mapping }: { mapping: Mapping }) {
  if (mapping === "exact") return <StatusLabel tone="success" text="Exact" />;
  if (mapping === "approximate") return <StatusLabel tone="info" text="Approximate" />;
  return <StatusLabel tone="danger" text="Unavailable" />;
}

export function VerdictStatus({ verdict }: { verdict: GroupVerdict }) {
  if (verdict === "ready") return <StatusLabel tone="success" text="Ready" />;
  if (verdict === "stopConsumers") return <StatusLabel tone="warning" text="Stop consumers first" />;
  return <StatusLabel tone="danger" text="Blocked" />;
}

export function CheckStatus({ state }: { state: "pending" | "running" | "pass" | "warn" }) {
  if (state === "pass") return <StatusLabel tone="success" text="Passed" />;
  if (state === "warn") return <StatusLabel tone="warning" text="Warning" />;
  if (state === "running") return <StatusLabel tone="progress" text="Checking" />;
  return <StatusLabel tone="neutral" text="Waiting" />;
}
