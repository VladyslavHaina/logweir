// A Switchover's phase and a group's outcome, in the console's one status style
// (components/Status.tsx `StatusLabel`). The API sends strings; a value this build does
// not know reads as what it is, never as a success.
import { StatusLabel } from "../../components/Status";
import CheckDoubleIcon from "@patternfly/react-icons/dist/esm/icons/check-double-icon";
import UnknownIcon from "@patternfly/react-icons/dist/esm/icons/unknown-icon";
import type { Switchover } from "../../api/switchovers";

/** Where a Switchover stands (§3.3): held, running, or how it ended. */
export function PhaseLabel({ sw }: { sw: Pick<Switchover, "phase" | "reason"> }) {
  const title = `phase ${sw.phase ?? "not written"}${sw.reason ? `; ${sw.reason}` : ""}`;
  switch (sw.phase) {
    case null:
    case undefined:
      // DUI-31.10: just created, before the controller writes a phase (seconds later it
      // reads "Waiting for approval" or "Starting"); "Not started" read as a stop.
      return <StatusLabel tone="progress" text="Submitted" title={title} />;
    case "Pending":
      return sw.reason === "ApprovalNotVerified" ? (
        <StatusLabel tone="neutral" text="Waiting for approval" title={title} />
      ) : (
        <StatusLabel tone="progress" text="Starting" title={title} />
      );
    case "Running":
      return <StatusLabel tone="progress" text="Running" title={title} />;
    case "Succeeded":
      return <StatusLabel tone="success" text="Succeeded" title={title} />;
    case "PartiallyApplied":
      return <StatusLabel tone="warning" text="Partially applied" title={title} />;
    case "Refused":
      return <StatusLabel tone="danger" text="Refused" title={title} />;
    case "Failed":
      return <StatusLabel tone="danger" text="Failed" title={title} />;
    default:
      return <StatusLabel tone="neutral" text="Unknown" title={title} icon={<UnknownIcon />} />;
  }
}

/** One group's outcome (§3.5); `null` while the run has not reported it. */
export function OutcomeLabel({ outcome }: { outcome: string | null }) {
  switch (outcome) {
    case null:
      return <StatusLabel tone="neutral" text="Waiting" />;
    case "applied":
      return <StatusLabel tone="success" text="Applied" title="applied" />;
    case "alreadyApplied":
      return <StatusLabel tone="info" text="Already applied" title="alreadyApplied" icon={<CheckDoubleIcon />} />;
    case "refused":
      return <StatusLabel tone="warning" text="Refused" title="refused: nothing committed for this group" />;
    case "failed":
      return <StatusLabel tone="danger" text="Failed" title="failed" />;
    case "untouched":
      return <StatusLabel tone="neutral" text="Not reached" title="untouched" />;
    default:
      return <StatusLabel tone="neutral" text="Unknown" title={outcome} icon={<UnknownIcon />} />;
  }
}
