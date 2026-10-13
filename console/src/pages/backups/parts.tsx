// Pieces the Backups, Recovery catalog and Trust pages share: the run's status
// word, the verdict label, a command block with copy, a description-list row,
// and the session's flags for this namespace.
import { useState, type ReactNode } from "react";
import {
  ClipboardCopyButton,
  CodeBlock,
  CodeBlockAction,
  CodeBlockCode,
  DescriptionListDescription,
  DescriptionListGroup,
  DescriptionListTerm,
} from "@patternfly/react-core";
import BanIcon from "@patternfly/react-icons/dist/esm/icons/ban-icon";
import UnknownIcon from "@patternfly/react-icons/dist/esm/icons/unknown-icon";
import { useSession } from "../../api/queries";
import type { Backup } from "../../api/backups";
import type { Capabilities } from "../../api/types";
import { StatusLabel } from "../../components/Status";
import { CASE_WORDS, listVerdict, progressWord, type Verdict } from "./verdict";

/** The session's capability flags in `ns` (`implemented && allowed`), or null without a grant or before the session answers. */
export function useFlags(ns: string): Capabilities | null {
  const session = useSession().data;
  return session?.namespaces.find((g) => g.name === ns)?.capabilities ?? null;
}

/** The union of every grant's flags: how a cluster-scoped read (trust) is gated. */
export function useAnyFlags(): Capabilities | null {
  return useSession().data?.capabilities ?? null;
}

export function Term({ term, children }: { term: string; children: ReactNode }) {
  return (
    <DescriptionListGroup>
      <DescriptionListTerm>{term}</DescriptionListTerm>
      <DescriptionListDescription>{children}</DescriptionListDescription>
    </DescriptionListGroup>
  );
}

/** A missing value reads "Not recorded", never a dash. */
export function Value({ value }: { value: string | number | null | undefined }) {
  if (value === null || value === undefined || value === "") return <span className="lw-subtle">Not recorded</span>;
  return <>{typeof value === "number" ? value.toLocaleString("en-US") : value}</>;
}

/**
 * A run's status in one word: its lifecycle while it moves, then the verdict. Green ("Verified")
 * only when the list rule passes; a finished run that is not green names its case beneath.
 */
export function backupStatusOf(b: Pick<Backup, "operation" | "queue">): { label: ReactNode; detail: string | null; title: string } {
  const op = b.operation;
  const title = `state ${op.state}; verification ${op.verificationState}`;
  if (!op.terminal) return { label: <StatusLabel tone="progress" text={progressWord(b)} title={title} />, detail: null, title };
  const verdict = listVerdict(op);
  const exit = typeof op.exitCode === "number" ? `exit ${op.exitCode}` : null;
  const reason = op.stateReason && op.stateReason !== "Ok" ? op.stateReason : null;
  const failure = [exit, reason].filter(Boolean).join(" · ") || null;
  switch (op.state) {
    case "succeeded":
      return verdict.green
        ? { label: <StatusLabel tone="success" text="Verified" title={title} />, detail: null, title }
        : {
            label: <StatusLabel tone="warning" text="Not verified" title={verdict.reason} />,
            detail: CASE_WORDS[verdict.reason] ?? null,
            title: verdict.reason,
          };
    case "failed":
      return { label: <StatusLabel tone="danger" text="Failed" title={title} />, detail: failure, title };
    case "refused":
      return { label: <StatusLabel tone="danger" text="Refused" title={title} />, detail: failure, title };
    case "cancelled":
      return { label: <StatusLabel tone="neutral" text="Cancelled" title={title} icon={<BanIcon />} />, detail: reason, title };
    default:
      return { label: <StatusLabel tone="neutral" text="Unknown" title={title} icon={<UnknownIcon />} />, detail: reason, title };
  }
}

export function BackupStatus({ b, withDetail = false }: { b: Pick<Backup, "operation" | "queue">; withDetail?: boolean }) {
  const s = backupStatusOf(b);
  return (
    <>
      {s.label}
      {withDetail && s.detail && <div className="lw-reason lw-ev-cell-note">{s.detail}</div>}
    </>
  );
}

/** The run page's verdict: "Verified" with what the controller recorded, or "Not verified" with the case. */
export function VerdictLabel({ verdict }: { verdict: Verdict }) {
  return verdict.green ? (
    <StatusLabel tone="success" text={verdict.historical ? "Verified (retired key)" : "Verified"} title={verdict.label} />
  ) : (
    <StatusLabel tone="warning" text="Not verified" title={verdict.reason} />
  );
}

let blockSerial = 0;

/** Lines to copy and run elsewhere: shown in full, wrapped, with one copy button. Never submitted. */
export function Commands({ lines, label }: { lines: string[]; label: string }) {
  const [copied, setCopied] = useState(false);
  const [id] = useState(() => `lw-cmd-${(blockSerial += 1)}`);
  const text = lines.join("\n");
  return (
    <CodeBlock
      actions={
        <CodeBlockAction>
          <ClipboardCopyButton
            id={`${id}-copy`}
            textId={`${id}-text`}
            aria-label={`Copy ${label}`}
            onClick={() => {
              void navigator.clipboard?.writeText(text).then(() => setCopied(true));
            }}
            exitDelay={copied ? 1500 : 600}
            onTooltipHidden={() => setCopied(false)}
            variant="plain"
          >
            {copied ? "Copied" : `Copy ${label}`}
          </ClipboardCopyButton>
        </CodeBlockAction>
      }
    >
      <CodeBlockCode id={`${id}-text`}>{text}</CodeBlockCode>
    </CodeBlock>
  );
}
