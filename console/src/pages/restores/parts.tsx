// Small pieces the Restores pages share: the restore status label (one status style),
// a fact list, a field's helper line, a copyable command block, and the problem block.
import { useId, useState, type ReactNode } from "react";
import {
  Alert,
  ClipboardCopyButton,
  CodeBlock,
  CodeBlockAction,
  CodeBlockCode,
  DescriptionList,
  DescriptionListDescription,
  DescriptionListGroup,
  DescriptionListTerm,
  FormHelperText,
  HelperText,
  HelperTextItem,
} from "@patternfly/react-core";
import { ApiError } from "../../api/client";
import { RunStatus, StatusLabel } from "../../components/Status";
import { When } from "../../components/When";
import { outcomeUnknown } from "../../api/client";
import type { Restore } from "../../api/restores";
import { restoreVerified, summaryCase } from "./words";

/** A restore's status: green only by the restore badge rule, and a complete verification
 *  that did not cover is never green whatever the summary says. */
export function RestoreStatus({ r }: { r: Pick<Restore, "operation" | "coverage"> }) {
  const op = r.operation;
  const title = `state ${op.state}; verification ${op.verificationState}`;
  if (r.coverage?.covered === false) return <StatusLabel tone="danger" text="Not covered" title={`${title}; complete coverage did not cover this restore`} />;
  if (op.state === "pending" && op.stateReason === "ApprovalNotVerified") return <StatusLabel tone="info" text="Awaiting approval" title={title} />;
  if (op.terminal && op.state === "succeeded" && !restoreVerified(r)) {
    return <StatusLabel tone="warning" text="Done, not verified" title={`${title}: ${summaryCase(op)}`} />;
  }
  return <RunStatus op={op} />;
}

export interface Fact {
  term: string;
  value: ReactNode;
  id?: string;
}

/** A compact term/description list, horizontal on wide screens. */
export function Facts({ items, narrow = false }: { items: (Fact | false | null | undefined)[]; narrow?: boolean }) {
  return (
    <DescriptionList isCompact className={narrow ? "lw-dl lw-dl--narrow" : "lw-dl"} orientation={{ sm: "vertical", md: "horizontal" }}>
      {items.filter((f): f is Fact => !!f).map((f) => (
        <DescriptionListGroup key={f.term}>
          <DescriptionListTerm>{f.term}</DescriptionListTerm>
          <DescriptionListDescription id={f.id}>{f.value}</DescriptionListDescription>
        </DescriptionListGroup>
      ))}
    </DescriptionList>
  );
}

/** "Not recorded" for an absent value, never a dash. */
export function shown(value: ReactNode | null | undefined): ReactNode {
  return value === null || value === undefined || value === "" ? <span className="lw-subtle">Not recorded</span> : value;
}

/** A sentence whose `code spans` render as code, never as literal backticks. */
export function Coded({ text }: { text: string }) {
  return (
    <>
      {text.split("`").map((part, i) =>
        i % 2 === 1 ? (
          <code key={i} className="lw-mono">
            {part}
          </code>
        ) : (
          part
        ),
      )}
    </>
  );
}

/** The line under a field: its error when there is one, its help otherwise. */
export function FieldHelp({ id, error, children }: { id: string; error?: string; children?: ReactNode }) {
  if (!error && !children) return null;
  return (
    <FormHelperText>
      <HelperText>
        <HelperTextItem id={`${id}-help`} variant={error ? "error" : "default"}>
          {error ? <Coded text={error} /> : children}
        </HelperTextItem>
      </HelperText>
    </FormHelperText>
  );
}

/** Commands (or a document) to copy, as one block with a copy button. */
export function Commands({ lines, label, exact }: { lines: string[]; label: string; exact?: string }) {
  const id = useId();
  const [copied, setCopied] = useState(false);
  const shownText = lines.join("\n");
  // A signed document is copied byte for byte, its trailing newline included.
  const text = exact ?? shownText;
  return (
    <CodeBlock
      className="lw-commands"
      actions={
        <CodeBlockAction>
          <ClipboardCopyButton
            id={`${id}-copy`}
            textId={`${id}-text`}
            aria-label={`Copy ${label}`}
            variant="plain"
            onClick={() => {
              void navigator.clipboard?.writeText(text).then(() => setCopied(true));
            }}
            onTooltipHidden={() => setCopied(false)}
          >
            {copied ? "Copied" : `Copy ${label}`}
          </ClipboardCopyButton>
        </CodeBlockAction>
      }
    >
      <CodeBlockCode id={`${id}-text`}>{shownText}</CodeBlockCode>
    </CodeBlock>
  );
}

/** A refusal or a lost answer, in the console's one error wording. */
export function WriteProblem({ error, title, unmatched = [] }: { error: unknown; title: string; unmatched?: string[] }) {
  if (outcomeUnknown(error)) {
    return (
      <Alert variant="warning" isInline title="The answer was lost">
        The restore may or may not exist. Submitting again is safe: the same plan carries the same key, so it never makes a second
        restore. {error instanceof Error ? `(${error.message})` : ""}
      </Alert>
    );
  }
  const api = error instanceof ApiError ? error : null;
  return (
    <Alert variant="danger" isInline title={title}>
      <p>{error instanceof Error ? error.message : String(error)}</p>
      {error instanceof ApiError && unmatched.length > 0 && (
        <ul className="lw-plain-list">
          {unmatched.map((u) => (
            <li key={u}>{u}</li>
          ))}
        </ul>
      )}
      {api && (
        <p className="lw-error-meta">
          {api.status} {api.code}
          {api.requestId ? ` · request ${api.requestId}` : ""}
        </p>
      )}
    </Alert>
  );
}

/** `When`, remounted per value: its 30 s tick would read an instant newer than the tick
 *  as "in a moment". */
export function Instant({ value, exact = false }: { value: string | number | null | undefined; exact?: boolean }) {
  return <When key={String(value)} value={value} exact={exact} />;
}
