// Pieces the schedule and protection pages share: the next-run table, the
// status of a schedule, the words a failed write gets, and the session's grant.
import type { ReactNode } from "react";
import {
  Alert,
  AlertActionLink,
  Content,
  DescriptionListDescription,
  DescriptionListGroup,
  DescriptionListTerm,
} from "@patternfly/react-core";
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import PauseCircleIcon from "@patternfly/react-icons/dist/esm/icons/pause-circle-icon";
import { grantOf, useSessionDoc } from "../../api/session";
import { ApiError } from "../../api/client";
import type { NextRun, Schedule } from "../../api/schedules";
import { StatusLabel } from "../../components/Status";
import { CompactList, useNarrow } from "../../components/CompactList";
import { When } from "../../components/When";
import { ADJUSTMENT_WORDS, localWords, outcomeOf, scheduleState } from "./model";

/** What the signed-in actor may do in `ns`, from the session; undefined without a grant there. */
export function useGrant(ns: string) {
  return grantOf(useSessionDoc(), ns)?.capabilities;
}

export function Term({ term, children }: { term: string; children: ReactNode }) {
  return (
    <DescriptionListGroup>
      <DescriptionListTerm>{term}</DescriptionListTerm>
      <DescriptionListDescription>{children}</DescriptionListDescription>
    </DescriptionListGroup>
  );
}

export function ScheduleStatus({ schedule }: { schedule: Schedule }) {
  const state = scheduleState(schedule);
  return (
    <StatusLabel
      tone={state.tone}
      text={state.text}
      title={state.title}
      icon={state.text === "Suspended" ? <PauseCircleIcon /> : undefined}
    />
  );
}

const MARKER_SHORT: Record<string, string> = {
  NonexistentLocalTimeShifted: "Moved to the end of a clock change",
  RepeatedLocalTimeFirst: "First of two runs (clocks go back)",
  RepeatedLocalTimeSecond: "Second of two runs (clocks go back)",
};

/** Next firings, from a draft's preview or a schedule's status.nextRuns: one
 *  renderer, one shape. The local time is shown as the API wrote it, offset
 *  included, so the two runs of a repeated hour stay two rows. */
export function NextRuns({ runs, zone, now = Date.now(), label }: { runs: NextRun[] | null | undefined; zone: string; now?: number; label: string }) {
  const narrow = useNarrow();
  if (runs === null || runs === undefined) {
    return (
      <Content component="p" className="lw-subtle">
        Not computed yet. The controller writes the next runs once it has evaluated this revision; the console never
        works them out itself.
      </Content>
    );
  }
  if (runs.length === 0) {
    return (
      <Content component="p" className="lw-subtle">
        No further run. A suspended schedule, a cadence the controller refused and one that has genuinely run out all
        look like this; the status above says which.
      </Content>
    );
  }
  const stale = Date.parse(runs[0]!.at) < now;
  return (
    <>
      {stale && (
        <Alert isInline isPlain variant="warning" title="These runs are out of date">
          The first one is in the past, so nothing has rewritten them since it came due: the controller may not be
          evaluating this schedule.
        </Alert>
      )}
      {narrow ? (
        <CompactList
          flush
          label={label}
          items={runs.map((r) => ({
            key: r.at,
            title: localWords(r.localTime),
            meta: (
              <>
                <When value={r.at} />
                {r.adjustment ? ` · ${MARKER_SHORT[r.adjustment] ?? r.adjustment}` : ""}
              </>
            ),
            status: null,
          }))}
        />
      ) : (
        <Table aria-label={label} variant="compact" gridBreakPoint="grid-md">
          <Thead>
            <Tr>
              <Th>Local time ({zone})</Th>
              <Th>When</Th>
              <Th>Clock change</Th>
            </Tr>
          </Thead>
          <Tbody>
            {runs.map((r) => (
              <Tr key={r.at}>
                <Td dataLabel={`Local time (${zone})`} className="lw-mono">
                  {localWords(r.localTime)}
                </Td>
                <Td dataLabel="When">
                  <When value={r.at} />
                </Td>
                <Td dataLabel="Clock change">
                  {r.adjustment ? (
                    <span title={r.adjustment}>{ADJUSTMENT_WORDS[r.adjustment] ?? r.adjustment}</span>
                  ) : (
                    <span className="lw-subtle">None</span>
                  )}
                </Td>
              </Tr>
            ))}
          </Tbody>
        </Table>
      )}
    </>
  );
}

/** How a write's request is made safe to resend, which decides what an unknown outcome says. */
export type WriteKind = "create" | "replace" | "command";

const UNKNOWN: Record<WriteKind, string> = {
  create:
    "Sending it again is safe: it resends the same request key, and the API answers a repeat with what the first request made instead of making a second.",
  replace:
    "Saving again is safe: if the first save landed, the revision has moved and the second is refused instead of applied twice.",
  command: "Trying again is safe: it asks for the same value, so it either makes the change or finds it already made.",
};

const capital = (text: string) => `${text[0]!.toUpperCase()}${text.slice(1)}`;

/** The words a failed write gets: what is known, about the request that was made. */
export function WriteAlert({
  error,
  kind,
  subject,
  verb,
  unmatched = [],
  children,
  onRetry,
  retryText = "Try again",
}: {
  error: unknown;
  kind: WriteKind;
  /** "the schedule" */
  subject: string;
  /** "created" */
  verb: string;
  unmatched?: string[];
  children?: ReactNode;
  onRetry?: () => void;
  retryText?: string;
}) {
  const outcome = outcomeOf(error);
  const api = error instanceof ApiError ? error : null;
  const meta = api ? `${api.status} ${api.code}${api.requestId ? ` · request ${api.requestId}` : ""}` : "No answer reached this page.";
  const title =
    outcome === "unknown"
      ? `No answer: whether ${subject} was ${verb} is unknown`
      : outcome === "invalid"
        ? `${capital(subject)} was not ${verb}: fix the fields marked below`
        : `${capital(subject)} was not ${verb}`;
  return (
    <Alert
      isInline
      variant={outcome === "unknown" ? "warning" : "danger"}
      title={title}
      actionLinks={onRetry ? <AlertActionLink onClick={onRetry}>{retryText}</AlertActionLink> : undefined}
    >
      {outcome === "unknown" && <p>It may or may not have happened. {UNKNOWN[kind]}</p>}
      {outcome !== "unknown" && error instanceof Error && <p>{error.message}</p>}
      {unmatched.length > 0 && (
        <ul>
          {unmatched.map((m) => (
            <li key={m}>{m}</li>
          ))}
        </ul>
      )}
      {children}
      <p className="lw-error-meta">{meta}</p>
    </Alert>
  );
}
