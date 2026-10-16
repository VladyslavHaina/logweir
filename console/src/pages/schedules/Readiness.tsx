// A backup readiness check: a real Preflight Job against the source, the
// destination and the named topics a form describes. Its verdict is the
// check's own record, it goes stale when the form changes, and it never gates
// a create: it is a statement about the minute it ran in.
import { useState } from "react";
import { Alert, Button, Content, Flex, Title } from "@patternfly/react-core";
import type { components } from "../../api/schema";
import { mintIntent, usePreflight, useStartReadiness, type Preflight } from "../../api/schedules";
import { StatusLabel, type Tone } from "../../components/Status";
import { When } from "../../components/When";
import { missingRead } from "./model";
import { WriteAlert } from "./parts";
import { Prose, checkMessage } from "../../components/Prose";
import { advisoryRows } from "../connections/parts";

type BackupCheck = components["schemas"]["BackupPreflightRequest"];

type CheckRow = Preflight["checks"][number];

const VERDICT: Record<string, { tone: Tone; text: string }> = {
  ready: { tone: "success", text: "Passed" },
  notReady: { tone: "danger", text: "Not met" },
  unknown: { tone: "warning", text: "Unknown" },
  skipped: { tone: "neutral", text: "Skipped" },
};

/** DUI-36: an advisory row never gates the verdict, so one that is not met is a warning, never a red "Not met". */
const ADVISORY_VERDICT: Record<string, { tone: Tone; text: string }> = {
  ready: { tone: "success", text: "Passed" },
  notReady: { tone: "warning", text: "Not met (advisory)" },
  unknown: { tone: "neutral", text: "Unknown" },
  skipped: { tone: "neutral", text: "Skipped" },
};

/** The row that says whether the source names each consumer group's type: it matters only to a schedule that selects groups. */
const GROUP_TYPES_ROW = "connection.groupTypes";

export function readinessWords(p: Preflight): { tone: Tone; title: string } {
  if (!p.terminal) return { tone: "progress", title: "Checking" };
  switch (p.state) {
    case "ready":
      return { tone: "success", title: "Ready: every blocking check passed" };
    case "notReady":
      return { tone: "danger", title: "Not ready: a prerequisite is not met" };
    case "unknown":
      return { tone: "warning", title: "Unknown: a blocking check could not be answered" };
    case "cancelled":
      return { tone: "neutral", title: "Cancelled" };
    default:
      return { tone: "danger", title: `The check itself failed${p.reason ? ` (${p.reason})` : ""}` };
  }
}

/**
 * The check's rows as Test connection shows them (DUI-7, DUI-36): the blocking rows the
 * verdict rests on, then the advisory ones, which never move it. An execution-only row
 * that could not be answered is in the run's own list below, not here. The consumer-group
 * types row is left out for a draft that selects no group: what it says about selected
 * groups does not apply to it.
 */
export function readinessRows(p: Preflight, groupsSelected: boolean): { blocking: CheckRow[]; advisory: CheckRow[] } {
  const atRun = (c: CheckRow) => c.gating === "executionOnly" && (c.state === "unknown" || c.state === "skipped");
  return {
    blocking: p.checks.filter((c) => c.gating !== "advisory" && !atRun(c)),
    advisory: advisoryRows(p).filter((c) => groupsSelected || c.id !== GROUP_TYPES_ROW),
  };
}

function CheckList({ label, rows, advisory }: { label: string; rows: CheckRow[]; advisory: boolean }) {
  return (
    <ol className="lw-checks" aria-label={label}>
      {rows.map((c) => {
        const read = missingRead(c);
        const verdict = (advisory ? ADVISORY_VERDICT : VERDICT)[c.state] ?? { tone: "neutral" as const, text: c.state };
        return (
          <li key={c.id}>
            {read ? (
              <span>
                <strong>Missing READ on {read.count} topic(s)</strong>
                {read.sample.length > 0 && (
                  <span>
                    : {read.sample.join(", ")}
                    {read.count > read.sample.length ? `, and ${read.count - read.sample.length} more` : ""}
                  </span>
                )}
                <span className="lw-subtle lw-small">
                  {" "}
                  <Prose text={read.message} />
                </span>
                {read.remedy && (
                  <span className="lw-subtle lw-small">
                    {" "}
                    <Prose text={read.remedy} />
                  </span>
                )}
              </span>
            ) : (
              <span>
                <Prose text={checkMessage(c.message ?? c.id)} />
                {c.remedy && (
                  <span className="lw-subtle lw-small">
                    {" "}
                    <Prose text={c.remedy} />
                  </span>
                )}
              </span>
            )}
            <StatusLabel {...verdict} title={`${c.code ?? c.id}${advisory ? " (advisory)" : ""}`} />
          </li>
        );
      })}
    </ol>
  );
}

export function Verdict({ preflight, groupsSelected = true }: { preflight: Preflight; groupsSelected?: boolean }) {
  const words = readinessWords(preflight);
  const { blocking, advisory } = readinessRows(preflight, groupsSelected);
  return (
    <div aria-live="polite">
      <Flex gap={{ default: "gapSm" }} alignItems={{ default: "alignItemsCenter" }}>
        <StatusLabel tone={words.tone} text={words.title} title={`state ${preflight.state}`} />
        {preflight.observedAt && (
          <span className="lw-subtle lw-small">
            observed <When value={preflight.observedAt} />
          </span>
        )}
      </Flex>
      {blocking.length > 0 && (
        <>
          <Title headingLevel="h3" size="md" className="lw-gap-top">
            Blocking checks
          </Title>
          <CheckList label="Blocking checks" rows={blocking} advisory={false} />
        </>
      )}
      {advisory.length > 0 && (
        <>
          <Title headingLevel="h3" size="md" className="lw-gap-top">
            Advisory
          </Title>
          <Content component="p" className="lw-subtle lw-small">
            These never change the verdict above: the backup runs either way, and each says what it records.
          </Content>
          <CheckList label="Advisory checks" rows={advisory} advisory />
        </>
      )}
      {preflight.executionOnly.length > 0 && (
        <>
          <Content component="p" className="lw-subtle lw-small">
            Only the run itself can answer:
          </Content>
          {/* One item per note: joined into one sentence, two notes ran together with no separator. */}
          <ul className="lw-plain-list lw-subtle lw-small" aria-label="Only the run itself can answer">
            {preflight.executionOnly.map((e) => (
              <li key={e.id}>
                <code>{e.id}</code> <Prose text={e.note} />
              </li>
            ))}
          </ul>
        </>
      )}
    </div>
  );
}

/** `request` null: this draft cannot be checked yet, for the reason given. */
export function Readiness({ ws, request, why, groupsSelected }: { ws: string; request: BackupCheck | null; why: string; groupsSelected: boolean }) {
  const start = useStartReadiness(ws);
  const [asked, setAsked] = useState<{ id: string; key: string } | null>(null);
  const preflight = usePreflight(ws, asked?.id);
  const key = request ? JSON.stringify(request) : "";
  const outdated = asked !== null && asked.key !== key;

  return (
    <div>
      <Content component="p" className="lw-subtle">
        Optional. A readiness check runs a real Job that dials the source, lists the destination and describes the
        topics. It does not block creating the schedule, and it is not a promise about the next run.
      </Content>
      {request === null ? (
        <Content component="p" className="lw-subtle">
          {why}
        </Content>
      ) : (
        <Button
          variant="secondary"
          isLoading={start.isPending || (preflight.data !== undefined && !preflight.data.terminal && !outdated)}
          isDisabled={start.isPending}
          onClick={() =>
            start.mutate(
              { backup: request, intent: mintIntent("readiness") },
              { onSuccess: (item) => setAsked({ id: item.id, key }) },
            )
          }
        >
          {asked ? "Check again" : "Check readiness"}
        </Button>
      )}
      {start.isError && <WriteAlert error={start.error} kind="create" subject="the readiness check" verb="started" />}
      {asked && outdated && (
        <Alert isInline isPlain variant="info" title="The form changed after this check" className="lw-gap-top">
          Its answer is about a different source, destination or topic list, so it is not shown. Check again.
        </Alert>
      )}
      {asked && !outdated && preflight.data && (
        <div className="lw-gap-top">
          <Verdict preflight={preflight.data} groupsSelected={groupsSelected} />
        </div>
      )}
    </div>
  );
}
