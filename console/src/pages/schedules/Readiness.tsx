// A backup readiness check: a real Preflight Job against the source, the
// destination and the named topics a form describes. Its verdict is the
// check's own record, it goes stale when the form changes, and it never gates
// a create: it is a statement about the minute it ran in.
import { useState } from "react";
import { Alert, Button, Content, Flex } from "@patternfly/react-core";
import type { components } from "../../api/schema";
import { mintIntent, usePreflight, useStartReadiness, type Preflight } from "../../api/schedules";
import { StatusLabel, type Tone } from "../../components/Status";
import { When } from "../../components/When";
import { WriteAlert } from "./parts";

type BackupCheck = components["schemas"]["BackupPreflightRequest"];

const VERDICT: Record<string, { tone: Tone; text: string }> = {
  ready: { tone: "success", text: "Passed" },
  notReady: { tone: "danger", text: "Not met" },
  unknown: { tone: "warning", text: "Unknown" },
  skipped: { tone: "neutral", text: "Skipped" },
};

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

export function Verdict({ preflight }: { preflight: Preflight }) {
  const words = readinessWords(preflight);
  const rows = [...preflight.checks, ...preflight.warnings];
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
      {rows.length > 0 && (
        <ol className="lw-checks">
          {rows.map((c) => (
            <li key={c.id}>
              <span>
                {c.message ?? c.id}
                {c.remedy && <span className="lw-subtle lw-small"> {c.remedy}</span>}
              </span>
              <StatusLabel {...(VERDICT[c.state] ?? { tone: "neutral" as const, text: c.state })} title={c.code ?? c.id} />
            </li>
          ))}
        </ol>
      )}
      {preflight.executionOnly.length > 0 && (
        <Content component="p" className="lw-subtle lw-small">
          Only the run itself can answer: {preflight.executionOnly.map((e) => e.note).join(" ")}
        </Content>
      )}
    </div>
  );
}

/** `request` null: this draft cannot be checked yet, for the reason given. */
export function Readiness({ ns, request, why }: { ns: string; request: BackupCheck | null; why: string }) {
  const start = useStartReadiness(ns);
  const [asked, setAsked] = useState<{ id: string; key: string } | null>(null);
  const preflight = usePreflight(ns, asked?.id);
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
          <Verdict preflight={preflight.data} />
        </div>
      )}
    </div>
  );
}
