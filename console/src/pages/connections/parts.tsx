// Pieces the Connections and Destinations pages share: a form field with its
// error beside it, the outcome of a write in words, a probe's labels, and a
// readiness check (Preflight) rendered from its own recorded fields.
import type { ReactNode } from "react";
import type { z } from "zod";
import {
  Alert,
  Button,
  Content,
  DescriptionList,
  DescriptionListDescription,
  DescriptionListGroup,
  DescriptionListTerm,
  Flex,
  FormGroup,
  FormHelperText,
  HelperText,
  HelperTextItem,
  Title,
} from "@patternfly/react-core";
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import BanIcon from "@patternfly/react-icons/dist/esm/icons/ban-icon";
import UnknownIcon from "@patternfly/react-icons/dist/esm/icons/unknown-icon";
import { ApiError } from "../../api/client";
import {
  CHECKING_SENTENCE,
  CHECK_UNFINISHED_SENTENCE,
  CHECK_UNREADABLE_SENTENCE,
  PROBE_WORDS,
  REFUSAL_GLOSS,
  outcomeOf,
  type Preflight,
  type ProbeState,
  type Stopped,
} from "../../api/connections";
import { StatusLabel, type Tone } from "../../components/Status";
import { When } from "../../components/When";
import { Ident } from "../../components/Ident";
import { CompactList, useNarrow } from "../../components/CompactList";

// ------------------------------------------------------------------- forms

/** Adds each of the page's own problems to zod's issues, at its field's path. */
export function withProblems<T>(problems: (v: T) => Partial<Record<string, string>>) {
  return (v: T, ctx: z.RefinementCtx) => {
    for (const [path, message] of Object.entries(problems(v))) {
      if (message) ctx.addIssue({ code: "custom", path: path.split("."), message });
    }
  };
}

/** A form field: its label, the control, and its error (or help) beside it. */
export function Field({
  id,
  label,
  error,
  help,
  isRequired,
  role,
  isInline,
  children,
}: {
  id: string;
  label: ReactNode;
  error?: string | undefined;
  help?: ReactNode;
  isRequired?: boolean;
  role?: "group" | "radiogroup";
  isInline?: boolean;
  children: ReactNode;
}) {
  return (
    <FormGroup label={label} fieldId={id} isRequired={isRequired} role={role} isStack={role !== undefined && !isInline} isInline={isInline}>
      {children}
      {(error || help) && (
        <FormHelperText>
          <HelperText id={`${id}-helper`}>
            <HelperTextItem variant={error ? "error" : "default"}>{error ?? help}</HelperTextItem>
          </HelperText>
        </FormHelperText>
      )}
    </FormGroup>
  );
}

/** The props a text control takes from its field's state. */
export function controlState(id: string, error: string | undefined, hasHelp = true) {
  return {
    id,
    validated: error ? ("error" as const) : ("default" as const),
    "aria-invalid": error ? true : undefined,
    "aria-describedby": error || hasHelp ? `${id}-helper` : undefined,
  };
}

export const CREDENTIALS_CLEARED =
  "What you typed is kept, except the credential fields: those are cleared after every submit and are never kept anywhere, so type them again.";

/**
 * What became of a write, in words that say only what is known. A create that
 * got no answer may or may not exist, and repeating it is safe because the
 * form still holds its idempotency key.
 */
export function WriteOutcome({
  error,
  subject,
  verb = "create",
  credentials,
  unmatched,
}: {
  error: unknown;
  subject: string;
  verb?: "create" | "rotate";
  credentials: boolean;
  unmatched: string[];
}) {
  const outcome = outcomeOf(error);
  const kept = credentials ? CREDENTIALS_CLEARED : "Your input is kept.";
  const api = error instanceof ApiError ? error : null;
  const meta = api ? (
    <Content component="p" className="lw-subtle lw-small">
      {api.status} {api.code}
      {api.requestId ? ` · request ${api.requestId}` : ""}
    </Content>
  ) : null;
  if (outcome === "local") {
    return (
      <Alert isInline variant="danger" title="Nothing was sent">
        <p>{error instanceof Error ? error.message : String(error)}</p>
      </Alert>
    );
  }
  if (outcome === "unknown") {
    return (
      <Alert isInline variant="warning" title="No answer reached this page">
        <p>
          {verb === "create"
            ? `Whether ${subject} was created is unknown. ${kept} Submitting again is safe, and it is the right thing to do: it resends the idempotency key this form is holding, and the service answers a repeat of that key with what the first request made rather than creating a second one.`
            : `Whether ${subject} was applied is unknown. ${kept} Re-read the destination: if the generation moved, the rotation landed; if it did not, rotate again. A rotation sent twice is refused the second time (the generation no longer matches), never applied twice.`}
        </p>
        <p className="lw-subtle lw-small">{error instanceof Error ? error.message : String(error)}</p>
      </Alert>
    );
  }
  return (
    <Alert
      isInline
      variant="danger"
      title={
        outcome === "invalid"
          ? `${capital(subject)} was not ${verb === "create" ? "created" : "changed"}: fix the fields marked below.`
          : `The service refused ${subject}. Nothing was ${verb === "create" ? "created" : "changed"}.`
      }
    >
      {outcome === "refused" && <p>{error instanceof Error ? error.message : String(error)}</p>}
      {unmatched.length > 0 && (
        <ul className="lw-plain-list">
          {unmatched.map((m) => (
            <li key={m}>{m}</li>
          ))}
        </ul>
      )}
      <p>{kept}</p>
      {meta}
    </Alert>
  );
}

function capital(text: string): string {
  return text.charAt(0).toUpperCase() + text.slice(1);
}

/**
 * `When`, remounted when its instant changes: its clock ticks every 30 s from
 * mount, so an instant recorded after the last tick would otherwise read
 * "in a moment".
 */
export function Instant({ value, exact }: { value: string | null | undefined; exact?: boolean }) {
  return <When key={String(value)} value={value} exact={exact} />;
}

/** One term of a description list. */
export function Term({ term, children }: { term: string; children: ReactNode }) {
  return (
    <DescriptionListGroup>
      <DescriptionListTerm>{term}</DescriptionListTerm>
      <DescriptionListDescription>{children}</DescriptionListDescription>
    </DescriptionListGroup>
  );
}

export function NotRecorded() {
  return <span className="lw-subtle">Not recorded</span>;
}

// ------------------------------------------------------------------- probe

const PROBE_TONE: Record<ProbeState["verdict"], Tone> = {
  reachable: "success",
  unreachable: "danger",
  refused: "danger",
  probing: "progress",
  unknown: "neutral",
  never: "neutral",
};

/** The probe's verdict, and beside it "Stale" when the observation is older than the budget. */
export function ProbeLabels({ state }: { state: ProbeState }) {
  const title = `connection probe: ${PROBE_WORDS[state.verdict].toLowerCase()}${state.observedAt ? `, observed ${state.observedAt}` : ", no observation recorded"}${state.reason ? `, reason ${state.reason}` : ""}`;
  return (
    <Flex gap={{ default: "gapXs" }} flexWrap={{ default: "wrap" }} alignItems={{ default: "alignItemsCenter" }} component="span">
      <StatusLabel
        tone={PROBE_TONE[state.verdict]}
        text={PROBE_WORDS[state.verdict]}
        title={title}
        icon={state.verdict === "unknown" ? <UnknownIcon /> : undefined}
      />
      {state.stale && <StatusLabel tone="warning" text="Stale" title="Older than the 630 s freshness budget: it describes the past, not now." />}
      {!state.observed && (state.verdict === "refused" || state.verdict === "unknown") && (
        <StatusLabel tone="neutral" text="Never observed" icon={<BanIcon />} />
      )}
    </Flex>
  );
}

/** The controller's own reason, verbatim, with its gloss when this page has one. */
export function ReasonText({ reason }: { reason: string }) {
  if (!reason) return <NotRecorded />;
  const gloss = REFUSAL_GLOSS[reason];
  return (
    <>
      <code>{reason}</code>
      {gloss && <span className="lw-subtle">: {gloss}</span>}
    </>
  );
}

// ------------------------------------------------------- readiness checks

/** A check's aggregate. The recorded state and nothing else: a state this build does not know is Unknown, never Ready. */
export function PreflightStatus({ state }: { state: string }) {
  switch (state) {
    case "ready":
      return <StatusLabel tone="success" text="Ready" title="state ready" />;
    case "notReady":
      return <StatusLabel tone="danger" text="Not ready" title="state notReady" />;
    case "failed":
      return <StatusLabel tone="danger" text="Failed: no result" title="state failed" />;
    case "cancelled":
      return <StatusLabel tone="neutral" text="Cancelled: no result" title="state cancelled" icon={<BanIcon />} />;
    case "pending":
      return <StatusLabel tone="progress" text="Starting" title="state pending" />;
    case "queued":
      return <StatusLabel tone="progress" text="Queued" title="state queued" />;
    case "running":
      return <StatusLabel tone="progress" text="Running" title="state running" />;
    default:
      return <StatusLabel tone="neutral" text="Unknown" title={`state ${state}`} icon={<UnknownIcon />} />;
  }
}

function CheckVerdict({ state }: { state: string }) {
  if (state === "ready") return <StatusLabel tone="success" text="Ready" />;
  if (state === "notReady") return <StatusLabel tone="danger" text="Not ready" />;
  if (state === "skipped") return <StatusLabel tone="neutral" text="Skipped (never a pass)" icon={<BanIcon />} />;
  return <StatusLabel tone="neutral" text="Unknown" icon={<UnknownIcon />} />;
}

type CheckEntry = Preflight["checks"][number];

function scopeText(scope: CheckEntry["scope"]): string {
  const kind = scope?.kind ?? "";
  const name = scope?.name ?? "";
  if (!kind && !name) return "Not recorded";
  return kind && name ? `${kind}/${name}` : kind + name;
}

function CheckRows({ label, checks }: { label: string; checks: CheckEntry[] }) {
  const narrow = useNarrow();
  if (narrow) {
    return (
      <CompactList
        flush
        label={label}
        items={checks.map((c) => ({
          key: c.id,
          title: c.id,
          meta: [c.code, c.message, c.remedy ? `Remedy: ${c.remedy}` : ""].filter(Boolean).join(" · ") || "No finding recorded",
          status: <CheckVerdict state={c.state} />,
        }))}
      />
    );
  }
  return (
    <Table aria-label={label} variant="compact" gridBreakPoint="grid-md">
      <Thead>
        <Tr>
          <Th>Check</Th>
          <Th>Verdict</Th>
          <Th>Finding</Th>
          <Th modifier="nowrap">When</Th>
        </Tr>
      </Thead>
      <Tbody>
        {checks.map((c) => (
          <Tr key={c.id}>
            <Td dataLabel="Check" className="lw-break">
              <code>{c.id}</code>
              <div className="lw-subtle lw-small">{c.code ?? "Not recorded"}</div>
            </Td>
            <Td dataLabel="Verdict">
              <CheckVerdict state={c.state} />
              <div className="lw-subtle lw-small">{c.gating ?? "blocking"}</div>
            </Td>
            <Td dataLabel="Finding" className="lw-break">
              <div>{c.message ?? <NotRecorded />}</div>
              {c.remedy && <div className="lw-subtle lw-small">Remedy: {c.remedy}</div>}
              <div className="lw-subtle lw-small">Scope: {scopeText(c.scope)}</div>
            </Td>
            <Td dataLabel="When" modifier="nowrap">
              <div className="lw-small">
                observed <Instant value={c.observedAt} />
              </div>
              <div className="lw-subtle lw-small">
                expires <Instant value={c.expiresAt} />
              </div>
            </Td>
          </Tr>
        ))}
      </Tbody>
    </Table>
  );
}

function staleReasonText(r: Preflight["staleReasons"][number]): string {
  const subject = r.kind ? ` (${r.kind}${r.name ? `/${r.name}` : ""})` : "";
  if (r.reason === "unverifiable") return `could not be checked${subject}${r.basis ? `: ${r.basis}` : ""}`;
  return `${r.reason}${subject}`;
}

/**
 * A readiness check, from its own recorded fields: the aggregate, whether it
 * still applies, its blocking and advisory rows, and what only the run itself
 * can answer. Nothing here is computed beyond counting.
 */
export function CheckResult({
  preflight,
  stopped,
  onRunAgain,
  runAgainDisabled,
}: {
  preflight: Preflight;
  stopped: Stopped;
  onRunAgain?: () => void;
  runAgainDisabled?: boolean;
}) {
  const p = preflight;
  const running = !p.terminal;
  const answeredAtRun = p.checks.filter((c) => c.gating === "executionOnly" && (c.state === "unknown" || c.state === "skipped")).length;
  const blocking = p.checks.filter((c) => c.gating !== "advisory" && c.gating !== "executionOnly");
  return (
    <div>
      <Flex gap={{ default: "gapSm" }} alignItems={{ default: "alignItemsCenter" }} flexWrap={{ default: "wrap" }}>
        <PreflightStatus state={p.state} />
        {p.state === "ready" && answeredAtRun > 0 && (
          <span className="lw-subtle lw-small">
            {answeredAtRun} {answeredAtRun === 1 ? "item is" : "items are"} confirmed when the run executes
          </span>
        )}
        <Ident value={p.id} label="Check id" />
      </Flex>
      {running && stopped === "" && (
        <Content component="p" className="lw-subtle lw-gap-top" role="status">
          {CHECKING_SENTENCE}
        </Content>
      )}
      {running && stopped !== "" && (
        <Alert
          isInline
          variant="warning"
          className="lw-gap-top"
          title={stopped === "deadline" ? "This check did not finish" : "This check could not be read again"}
          actionLinks={
            onRunAgain && (
              <Button variant="link" isInline onClick={onRunAgain} isDisabled={runAgainDisabled}>
                Run the check again
              </Button>
            )
          }
        >
          {stopped === "deadline" ? CHECK_UNFINISHED_SENTENCE : CHECK_UNREADABLE_SENTENCE}
        </Alert>
      )}
      {!running && (
        <Flex gap={{ default: "gapSm" }} alignItems={{ default: "alignItemsCenter" }} flexWrap={{ default: "wrap" }} className="lw-gap-top">
          {p.applicable ? (
            <StatusLabel tone="success" text="Still applies" title="applicable" />
          ) : (
            <StatusLabel tone="warning" text="No longer applies" title="not applicable" />
          )}
          <span className="lw-subtle lw-small">
            {p.staleReasons.length > 0 ? `Why: ${p.staleReasons.map(staleReasonText).join("; ")}. ` : ""}
            {p.staleBasis.length > 0 ? `Compared: ${p.staleBasis.join(", ")}.` : "Compared: nothing. An empty comparison is not a match."}
          </span>
        </Flex>
      )}
      <DescriptionList isCompact isHorizontal className="lw-gap-top" horizontalTermWidthModifier={{ default: "10ch", md: "12ch" }}>
        <Term term="Observed">
          <Instant value={p.observedAt} exact />
        </Term>
        <Term term="Expires">
          <Instant value={p.expiresAt} exact />
        </Term>
        <Term term="Reason">{p.reason ? <code>{p.reason}</code> : <NotRecorded />}</Term>
        <Term term="About">
          {p.binding.referents.length > 0 ? p.binding.referents.map((r) => `${r.kind}/${r.name}`).join(", ") : <NotRecorded />}
        </Term>
      </DescriptionList>
      <Title headingLevel="h4" size="md" className="lw-gap-top">
        Blocking checks
      </Title>
      {blocking.length > 0 ? (
        <CheckRows label="Blocking checks" checks={blocking} />
      ) : (
        <Content component="p" className="lw-subtle">
          {running
            ? "No check has recorded a verdict yet."
            : "No blocking check has recorded a verdict. That is not a pass: it is an empty result, and the state above says what this check amounts to."}
        </Content>
      )}
      {p.warnings.length > 0 && (
        <>
          <Title headingLevel="h4" size="md" className="lw-gap-top">
            Advisory
          </Title>
          <CheckRows label="Advisory checks" checks={p.warnings} />
        </>
      )}
      {p.executionOnly.length > 0 && (
        <>
          <Title headingLevel="h4" size="md" className="lw-gap-top">
            Only knowable when the run executes
          </Title>
          <Content component="p" className="lw-subtle">
            These are excluded from the verdict above, and Ready never means they passed.
          </Content>
          <ul className="lw-plain-list">
            {p.executionOnly.map((e) => (
              <li key={e.id}>
                <code>{e.id}</code> {e.note}
              </li>
            ))}
          </ul>
        </>
      )}
    </div>
  );
}

/** The newest recorded test, as a summary that points at the check. A stale test is labelled, never shown as health. */
export function LastTestLine({
  test,
  noun,
  subject,
}: {
  test: { preflightId: string; state: string; stale: boolean; truncated: boolean; observedAt?: string | null } | null | undefined;
  noun: string;
  subject: string;
}) {
  if (!test) {
    return (
      <Content component="p" className="lw-subtle">
        No {noun} is recorded for this {subject}. That says what is stored, not whether it works.
      </Content>
    );
  }
  return (
    <div>
      <Flex gap={{ default: "gapSm" }} alignItems={{ default: "alignItemsCenter" }} flexWrap={{ default: "wrap" }}>
        <span>Last {noun}:</span>
        <PreflightStatus state={test.state} />
        {test.stale && <StatusLabel tone="warning" text="Stale: not health" />}
        <span className="lw-subtle lw-small">
          observed <Instant value={test.observedAt} />
        </span>
        <Ident value={test.preflightId} label="Check id" />
      </Flex>
      {test.truncated && (
        <Content component="p" className="lw-subtle lw-small">
          The search for the newest {noun} hit its page bound, so this may not be the newest one.
        </Content>
      )}
    </div>
  );
}
