// Theme A's cutover from a verified restore (PROD-04.2 §1 steps 5 and 6, §9 screens 3
// and 5): review how each captured consumer group's position maps onto the restored
// topics and what the target group holds now (the Switchover preflight), confirm the
// plan under the namespace's approval mode, then follow the Switchover the controller
// runs. The preflight's word decides what is offered; nothing changes on the target
// until the Switchover's Job runs, and that Job repeats every check.
import { useEffect, useMemo, useRef, useState } from "react";
import { Link, useNavigate, useParams, useSearch } from "@tanstack/react-router";
import { useForm, useWatch } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import {
  Alert,
  AlertActionLink,
  Breadcrumb,
  BreadcrumbItem,
  Button,
  Card,
  CardBody,
  CardHeader,
  CardTitle,
  Checkbox,
  Content,
  DescriptionList,
  DescriptionListDescription,
  DescriptionListGroup,
  DescriptionListTerm,
  Flex,
  FlexItem,
  Form,
  FormGroup,
  FormHelperText,
  HelperText,
  HelperTextItem,
  PageSection,
  ProgressStep,
  ProgressStepper,
  TextInput,
} from "@patternfly/react-core";
import ExchangeAltIcon from "@patternfly/react-icons/dist/esm/icons/exchange-alt-icon";
import LockIcon from "@patternfly/react-icons/dist/esm/icons/lock-icon";
import { useCan } from "../../api/session";
import { useRestore } from "../../api/queries";
import { ApiError } from "../../api/client";
import { useApprovalPolicy, type ApprovalPolicy } from "../../api/approvals";
import {
  EVIDENCE_UNREADABLE_WORDS,
  UNKNOWN_OUTCOME,
  chosenByDefault,
  evidenceDigests,
  evidenceUnreadable,
  groupsPartial,
  isTerminal,
  mintNames,
  offered,
  outcomeUnknown,
  parseOwnPlan,
  planDocument,
  plannedPositions,
  reasonWords,
  reviewGroups,
  reviewState,
  unavailablePositions,
  useCreateSwitchover,
  usePreflight,
  usePreflightDetails,
  useStartReview,
  useSwitchover,
  verdictOf,
  type Preflight,
  type Restore,
  type ReviewGroup,
  type Switchover,
} from "../../api/switchovers";
import { PageHeader } from "../../components/PageHeader";
import { VerdictStatus } from "../../components/Status";
import { Empty, ErrorState, LoadingRows } from "../../components/States";
import { Ident } from "../../components/Ident";
import { useSha256 } from "../../components/hash";
import { useNarrow } from "../../components/CompactList";
import { Legend, OutcomeCounts, OutcomeTable, PlanBlock, ReviewCards, ReviewTable, RollbackCard, planCounts, plural } from "./parts";
import { PhaseLabel } from "./status";
import "./cutover.css";
import { ErrorText } from "../../components/Prose";
import { exactUtc } from "../../components/When";

/** Why this restore cannot be switched over, from the restore alone (§3.4's admission). */
export function cutoverRefusal(r: Restore): { word: string; title: string; body: string } | null {
  if (r.target.mode === "scratch") return { word: "RestoreIsScratch", title: "A clone cannot be switched over", body: "This restore is a clone (scratch mode): its topics are deleted when it expires, so consumers are never moved onto them." };
  if (!r.operation.terminal) return { word: "RestoreNotSucceeded", title: "The restore has not finished", body: "Positions are mapped when the restore passes its complete verification. Come back when it is verified." };
  if (r.operation.state !== "succeeded" || !r.operation.verifiedSuccess) return { word: "RestoreNotSucceeded", title: "This restore cannot be switched over", body: "Only a restore that succeeded with a verified scorecard has positions to apply." };
  if (r.coverage.recorded !== "complete" || r.coverage.covered === false) {
    return { word: "VerificationNotComplete", title: "No positions to switch over", body: "This restore was not verified completely. Positions are mapped only by a complete verification of every partition; restore again with complete verification to switch consumers over." };
  }
  return null;
}

type Step = "review" | "confirm" | "done";

function Steps({ step, sw }: { step: Step; sw?: Switchover }) {
  const index = step === "review" ? 0 : step === "confirm" ? 1 : 2;
  const ended = sw && isTerminal(sw);
  const last = !ended ? (index === 2 ? "info" : "pending") : sw.phase === "Succeeded" ? "success" : sw.phase === "PartiallyApplied" ? "warning" : "danger";
  return (
    <ProgressStepper isCenterAligned aria-label="Cutover steps" className="lw-stepper">
      {["Review positions", "Confirm", "Switched over"].map((s, i) => (
        <ProgressStep
          key={s}
          id={`co-${i}`}
          titleId={`co-${i}-t`}
          variant={i === 2 ? last : i < index ? "success" : i === index ? "info" : "pending"}
          isCurrent={i === index && !ended}
        >
          {s}
        </ProgressStep>
      ))}
    </ProgressStepper>
  );
}

// ------------------------------------------------------------------- review

function Review({
  restore,
  pf,
  groups,
  notes,
  checking,
  onCheck,
  selected,
  setSelected,
  onContinue,
}: {
  restore: Restore;
  pf: Preflight;
  groups: ReviewGroup[];
  notes: string[];
  checking: boolean;
  onCheck: () => void;
  selected: Set<string>;
  setSelected: (s: Set<string>) => void;
  onContinue: () => void;
}) {
  const narrow = useNarrow(991);
  const target = restore.target.clusterRef.name;
  const by = (v: ReturnType<typeof verdictOf>["verdict"]) => groups.filter((g) => verdictOf(g).verdict === v);
  const waiting = by("stopConsumers");
  const blocked = by("blocked");
  const chosen = groups.filter((g) => selected.has(g.source) && offered(g));
  const props = { groups, selected, setSelected, target };
  return (
    <PageSection>
      <Card>
        <CardHeader>
          <CardTitle component="h2">{plural(groups.length, "consumer group")} with mapped positions</CardTitle>
        </CardHeader>
        <CardBody>
          <ul className="lw-verdicts" aria-label="Groups by verdict">
            {(["ready", "alreadyApplied", "stopConsumers", "blocked"] as const).map((v) => (
              <li key={v}>
                <VerdictStatus verdict={v} />
                <strong className="lw-num">{by(v).length}</strong>
              </li>
            ))}
          </ul>
          <Legend />
          {narrow ? <ReviewCards {...props} /> : <ReviewTable {...props} />}
          {waiting.length > 0 && (
            <Alert
              isInline
              variant="warning"
              className="lw-gap-top"
              title={`${waiting.map((g) => g.source).join(", ")} still ${waiting.length === 1 ? "has" : "have"} consumers connected to ${target}`}
              actionLinks={
                <AlertActionLink onClick={onCheck} isDisabled={checking}>
                  {checking ? "Checking" : "Check again"}
                </AlertActionLink>
              }
            >
              A switchover would refuse {waiting.length === 1 ? "it" : "them"} (GroupActive): the broker takes a position only while a group has
              no members. Stop those consumers, then check again.
            </Alert>
          )}
          {blocked.length > 0 && (
            <Alert isInline variant="danger" title={`${blocked.map((g) => g.source).join(", ")} cannot be switched over`} className="lw-gap-top">
              The review refused {blocked.length === 1 ? "it" : "them"}; open the group to see why. Nothing is committed for a group left out:
              reset it by hand on {target} if its application moves there.
            </Alert>
          )}
          {notes.map((n) => (
            <Content key={n} component="p" className="lw-subtle lw-small lw-gap-top">
              {n}
            </Content>
          ))}
          <Content component="p" className="lw-subtle lw-small lw-gap-top">
            Checked {pf.observedAt ? exactUtc(pf.observedAt) : "just now"} by the
            switchover review ({pf.id}). Groups the backup did not capture, and groups with nothing mapped, are not listed.
          </Content>
        </CardBody>
      </Card>
      <Flex className="lw-gap-top" gap={{ default: "gapMd" }} flexWrap={{ default: "wrap" }}>
        <FlexItem>
          <Button variant="primary" isDisabled={chosen.length === 0} onClick={onContinue}>
            Continue with {plural(chosen.length, "group")}
          </Button>
        </FlexItem>
        <FlexItem>
          <Button variant="secondary" onClick={onCheck} isLoading={checking} isDisabled={checking}>
            {checking ? "Checking" : "Check again"}
          </Button>
        </FlexItem>
      </Flex>
    </PageSection>
  );
}

// ------------------------------------------------------------------ confirm

function modeSentence(ns: string, p: ApprovalPolicy | undefined): { text: string; blocked?: string } {
  if (!p) return { text: "The namespace's approval mode could not be read; the service applies it anyway." };
  switch (p.operatorMode) {
    case "confirm":
      return p.ordinaryConfirmationAvailable
        ? { text: `${ns} uses one-person confirmation: confirming here approves the switchover.` }
        : { text: `${ns} uses one-person confirmation.`, blocked: "This console cannot confirm here yet: its confirmation key is not configured, so the request would be refused and nothing created." };
    case "two-person":
      return p.consoleApprovalAvailable
        ? { text: `${ns} needs a second person: an approver sees every group and position below in Approvals, and nothing runs until they approve.` }
        : { text: `${ns} needs a second person.`, blocked: "This console cannot take a second person's approval here (it is the administrator console, or its key is missing)." };
    default:
      return p.legacy
        ? { text: `${ns} has no approval policy: an approver runs logweir drill approve --subject-kind Switchover over the plan and uploads both files in Approvals.` }
        : { text: `${ns} needs a personal-key approval: an approver countersigns the console's request with logweir drill countersign and uploads the signature in Approvals.` };
  }
}

function Confirm({
  ns,
  restore,
  pf,
  chosen,
  left,
  attempt,
  onBack,
}: {
  ns: string;
  restore: Restore;
  pf: Preflight;
  chosen: ReviewGroup[];
  left: ReviewGroup[];
  attempt: string;
  onBack: () => void;
}) {
  const navigate = useNavigate();
  const policy = useApprovalPolicy(ns);
  const create = useCreateSwitchover(ns);
  const digests = evidenceDigests(pf);
  const target = restore.target.clusterRef.name;
  const doc = useMemo(
    () => (digests ? planDocument({ name: restore.name, uid: restore.uid, planHash: restore.planHash, ...digests }, chosen) : { refused: "The review did not state the scorecard's and the mapping's digests, so the console cannot bind a plan to them. Build it with logweir switchover plan." }),
    [digests?.scorecardSha256, digests?.mappingSha256, restore.name, restore.uid, restore.planHash, chosen],
  );
  const bytes = "bytes" in doc ? doc.bytes : "";
  const hash = useSha256(bytes);
  const ready = "bytes" in doc && hash.startsWith("sha256:");
  const names = ready ? mintNames(hash, attempt) : null;
  const p = policy.data;
  const mode = modeSentence(ns, p);
  const ticketShown = !p?.legacy;
  const schema = useMemo(
    () =>
      z.object({
        ticket: p?.ticketRequired
          ? z.string().trim().min(1, "This namespace's policy requires a change ticket.").max(128, "At most 128 characters.").regex(/^[\x20-\x7e]*$/, "Printable characters only.")
          : z.string().trim().max(128, "At most 128 characters.").regex(/^[\x20-\x7e]*$/, "Printable characters only."),
        stopped: z.literal(true, { error: "Confirm that these groups' consumers stay stopped." }),
      }),
    [p?.ticketRequired],
  );
  const form = useForm<{ ticket: string; stopped: boolean }>({ resolver: zodResolver(schema) as never, defaultValues: { ticket: "", stopped: false } });
  const stopped = useWatch({ control: form.control, name: "stopped" });
  const planned = chosen.flatMap(plannedPositions);
  const c = planCounts(chosen);
  const created = chosen.filter((g) => g.absent);
  const partial = chosen.filter((g) => unavailablePositions(g).length > 0);
  const err = create.error;
  const fieldErrors = err instanceof ApiError ? err.fieldErrors : [];
  useEffect(() => {
    for (const f of fieldErrors) if (f.field === "ticket") form.setError("ticket", { message: f.message });
  }, [err]);

  const submit = form.handleSubmit(async (values) => {
    if (!names || !("bytes" in doc)) return;
    const res = await create.mutateAsync({
      restore: { name: restore.name, uid: restore.uid },
      planBytes: doc.bytes,
      planHash: hash,
      approvalName: names.approvalName,
      key: names.key,
      ...(ticketShown && values.ticket.trim() ? { ticket: values.ticket.trim() } : {}),
    }).catch(() => null);
    if (res) void navigate({ to: "/$ns/restores/$name/cutover", params: { ns, name: restore.name }, search: { switchover: res.item.name }, replace: true });
  });
  const confirmWord = p?.operatorMode === "confirm" ? `Switch over ${plural(chosen.length, "group")}` : `Request a switchover of ${plural(chosen.length, "group")}`;

  return (
    <PageSection>
      <Card>
        <CardHeader>
          <CardTitle component="h2">
            Switch {plural(chosen.length, "group")} over on {target}
          </CardTitle>
        </CardHeader>
        <CardBody>
          {"refused" in doc ? (
            <Alert isInline variant="danger" title="No plan can be built from this review">
              {doc.refused}
            </Alert>
          ) : (
            <DescriptionList isCompact className="lw-dl" orientation={{ sm: "vertical", md: "horizontal" }}>
              <DescriptionListGroup>
                <DescriptionListTerm>Groups</DescriptionListTerm>
                <DescriptionListDescription>
                  <ul className="lw-plain-list">
                    {chosen.map((g) => (
                      <li key={g.source}>
                        {g.source}
                        <span className="lw-subtle">
                          {g.absent ? " (created on the target)" : ""}
                          {unavailablePositions(g).length > 0 ? ` (${plural(unavailablePositions(g).length, "unavailable partition")} not planned)` : ""}
                        </span>
                      </li>
                    ))}
                  </ul>
                </DescriptionListDescription>
              </DescriptionListGroup>
              <DescriptionListGroup>
                <DescriptionListTerm>Positions</DescriptionListTerm>
                <DescriptionListDescription>
                  {planned.length}: {c.exact} exact, {c.approximate} approximate
                </DescriptionListDescription>
              </DescriptionListGroup>
              <DescriptionListGroup>
                <DescriptionListTerm>Each partition</DescriptionListTerm>
                <DescriptionListDescription>
                  Committed only if the group holds no position there when the run reads it (<span className="lw-mono">expected: none</span>).
                  A group that moved since this review is refused whole (TargetPositionsChanged), and nothing is committed for it.
                </DescriptionListDescription>
              </DescriptionListGroup>
              <DescriptionListGroup>
                <DescriptionListTerm>Saved first</DescriptionListTerm>
                <DescriptionListDescription>Every group's positions on {target}, read before any commit, in the signed switchover record.</DescriptionListDescription>
              </DescriptionListGroup>
              <DescriptionListGroup>
                <DescriptionListTerm>Left out</DescriptionListTerm>
                <DescriptionListDescription>
                  {left.length === 0
                    ? "None"
                    : left.map((g) => {
                        const v = verdictOf(g);
                        return (
                          <div key={g.source}>
                            {g.source}{" "}
                            <span className="lw-reason">
                              {v.verdict === "alreadyApplied" ? "alreadyApplied" : v.reason ?? (offered(g) ? "not chosen" : "nothing to commit")}
                            </span>
                          </div>
                        );
                      })}
                </DescriptionListDescription>
              </DescriptionListGroup>
              <DescriptionListGroup>
                <DescriptionListTerm>Approval</DescriptionListTerm>
                <DescriptionListDescription>{mode.text}</DescriptionListDescription>
              </DescriptionListGroup>
              <DescriptionListGroup>
                <DescriptionListTerm>Restore plan</DescriptionListTerm>
                <DescriptionListDescription>
                  <Ident value={restore.planHash} label="restore plan sha256" />
                  <div className="lw-subtle lw-small">Restore {restore.name}'s: the target cluster and its credentials come from it.</div>
                </DescriptionListDescription>
              </DescriptionListGroup>
              <DescriptionListGroup>
                <DescriptionListTerm>Signed scorecard</DescriptionListTerm>
                <DescriptionListDescription>
                  <Ident value={digests!.scorecardSha256} label="scorecard sha256" />
                </DescriptionListDescription>
              </DescriptionListGroup>
              <DescriptionListGroup>
                <DescriptionListTerm>Positions mapping</DescriptionListTerm>
                <DescriptionListDescription>
                  <Ident value={digests!.mappingSha256} label="positions mapping sha256" />
                  <div className="lw-subtle lw-small">The one the scorecard binds; the run checks every planned position against it.</div>
                </DescriptionListDescription>
              </DescriptionListGroup>
              <DescriptionListGroup>
                <DescriptionListTerm>Plan</DescriptionListTerm>
                <DescriptionListDescription>
                  {ready ? <Ident value={hash} label="switchover plan sha256" /> : <span className="lw-subtle">{hash}</span>}
                </DescriptionListDescription>
              </DescriptionListGroup>
            </DescriptionList>
          )}
          {"bytes" in doc && (
            <div className="lw-gap-top">
              <PlanBlock bytes={doc.bytes} name={restore.name} />
            </div>
          )}
          <Alert isInline variant="info" title="Undoing a switchover has limits" className="lw-gap-top">
            A rollback is a new switchover built from this one's record. It puts the saved positions back only while no consumer has
            committed on {target} since; after that it is refused, naming the records read. These groups hold no position on these
            partitions now{created.length > 0 ? ` (${created.map((g) => g.source).join(", ")} does not exist yet)` : ""}, and a position that
            did not exist cannot be put back through Logweir: undoing means removing it with kafka-consumer-groups.sh --delete-offsets.
          </Alert>
          {partial.length > 0 && (
            <Alert isInline variant="warning" className="lw-gap-top" title={`${partial.map((g) => g.source).join(", ")}: some partitions are not planned`}>
              Their unavailable partitions get no committed position; those consumers start there from their own auto.offset.reset.
            </Alert>
          )}
          {mode.blocked && (
            <Alert isInline variant="danger" className="lw-gap-top" title="This console cannot submit it here">
              {mode.blocked}
            </Alert>
          )}
          <Form className="lw-gap-top" onSubmit={(e) => void submit(e)}>
            {ticketShown && (
              <FormGroup label="Change ticket" fieldId="ticket" isRequired={p?.ticketRequired}>
                <TextInput id="ticket" {...form.register("ticket")} validated={form.formState.errors.ticket ? "error" : "default"} aria-describedby="ticket-help" />
                <FormHelperText>
                  <HelperText id="ticket-help">
                    <HelperTextItem variant={form.formState.errors.ticket ? "error" : "default"}>
                      {form.formState.errors.ticket?.message ?? (p?.ticketRequired ? "Required here; signed beside the plan." : "Optional; signed beside the plan.")}
                    </HelperTextItem>
                  </HelperText>
                </FormHelperText>
              </FormGroup>
            )}
            <FormGroup fieldId="stopped">
              <Checkbox
                id="stopped"
                isChecked={stopped}
                onChange={(_e, on) => form.setValue("stopped", on, { shouldValidate: form.formState.isSubmitted })}
                label="The consumers of these groups stay stopped until the switchover finishes."
              />
              {form.formState.errors.stopped && (
                <FormHelperText>
                  <HelperText>
                    <HelperTextItem variant="error">{form.formState.errors.stopped.message}</HelperTextItem>
                  </HelperText>
                </FormHelperText>
              )}
            </FormGroup>
            {err && outcomeUnknown(err) && (
              <Alert isInline variant="warning" title="The outcome is not known">
                {UNKNOWN_OUTCOME}
              </Alert>
            )}
            {err && !outcomeUnknown(err) && (
              <Alert isInline variant="danger" title={reasonWords(err.message) ? "Refused" : "The service refused the switchover"}>
                <ErrorText error={err} />
                {fieldErrors.filter((f) => f.field !== "ticket").map((f) => (
                  <div key={f.field + f.code} className="lw-small">
                    <span className="lw-mono">{f.field}</span>: {f.message}
                  </div>
                ))}
              </Alert>
            )}
            <Flex gap={{ default: "gapMd" }} flexWrap={{ default: "wrap" }}>
              <FlexItem>
                <Button
                  type="submit"
                  variant="primary"
                  icon={<ExchangeAltIcon />}
                  isDisabled={!ready || !stopped || !!mode.blocked || create.isPending || policy.isPending}
                  isLoading={create.isPending}
                >
                  {err && outcomeUnknown(err) ? "Submit again" : confirmWord}
                </Button>
              </FlexItem>
              <FlexItem>
                <Button variant="secondary" onClick={onBack} isDisabled={create.isPending}>
                  Back
                </Button>
              </FlexItem>
            </Flex>
          </Form>
        </CardBody>
      </Card>
    </PageSection>
  );
}

// --------------------------------------------------------------- switched over

/** The Switchover after the click: held for approval, running, then each group's outcome. */
export function SwitchoverResult({ ns, name, restore, onPage = false }: { ns: string; name: string; restore?: Restore; onPage?: boolean }) {
  const sw = useSwitchover(ns, name);
  const policy = useApprovalPolicy(ns);
  if (sw.isPending) return <LoadingRows rows={4} />;
  if (sw.isError) return <ErrorState error={sw.error} onRetry={() => void sw.refetch()} />;
  const s = sw.data;
  const plan = s.planBytes ? parseOwnPlan(s.planBytes) : null;
  const target = restore?.target.clusterRef.name ?? "the target";
  const waiting = s.phase === "Pending" && s.reason === "ApprovalNotVerified";
  const left = s.groups.filter((g) => g.outcome !== "applied" && g.outcome !== "alreadyApplied");
  return (
    <>
      <Card>
        <CardHeader>
          {onPage ? (
            <CardTitle component="h2">Outcome per group</CardTitle>
          ) : (
            <Flex alignItems={{ default: "alignItemsCenter" }} gap={{ default: "gapSm" }}>
              <CardTitle component="h2">
                Switchover <Link to="/$ns/switchovers/$name" params={{ ns, name: s.name }}>{s.name}</Link>
              </CardTitle>
              <PhaseLabel sw={s} />
            </Flex>
          )}
        </CardHeader>
        <CardBody>
          {waiting && (
            <Alert isInline variant="info" title="Waiting for approval" className="lw-gap-below">
              {policy.data?.operatorMode === "two-person"
                ? "A second person approves it in Approvals, seeing every group and position. Nothing runs before that."
                : "An approver signs it with their personal key and uploads the signature in Approvals. Nothing runs before that."}{" "}
              {policy.data?.operatorMode === "two-person" ? (
                // DUI-27: its own approval page, which the second person opens from Approvals too.
                <Link to="/$ns/approvals/switchover/$name" params={{ ns, name: s.name }}>
                  Open its approval
                </Link>
              ) : (
                <Link to="/$ns/approvals" params={{ ns }}>
                  Approvals
                </Link>
              )}
            </Alert>
          )}
          {s.phase === "Refused" && (
            <Alert isInline variant="danger" title={`Refused: ${s.reason ?? "no reason recorded"}`} className="lw-gap-below">
              <p>{reasonWords(s.reason) ?? "The switchover was refused."} Nothing was committed.</p>
              {s.message && <p className="lw-break">{s.message}</p>}
            </Alert>
          )}
          {s.phase === "Failed" && (
            <Alert isInline variant="danger" title={`Failed: ${s.reason ?? "no reason recorded"}`} className="lw-gap-below">
              <p>{reasonWords(s.reason) ?? "The switchover run failed; its signed record says what it applied."}</p>
              {s.message && <p className="lw-break">{s.message}</p>}
            </Alert>
          )}
          {groupsPartial(s) && (
            <Alert isInline variant="warning" title="Not every group is listed here" className="lw-gap-below">
              {s.message}
            </Alert>
          )}
          <OutcomeTable sw={s} plan={plan} target={target} />
          <OutcomeCounts sw={s} />
          {s.recordKey && (
            <DescriptionList isCompact className="lw-dl lw-gap-top" orientation={{ sm: "vertical", md: "horizontal" }}>
              <DescriptionListGroup>
                <DescriptionListTerm>Switchover record</DescriptionListTerm>
                <DescriptionListDescription>
                  <Ident value={s.recordKey} label="switchover record key" />
                  <div className="lw-subtle lw-small">
                    Signed by the run: every partition's position before and after. Check it with logweir drill verify --payload-type
                    switchover-record.
                  </div>
                </DescriptionListDescription>
              </DescriptionListGroup>
            </DescriptionList>
          )}
          {s.phase === "Succeeded" && (
            <Alert isInline variant="success" title={`Start your consumers on ${target}`} className="lw-gap-top">
              {s.groups.map((g) => g.target).join(", ")} now resume at the translated positions.
            </Alert>
          )}
          {s.phase === "PartiallyApplied" && (
            <Alert isInline variant="warning" title={groupsPartial(s) ? "Some groups not switched over" : `${plural(left.length, "group")} not switched over`} className="lw-gap-top">
              {left.map((g) => `${g.target} (${g.reason ?? g.outcome})`).join(", ")}. The groups that were applied can start on {target}. Review
              again to switch the rest: the applied ones then read as already switched.
            </Alert>
          )}
        </CardBody>
      </Card>
      {isTerminal(s) && (
        <div className="lw-gap-top">
          <RollbackCard sw={s} plan={plan} />
        </div>
      )}
    </>
  );
}

// ---------------------------------------------------------------------- page

/** The route's page. Leaving the third step for a new review ("Review again") mounts the
 *  flow afresh, so no earlier review, choice or step carries over. */
export function Cutover() {
  const search = useSearch({ from: "/$ns/restores/$name/cutover" });
  return <CutoverFlow key={search.switchover ? `sw:${search.switchover}` : "review"} />;
}

function CutoverFlow() {
  const { ns, name } = useParams({ from: "/$ns/restores/$name/cutover" });
  const search = useSearch({ from: "/$ns/restores/$name/cutover" });
  const navigate = useNavigate();
  // Read again on every visit (DUI-28): a copy cached while the run was going would refuse
  // a restore that has since succeeded.
  const restore = useRestore(ns, name, { fresh: true });
  // DUI-30: a review is a write (POST preflights), and so is a switchover. A role that may
  // not create a restore here starts neither and is offered neither.
  const canWrite = useCan(ns, "restoreCreate");
  const start = useStartReview(ns);
  const [reviewId, setReviewId] = useState<string>();
  const [rounds, setRounds] = useState(0);
  const [step, setStep] = useState<Step>("review");
  // The operator's choice, with the groups that were on screen when it was made: a default
  // group the operator unticked stays unticked; one offered only since is added.
  const [choice, setChoice] = useState<{ selected: Set<string>; seen: Set<string> } | null>(null);
  const started = useRef(false);
  const pf = usePreflight(ns, reviewId);
  const details = usePreflightDetails(ns, pf.data);
  const switchoverName = search.switchover;
  const sw = useSwitchover(ns, switchoverName);
  const r = restore.isFetchedAfterMount ? restore.data : undefined;
  const refusal = r ? cutoverRefusal(r) : null;

  const beginReview = () => {
    if (!r) return;
    start.mutate(
      { name: r.name, uid: r.uid },
      {
        onSuccess: (item) => {
          setReviewId(item.id);
          setRounds((n) => n + 1);
        },
      },
    );
  };
  useEffect(() => {
    if (r && !refusal && !switchoverName && canWrite && !started.current) {
      started.current = true;
      beginReview();
    }
  }, [r, refusal, switchoverName, canWrite]);

  const state = pf.data ? reviewState(pf.data) : null;
  const read = useMemo(() => (details.data ? reviewGroups(details.data) : null), [details.data]);
  const groups = state?.kind === "decided" && read ? read.groups : [];
  // The default choice: every group offered with every partition mapped. A re-check keeps
  // what was chosen and still offered, and adds what became offered since.
  const picked = useMemo(() => {
    const base = groups.filter(chosenByDefault).map((g) => g.source);
    if (!choice) return new Set(base);
    const added = base.filter((s) => !choice.seen.has(s));
    return new Set([...choice.selected, ...added].filter((s) => groups.some((g) => g.source === s && offered(g))));
  }, [groups, choice]);
  const setSelected = (next: Set<string>) => setChoice({ selected: next, seen: new Set(groups.map((g) => g.source)) });
  const chosen = groups.filter((g) => picked.has(g.source) && offered(g));
  const left = groups.filter((g) => !chosen.includes(g));

  const breadcrumb = (
    <Breadcrumb className="lw-breadcrumb">
      <BreadcrumbItem render={() => <Link to="/$ns/restores" params={{ ns }}>Restores</Link>} />
      <BreadcrumbItem render={() => <Link to="/$ns/restores/$name" params={{ ns, name }}>{name}</Link>} />
      {switchoverName ? (
        <>
          {/* DUI-28: the way back to a new review of this restore. */}
          <BreadcrumbItem render={() => <Link to="/$ns/restores/$name/cutover" params={{ ns, name }} search={{}}>Switch consumers over</Link>} />
          <BreadcrumbItem isActive>{switchoverName}</BreadcrumbItem>
        </>
      ) : (
        <BreadcrumbItem isActive>Switch consumers over</BreadcrumbItem>
      )}
    </Breadcrumb>
  );
  const header = (description?: string) => <PageHeader title="Switch consumers over" breadcrumb={breadcrumb} description={description} />;

  if (restore.isError || !r) {
    return (
      <>
        {header()}
        <PageSection>{restore.isError ? <ErrorState error={restore.error} onRetry={() => void restore.refetch()} /> : <LoadingRows rows={6} />}</PageSection>
      </>
    );
  }
  const target = r.target.clusterRef.name;

  if (switchoverName) {
    return (
      <>
        {header(`The switchover applies the reviewed positions on ${target}. Its Job checks everything again before any commit.`)}
        <PageSection>
          <Steps step="done" sw={sw.data} />
        </PageSection>
        <PageSection>
          <SwitchoverResult ns={ns} name={switchoverName} restore={r} />
          {/* DUI-28: after any outcome, a Succeeded one included, a new review is one click. */}
          {sw.data && isTerminal(sw.data) && !refusal && canWrite && (
            <Flex className="lw-gap-top" gap={{ default: "gapMd" }}>
              <FlexItem>
                <Button variant="secondary" onClick={() => void navigate({ to: "/$ns/restores/$name/cutover", params: { ns, name }, search: {} })}>
                  Review again
                </Button>
              </FlexItem>
            </Flex>
          )}
        </PageSection>
      </>
    );
  }

  if (refusal) {
    return (
      <>
        {header()}
        <PageSection>
          <Card>
            <Empty
              title={refusal.title}
              actions={
                <Button variant="secondary" onClick={() => void navigate({ to: "/$ns/restores/$name", params: { ns, name } })}>
                  Back to the restore
                </Button>
              }
            >
              {refusal.body} <span className="lw-reason">{refusal.word}</span>
            </Empty>
          </Card>
        </PageSection>
      </>
    );
  }

  if (!canWrite) {
    return (
      <>
        {header()}
        <PageSection>
          <Card>
            <Empty
              title="Your role does not switch consumers over"
              icon={LockIcon}
              actions={
                <Button variant="secondary" onClick={() => void navigate({ to: "/$ns/restores/$name", params: { ns, name } })}>
                  Back to the restore
                </Button>
              }
            >
              Your sign-in reads restores and switchovers in {ns}. Reviewing the positions and switching consumers over is for a role that
              may create restores here, such as operator. Nothing was sent.
            </Empty>
          </Card>
        </PageSection>
      </>
    );
  }

  const checking = start.isPending || (!!pf.data && !pf.data.terminal) || details.isFetching;
  const notes = [
    ...(read?.truncated ? ["The review's details were cut at their size bound: groups past it are not listed. Build the plan with logweir switchover plan instead."] : []),
    ...(read && read.unread > 0 ? [`${plural(read.unread, "line")} of the review could not be read; ${read.unread === 1 ? "its group is" : "those groups are"} not offered.`] : []),
    ...(rounds > 1 ? ["Checked again just now."] : []),
  ];
  let body: React.ReactNode;
  if (start.isError) body = <ErrorState error={start.error} onRetry={beginReview} />;
  else if (pf.isError) body = <ErrorState error={pf.error} onRetry={() => void pf.refetch()} />;
  else if (!pf.data || state?.kind === "running" || (state?.kind === "decided" && !details.data && !details.isError)) {
    body = (
      <Card>
        <CardBody>
          <Content component="p" className="lw-subtle">
            Reading the restore's signed mapping and each consumer group on {target}. Nothing is committed by this check.
          </Content>
          <LoadingRows rows={4} label="Reviewing positions" />
        </CardBody>
      </Card>
    );
  } else if (details.isError) body = <ErrorState error={details.error} onRetry={() => void details.refetch()} />;
  else if (state?.kind === "refused") {
    body = (
      <Card>
        <CardBody>
          <Alert
            isInline
            variant="danger"
            title={state.checks.every((c) => evidenceUnreadable(c.message)) ? "The review could not read the restore's evidence" : "The review refused this restore"}
          >
            {state.checks.every((c) => evidenceUnreadable(c.message)) && <p>{EVIDENCE_UNREADABLE_WORDS}</p>}
            {state.checks.map((c) => (
              <div key={c.id}>
                <span className="lw-reason">{c.code ?? c.id}</span> {c.message}
                {c.remedy && <div className="lw-subtle lw-small">{c.remedy}</div>}
              </div>
            ))}
          </Alert>
          <Button variant="secondary" className="lw-gap-top" onClick={beginReview} isLoading={checking} isDisabled={checking}>
            Check again
          </Button>
        </CardBody>
      </Card>
    );
  } else if (state?.kind === "undecided") {
    body = (
      <Card>
        <CardBody>
          <Alert isInline variant="warning" title="The review did not decide">
            {state.why} Nothing is offered without its answer.
          </Alert>
          <Button variant="secondary" className="lw-gap-top" onClick={beginReview} isLoading={checking} isDisabled={checking}>
            Check again
          </Button>
        </CardBody>
      </Card>
    );
  } else if (groups.length === 0) {
    body = (
      <Card>
        <Empty title="No consumer group has a mapped position">
          The restore's mapping holds no group with a position on the restored topics: the backup captured none, or none maps. There is
          nothing to switch over.
        </Empty>
      </Card>
    );
  }

  return (
    <>
      {header(`Move consumer groups to the restored topics on ${target}. Each captured position was mapped by the restore's complete verification, and the groups must be stopped. Nothing changes on ${target} until the switchover runs.`)}
      <PageSection>
        <Steps step={step} />
      </PageSection>
      {body ? (
        <PageSection>{body}</PageSection>
      ) : step === "review" ? (
        <Review
          restore={r}
          pf={pf.data!}
          groups={groups}
          notes={notes}
          checking={checking}
          onCheck={beginReview}
          selected={picked}
          setSelected={setSelected}
          onContinue={() => setStep("confirm")}
        />
      ) : (
        <Confirm ns={ns} restore={r} pf={pf.data!} chosen={chosen} left={left} attempt={`r${rounds}-${pf.data!.id}`} onBack={() => setStep("review")} />
      )}
    </>
  );
}
