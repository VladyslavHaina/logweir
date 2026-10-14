// The restore wizard: five steps, each answering one question, one action at the end.
// Point, topics and time, target, readiness, review. The plan bytes are rendered in the
// browser in the runner's grammar (./plan.ts, held to ui/'s goldens), hashed with
// crypto.subtle, and sent unchanged; the hash on screen is the hash the approver signs.
import { ApiError, LocalRefusal } from "../../api/client";
import { useEffect, useMemo, useRef, useState } from "react";
import { Link, useNavigate, useParams, useSearch } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import { useForm, useWatch } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import {
  Alert,
  Breadcrumb,
  BreadcrumbItem,
  Button,
  Card,
  PageSection,
  Wizard,
  WizardFooterWrapper,
  WizardStep,
  useWizardContext,
} from "@patternfly/react-core";
import { PageHeader } from "../../components/PageHeader";
import { ErrorState, LoadingRows } from "../../components/States";
import {
  cancelPreflight,
  readDestination,
  readCatalogOffers,
  readLatestDiscoveries,
  readPreflight,
  startPreflight,
  useApprovalPolicy,
  useBackupList,
  useCapabilities,
  useConnectionList,
  useCreateRestore,
  useDestinationList,
} from "../../api/restores";
import { INSECURE_CONTEXT_SENTENCE, NO_TOPIC_CONFIGURATION, planHashAvailable, preparePlanDocument, renderPlanBytes, type PreparedPlan } from "./plan";
import { frozenDestinationProblem, isRecoveryPoint, recoveryPoints, refusalSentence, resolvePoint, type Resolution } from "./points";
import { resolveBackup, resolveCatalogPoint, type Resolved } from "./resolve";
import {
  BROKERS_NOT_READ,
  defaultPrefixFor,
  endInstant,
  planFields,
  policyRefusal,
  readinessRefusal,
  readinessRequest,
  replicationChoice,
  restoreBody,
  restoreIdempotencyKey,
  targetBrokerFact,
  typedTopicsProblem,
  validate,
  type WizardContext,
  type WizardValues,
} from "./model";
import { CheckStep, PointStep, ReviewStep, TargetStep, WhatStep, Resolution as ResolutionView, offerKeyOf, pointKeyOf, type ReadinessState } from "./WizardSteps";
import { Coded, WriteProblem } from "./parts";
import { TOPIC_CONFIGURATION_CHECK, TopicConfigurationPreview, topicPreviewLines } from "../cutover/TopicConfiguration";
import { usePreflightDetails } from "../../api/switchovers";
import "./restores.css";

/** The wizard's address: `?backup=` (and `uid`) or `?catalog=&point=`, and `retryOf`. */
export interface RestoreSearch {
  backup?: string;
  uid?: string;
  catalog?: string;
  point?: string;
  retryOf?: string;
}

export function restoreSearch(search: Record<string, unknown>): RestoreSearch {
  const out: RestoreSearch = {};
  for (const key of ["backup", "uid", "catalog", "point", "retryOf"] as const) {
    if (typeof search[key] === "string" && search[key]) out[key] = search[key] as string;
  }
  return out;
}

type StepId = "point" | "what" | "target" | "check" | "review";

/** Which step each value is chosen on: Next validates those, and an API refusal opens there. */
export const FIELD_STEP: Record<string, StepId> = {
  pointKey: "point",
  archive: "point",
  topics: "what",
  topicsText: "what",
  pointInTime: "what",
  completeMaxRecords: "what",
  target: "target",
  prefix: "target",
  noOwner: "target",
  scratch: "target",
  cloneTtl: "target",
  topicConfiguration: "target",
  replication: "target",
  evidenceDestination: "target",
  archiveSecret: "target",
  evidenceBucket: "target",
  ticket: "review",
  typedNames: "review",
};
const STEP_FIELDS = (id: StepId) => Object.keys(FIELD_STEP).filter((k) => FIELD_STEP[k] === id) as (keyof WizardValues)[];

/** The API's field paths, to the inputs they are about (longest prefix wins). */
const FIELD_PATHS: [string, keyof WizardValues | "archive"][] = [
  ["pointInTime", "pointInTime"],
  ["target.topicNaming", "prefix"],
  ["target.clusterRef", "target"],
  ["target.mode", "scratch"],
  ["sourceArchive.credentialRef", "archiveSecret"],
  ["sourceArchive", "archive"],
  ["topicMapping", "topics"],
  ["evidenceDestinationRef", "evidenceDestination"],
  ["ticket", "ticket"],
  ["originalNameConfirmation", "typedNames"],
];

export function fieldOfPath(path: string): string | undefined {
  const hit = FIELD_PATHS.filter(([p]) => path === p || path.startsWith(`${p}.`) || path.startsWith(`${p}[`)).sort((a, b) => b[0].length - a[0].length)[0];
  return hit?.[1];
}

const BLANK: WizardValues = {
  pointKey: "",
  topics: [],
  topicsText: "",
  timeMode: "end",
  pointInTime: "",
  timeBasis: false,
  coverage: "complete",
  completeMaxRecords: "",
  target: "",
  naming: "prefix",
  prefix: "",
  noOwner: false,
  scratch: false,
  cloneTtl: null,
  topicConfiguration: NO_TOPIC_CONFIGURATION,
  replication: "",
  evidenceDestination: "",
  archiveSecret: "",
  storeEndpoint: "",
  storeRegion: "",
  storePathStyle: false,
  storeAllowHttp: false,
  evidenceBucket: "",
  ticket: "",
  typedNames: "",
};

const FOLLOW_DEADLINE_MS = 12 * 60_000;

export function RestoreWizard() {
  const { ns } = useParams({ from: "/$ns/restores/new" });
  const search = useSearch({ from: "/$ns/restores/new" }) as RestoreSearch;
  const caps = useCapabilities(ns);
  const backups = useBackupList(ns);
  const connections = useConnectionList(ns);
  const policy = useApprovalPolicy(ns);

  const header = (
    <PageHeader
      title={search.retryOf ? "Retry a restore to a fresh target" : "Restore a recovery point"}
      description="Five steps. Nothing is created until the last one."
      breadcrumb={
        <Breadcrumb className="lw-breadcrumb">
          <BreadcrumbItem render={() => <Link to="/$ns/restores" params={{ ns }}>Restores</Link>} />
          <BreadcrumbItem isActive>New restore</BreadcrumbItem>
        </Breadcrumb>
      }
    />
  );
  const body = (content: React.ReactNode) => (
    <>
      {header}
      <PageSection isFilled>{content}</PageSection>
    </>
  );
  if (!planHashAvailable()) {
    return body(
      <Alert variant="danger" isInline title="This page cannot compute a plan hash here" id="insecure-context">
        {INSECURE_CONTEXT_SENTENCE}
      </Alert>,
    );
  }
  if (caps && caps.restoreCreate === false) {
    return body(
      <Alert variant="info" isInline title="Your role here may read restores and not create them">
        Ask an operator of {ns} to create the restore.
      </Alert>,
    );
  }
  if (backups.isPending || connections.isPending || policy.isPending) return body(<LoadingRows rows={6} label="Loading recovery points" />);
  const failed = [backups, connections].find((q) => q.isError);
  if (failed) return body(<ErrorState error={failed.error} onRetry={() => void failed.refetch()} />);
  return (
    <>
      {header}
      <PageSection isFilled>
        <WizardBody key={`${ns}?${new URLSearchParams(search as Record<string, string>).toString()}`} ns={ns} search={search} backups={backups.data ?? []} connections={connections.data ?? []} policy={policy.data ?? null} canCheck={caps?.preflight !== false} />
      </PageSection>
    </>
  );
}

function WizardBody({
  ns,
  search,
  backups,
  connections,
  policy,
  canCheck,
}: {
  ns: string;
  search: RestoreSearch;
  backups: import("../../api/restores").Backup[];
  connections: import("../../api/restores").Connection[];
  policy: import("../../api/restores").ApprovalPolicy | null;
  canCheck: boolean;
}) {
  const navigate = useNavigate();
  const create = useCreateRestore(ns);
  const points = useMemo(() => recoveryPoints(backups), [backups]);
  const targets = connections.filter((c) => c.role === "target");
  const retryOf = search.retryOf ?? "";

  // The route's identity: a Backup by uid or name, or a catalog point. A refusal names it.
  const routed: Resolution = useMemo(() => resolvePoint(backups, { backup: search.backup, uid: search.uid }), [backups, search.backup, search.uid]);
  // A retry names no point (a Restore carries a backup set id, not its Backup): the operator chooses.
  const initialKey = search.catalog
    ? `catalog/${search.catalog}/${search.point ?? ""}`
    : routed.state === "selected"
      ? pointKeyOf(routed.backup)
      : search.backup || search.uid || retryOf || !points[0]
        ? ""
        : pointKeyOf(points[0]);
  const routeRefusal = routed.state === "missing" || routed.state === "unusable" ? refusalSentence(routed) : "";

  const offers = useQuery({ queryKey: ["restore-offers", ns], queryFn: ({ signal }) => readCatalogOffers(ns, signal), staleTime: 60_000 });
  const destinations = useDestinationList(ns);

  // --------------------------------------------------------------- the form
  const ctxRef = useRef<{ ctx: WizardContext | null; problem: string | null }>({ ctx: null, problem: null });
  const schema = useMemo(
    () =>
      z.custom<WizardValues>().superRefine((v, issue) => {
        const { ctx, problem } = ctxRef.current;
        if (!v.pointKey) issue.addIssue({ code: "custom", path: ["pointKey"], message: "Choose a recovery point." });
        if (!ctx) return;
        const f = planFields(ctx, v);
        const problems = validate(ctx, v, f, problem);
        for (const [k, message] of Object.entries(problems)) issue.addIssue({ code: "custom", path: [k === "archive" ? "pointKey" : k], message: message! });
        if (f.sample.coverage === "complete" && v.completeMaxRecords.trim() && !/^[0-9]+$/.test(v.completeMaxRecords.trim())) {
          issue.addIssue({ code: "custom", path: ["completeMaxRecords"], message: "A record bound is a whole number of at least 1, or empty for none." });
        } else if (v.completeMaxRecords.trim() === "0") {
          issue.addIssue({ code: "custom", path: ["completeMaxRecords"], message: "A record bound is at least 1." });
        }
        if (policy?.ticketRequired && !v.ticket.trim()) issue.addIssue({ code: "custom", path: ["ticket"], message: "A namespace under a governed policy requires a change ticket; it is signed into the request the approver sees." });
        const typed = typedTopicsProblem(policy, v, f.topics);
        if (typed) issue.addIssue({ code: "custom", path: ["typedNames"], message: typed });
      }),
    [policy],
  );
  const form = useForm<WizardValues>({ resolver: zodResolver(schema), mode: "onChange", defaultValues: { ...BLANK, pointKey: initialKey, target: targets[0]?.name ?? "" } });
  const values = useWatch({ control: form.control }) as WizardValues;

  // --------------------------------------------------------- the chosen point
  const chosenBackup = backups.find((b) => pointKeyOf(b) === values.pointKey) ?? null;
  const chosenOffer = (offers.data?.offers ?? []).find((o) => offerKeyOf(o) === values.pointKey) ?? null;
  const catalogTarget = values.pointKey.startsWith("catalog/") ? values.pointKey.slice("catalog/".length) : "";
  const resolution = useQuery<Resolved>({
    queryKey: ["restore-point", ns, values.pointKey],
    enabled: values.pointKey !== "" && (values.pointKey.startsWith("catalog/") || !!chosenBackup),
    queryFn: ({ signal }) => {
      if (chosenBackup) return resolveBackup(ns, chosenBackup, signal);
      const slash = catalogTarget.indexOf("/");
      const catalog = chosenOffer?.catalog.name ?? catalogTarget.slice(0, slash);
      return resolveCatalogPoint(ns, catalog, chosenOffer?.entry.pointId ?? catalogTarget.slice(slash + 1), signal);
    },
    staleTime: 30_000,
  });
  const resolved = resolution.data?.state === "selected" ? resolution.data : null;
  const point = resolved?.point ?? null;

  // A new point starts the later steps over from its own defaults.
  const seeded = useRef("");
  useEffect(() => {
    if (!point || seeded.current === point.key) return;
    seeded.current = point.key;
    const listed = point.catalog?.entry.topics ?? [];
    const end = endInstant(point);
    form.reset({
      ...form.getValues(),
      pointKey: values.pointKey,
      topics: point.topics?.slice() ?? [],
      topicsText: point.topics === null && listed.length > 0 && !point.catalog?.entry.topicsOmitted ? listed.map((t) => t.name).join(", ") : "",
      timeMode: "end",
      pointInTime: end,
      prefix: defaultPrefixFor(end, retryOf),
      evidenceDestination: point.destinationName,
      archiveSecret: point.archiveSecretName,
      evidenceBucket: point.destinationName ? "" : bucketOfArchive(point.archiveUrl),
      typedNames: "",
    });
  }, [point, form, values.pointKey, retryOf]);

  const connection = connections.find((c) => c.name === values.target && c.role === "target") ?? null;
  const discovery = useQuery({
    queryKey: ["restore-target-discovery", ns, connection?.name ?? ""],
    enabled: !!connection,
    queryFn: async ({ signal }) => {
      try {
        return await readLatestDiscoveries(ns, connection!.name, signal);
      } catch (error) {
        if (error instanceof DOMException && error.name === "AbortError") throw error;
        return { unread: error instanceof Error ? error.message : String(error) };
      }
    },
  });
  const brokers = connection ? targetBrokerFact(discovery.data ?? null, connection) : BROKERS_NOT_READ;
  const evidenceName = point?.destinationName ? values.evidenceDestination || point.destinationName : "";
  const evidence = useQuery({
    queryKey: ["destinations", ns, "item", evidenceName],
    enabled: !!evidenceName && evidenceName !== point?.destinationName,
    queryFn: ({ signal }) => readDestination(ns, evidenceName, signal),
  });
  const evidenceDestination = !evidenceName ? null : evidenceName === point?.destinationName ? (resolved?.destination ?? null) : (evidence.data ?? null);

  const ctx: WizardContext | null = point ? { point, retryOf, destination: resolved?.destination ?? null, evidence: evidenceDestination, connection, brokers } : null;
  ctxRef.current = { ctx, problem: resolved?.destinationProblem ?? (resolution.data?.state === "refused" ? resolution.data.reason : null) };

  // --------------------------------------------------------------- the plan
  const fields = ctx ? planFields(ctx, values) : null;
  let bytes: string | null = null;
  let planProblem: string | null = null;
  if (fields) {
    try {
      bytes = renderPlanBytes(fields);
    } catch (error) {
      planProblem = error instanceof Error ? error.message : String(error);
    }
  }
  const [prepared, setPrepared] = useState<PreparedPlan | null>(null);
  useEffect(() => {
    let live = true;
    if (bytes === null || !fields) {
      setPrepared(null);
      return;
    }
    void preparePlanDocument(fields, { bytes }).then((p) => live && setPrepared(p));
    return () => {
      live = false;
    };
    // The bytes are the document: `fields` only produced them, so they are not a dependency.
  }, [bytes]);
  const current = prepared && prepared.bytes === bytes ? prepared : null;

  // ---------------------------------------------------------- readiness
  const [readiness, setReadiness] = useState<ReadinessState>({ result: null, boundHash: "", boundKey: "", error: null, starting: false, stoppedAt: null });
  const attempt = useRef({ nonce: crypto.randomUUID().replace(/-/g, ""), n: 0 });
  const boundKey = `${point?.key ?? ""}|${connection?.uid ?? ""}|${evidenceDestination?.uid ?? ""}|${values.archiveSecret.trim()}`;
  const following = useRef(0);
  const follow = (id: string, hash: string, started: number, run: number) => {
    let gap = 2000;
    const tick = async () => {
      if (following.current !== run) return;
      try {
        const result = await readPreflight(ns, id, hash);
        if (following.current !== run) return;
        setReadiness((r) => ({ ...r, result, error: null }));
        if (result.terminal) return;
      } catch (error) {
        if (following.current !== run) return;
        setReadiness((r) => ({ ...r, error }));
      }
      if (Date.now() - started > FOLLOW_DEADLINE_MS) {
        setReadiness((r) => ({ ...r, stoppedAt: Date.now() }));
        return;
      }
      gap = Math.min(gap * 1.5, 10_000);
      window.setTimeout(() => void tick(), gap);
    };
    window.setTimeout(() => void tick(), gap);
  };
  useEffect(() => () => void (following.current += 1), []);
  const startCheck = async () => {
    if (!ctx || !current) return;
    attempt.current.n += 1;
    const key = `logweir-console.restore-readiness.${ns}.${attempt.current.nonce}-${attempt.current.n}`;
    setReadiness((r) => ({ ...r, starting: true, error: null, stoppedAt: null }));
    try {
      const result = await startPreflight(ns, readinessRequest(ctx, values, current), key);
      const run = (following.current += 1);
      setReadiness({ result, boundHash: current.hash, boundKey, error: null, starting: false, stoppedAt: null });
      if (!result.terminal) follow(result.id, current.hash, Date.now(), run);
    } catch (error) {
      setReadiness((r) => ({ ...r, starting: false, error }));
    }
  };
  const cancelCheck = async () => {
    const id = readiness.result?.id;
    if (!id) return;
    try {
      await cancelPreflight(ns, id);
      setReadiness((r) => ({ ...r, result: r.result ? { ...r.result, state: "cancelled", terminal: true } : r.result }));
      following.current += 1;
    } catch (error) {
      setReadiness((r) => ({ ...r, error }));
    }
  };
  const hash = current?.hash ?? "";
  // A change of target, evidence destination or archive Secret changes no byte of the plan
  // and does make the verdict stale: it was about other objects.
  const movedUnder = readiness.result !== null && readiness.boundKey !== boundKey ? "The target, the evidence destination or the archive Secret changed since the check ran; it is not a verdict about this plan. Run it again." : null;
  const checkRefusal = canCheck ? (movedUnder ?? readinessRefusal(readiness.result, readiness.boundHash, hash)) : null;
  // Theme A: the check's per-topic configuration preview, read once it is done.
  const tcChosen = values.topicConfiguration?.transition != null;
  const tcDetails = usePreflightDetails(ns, tcChosen && readiness.result ? readiness.result : undefined, TOPIC_CONFIGURATION_CHECK);
  const ownersAsked = tcChosen && (topicPreviewLines(tcDetails.data ?? []).refusal ?? "").includes("TopicOwnerNotChecked");
  const topicPreview = tcChosen && tcDetails.data ? <TopicConfigurationPreview entries={tcDetails.data} /> : undefined;

  // ------------------------------------------------------------ the submit
  const [submitError, setSubmitError] = useState<{ error: unknown; unmatched: string[] } | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [goTo, setGoTo] = useState<StepId | null>(null);

  const submit = async () => {
    setSubmitError(null);
    if (!ctx || !current || !fields) return;
    const reviewed = current;
    if (!(await form.trigger())) {
      const first = Object.keys(form.formState.errors)[0];
      if (first && FIELD_STEP[first]) setGoTo(FIELD_STEP[first]);
      return;
    }
    const refusePolicy = policyRefusal(policy);
    if (refusePolicy) {
      setSubmitError({ error: new LocalRefusal(refusePolicy), unmatched: [] });
      return;
    }
    setSubmitting(true);
    try {
      // The verdict on screen is a memory: read it again against the reviewed hash.
      if (canCheck && readiness.result) {
        const fresh = await readPreflight(ns, readiness.result.id, reviewed.hash);
        setReadiness((r) => ({ ...r, result: fresh.stale || !r.result?.stale ? fresh : r.result }));
        const again = movedUnder ?? readinessRefusal(fresh, readiness.boundHash, reviewed.hash);
        if (again) throw new LocalRefusal(`Nothing was sent: ${again}`);
      } else if (canCheck) {
        throw new LocalRefusal("Nothing was sent: run the readiness check for this plan first.");
      }
      // The point's frozen destination, and the evidence destination, read again.
      if (ctx.point.destinationName) {
        const live = await readDestination(ns, ctx.point.destinationName);
        const moved = frozenDestinationProblem(ctx.point, live);
        if (moved) throw new LocalRefusal(`${moved}. Nothing was sent; the reviewed plan is still here.`);
        if (evidenceDestination && evidenceDestination.name !== ctx.point.destinationName) {
          const again = await readDestination(ns, evidenceDestination.name);
          if (!again || again.uid !== evidenceDestination.uid || again.locationDigest !== evidenceDestination.locationDigest) {
            throw new LocalRefusal(`Evidence destination ${evidenceDestination.name} was deleted, recreated or moved since it was read. Nothing was sent.`);
          }
        }
      }
      // The bytes a click sends are the bytes that were on screen when it was clicked.
      const now = await preparePlanDocument(planFields(ctx, form.getValues()));
      if (now !== reviewed) throw new LocalRefusal(`The plan changed after it was displayed (the page showed ${reviewed.hash}, the values now hash to ${now.hash}). Review the plan shown now and submit again.`);
      const request = restoreBody(ctx, form.getValues(), fields, reviewed, policy);
      const created = await create.mutateAsync({ body: request, key: restoreIdempotencyKey(ns, reviewed) });
      void navigate({ to: "/$ns/restores/$name", params: { ns, name: created.item.name } });
    } catch (error) {
      const unmatched: string[] = [];
      if (error instanceof ApiError) {
        for (const fe of error.fieldErrors) {
          const input = fieldOfPath(fe.field);
          if (input && input !== "archive") form.setError(input as keyof WizardValues, { type: "server", message: `${fe.message} (${fe.code})` });
          else unmatched.push(`${fe.field}: ${fe.message} (${fe.code})`);
        }
        const first = error.fieldErrors.map((fe) => fieldOfPath(fe.field)).find((f) => f && FIELD_STEP[f]);
        if (first) setGoTo(FIELD_STEP[first]!);
      }
      setSubmitError({ error, unmatched });
    } finally {
      setSubmitting(false);
    }
  };

  const others = connections.length - targets.length;
  const errors = form.formState.errors as Record<string, { message?: string }>;
  const replication = ctx ? replicationChoice(ctx, values) : null;

  return (
    <Card className="lw-wizard-card">
      <Wizard
        navAriaLabel="Restore steps"
        isVisitRequired
        onClose={() => void navigate({ to: "/$ns/restores", params: { ns } })}
        footer={
          <Footer
            validate={async (id) => (id === "point" ? !!point && !resolved?.destinationProblem && (await form.trigger(STEP_FIELDS(id))) : form.trigger(STEP_FIELDS(id)))}
            nextBlocked={(id) => (id === "point" ? !point || !!resolved?.destinationProblem : id === "check" ? canCheck && checkRefusal !== null : false)}
            onCreate={() => void submit()}
            creating={submitting}
            createBlocked={!current || policyRefusal(policy) !== null}
            onCancel={() => void navigate({ to: "/$ns/restores", params: { ns } })}
            goTo={goTo}
            onWent={() => setGoTo(null)}
          />
        }
      >
        <WizardStep name="Recovery point" id="point">
          <PointStep
            control={form.control}
            points={points.filter((b) => isRecoveryPoint(b))}
            offers={(offers.data?.offers ?? []).filter((o) => !points.some((b) => b.backupId === o.entry.backupId))}
            offerNotes={offers.data?.notes ?? []}
            offersPending={offers.isPending}
            retryOf={retryOf}
            refusal={routeRefusal}
            errors={errors}
            resolution={
              resolution.data?.state === "refused" ? (
                <Alert variant="danger" isInline title="This point cannot be restored" className="lw-gap-top" id="point-refused">
                  <Coded text={resolution.data.reason} />
                </Alert>
              ) : (
                <ResolutionView point={point} destinationProblem={resolved?.destinationProblem ?? null} unboundReason={resolved?.unboundReason ?? null} pending={resolution.isFetching && !point} error={resolution.error} />
              )
            }
          />
        </WizardStep>
        <WizardStep name="Topics and time" id="what" isDisabled={!point}>
          {point && <WhatStep control={form.control} point={point} errors={errors} />}
        </WizardStep>
        <WizardStep name="Target" id="target" isDisabled={!point}>
          {point && fields && replication && (
            <TargetStep
              control={form.control}
              point={point}
              targets={targets}
              others={others}
              connection={connection}
              fields={fields}
              replication={replication}
              brokers={brokers}
              destinations={evidenceOptions(point, resolved?.destination ?? null, destinations.data ?? [])}
              errors={errors}
              ns={ns}
              ownersAsked={ownersAsked}
            />
          )}
        </WizardStep>
        <WizardStep name="Readiness check" id="check" isDisabled={!point}>
          <CheckStep readiness={readiness} refusal={checkRefusal} canCheck={canCheck} hash={hash} planProblem={planProblem} connection={connection} onStart={() => void startCheck()} onCancel={() => void cancelCheck()} topicPreview={topicPreview} />
        </WizardStep>
        <WizardStep name="Review and create" id="review" isDisabled={!point}>
          {point && fields && replication && (
            <ReviewStep control={form.control} point={point} fields={fields} prepared={current} planProblem={planProblem} replication={replication} brokers={brokers} policy={policy} errors={errors} values={values} />
          )}
          {policyRefusal(policy) && (
            <Alert variant="danger" isInline title="Nothing can be sent here" className="lw-gap-top">
              {policyRefusal(policy)}
            </Alert>
          )}
          {submitError && <div className="lw-gap-top"><WriteProblem error={submitError.error} title="The restore was not created" unmatched={submitError.unmatched} /></div>}
        </WizardStep>
      </Wizard>
    </Card>
  );
}

/** The evidence destinations offered: the point's own first, then every other one, each once by uid. */
function evidenceOptions(point: import("./points").RecoveryPoint, own: { name: string; uid: string } | null, listed: { name: string; uid: string }[]) {
  const out: { name: string; uid: string }[] = [];
  const seen = new Set<string>();
  for (const d of [own ?? { name: point.destinationName, uid: point.destinationUid }, ...listed]) {
    if (!d.name || seen.has(d.uid || d.name)) continue;
    seen.add(d.uid || d.name);
    out.push({ name: d.name, uid: d.uid });
  }
  return out;
}

function bucketOfArchive(url: string): string {
  const rest = url.includes("://") ? url.slice(url.indexOf("://") + 3) : "";
  return rest.split("/")[0] ?? "";
}

function Footer({
  validate,
  nextBlocked,
  onCreate,
  creating,
  createBlocked,
  onCancel,
  goTo,
  onWent,
}: {
  validate: (id: StepId) => Promise<boolean>;
  nextBlocked: (id: StepId) => boolean;
  onCreate: () => void;
  creating: boolean;
  createBlocked: boolean;
  onCancel: () => void;
  goTo: StepId | null;
  onWent: () => void;
}) {
  const { activeStep, goToNextStep, goToPrevStep, goToStepById } = useWizardContext();
  const id = String(activeStep.id) as StepId;
  const last = id === "review";
  useEffect(() => {
    if (goTo && goTo !== id) goToStepById(goTo);
    if (goTo) onWent();
  }, [goTo, id, goToStepById, onWent]);
  return (
    <WizardFooterWrapper>
      <Button
        variant="primary"
        isDisabled={(last ? createBlocked : nextBlocked(id)) || creating}
        isLoading={creating}
        onClick={async () => {
          if (last) {
            onCreate();
            return;
          }
          if (await validate(id)) await goToNextStep();
        }}
      >
        {last ? "Create restore" : "Next"}
      </Button>
      <Button variant="secondary" onClick={() => void goToPrevStep()} isDisabled={id === "point"}>
        Back
      </Button>
      <Button variant="link" onClick={onCancel}>
        Cancel
      </Button>
    </WizardFooterWrapper>
  );
}

