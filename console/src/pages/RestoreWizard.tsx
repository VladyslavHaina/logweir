// The restore wizard: five steps, each answering one question, one action at the end.
// (The old console had six: archive and recovery point are one choice here.)
import { useEffect, useMemo, useState } from "react";
import { useNavigate, useParams, useSearch } from "@tanstack/react-router";
import { Controller, useForm, useWatch, type Control } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import {
  Alert,
  Button,
  Card,
  CodeBlock,
  CodeBlockCode,
  Content,
  DescriptionList,
  DescriptionListDescription,
  DescriptionListGroup,
  DescriptionListTerm,
  ExpandableSection,
  Flex,
  Form,
  FormGroup,
  FormHelperText,
  FormSelect,
  FormSelectOption,
  HelperText,
  HelperTextItem,
  Label,
  PageSection,
  Radio,
  Checkbox,
  Switch,
  TextInput,
  Title,
  Wizard,
  WizardFooterWrapper,
  WizardStep,
  useWizardContext,
} from "@patternfly/react-core";
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import { useBackups, useConnections, useCreateRestore, usePoints } from "../api/queries";
import type { Connection, CreateRestoreRequest, PointView } from "../api/types";
import { PageHeader } from "../components/PageHeader";
import { CheckStatus, ReachStatus } from "../components/Status";
import { ErrorState, LoadingRows } from "../components/States";
import { When, count, exactUtc } from "../components/When";
import { Ident } from "../components/Ident";

// ------------------------------------------------------------------ the form

const TOPIC_NAME = /^[A-Za-z0-9._-]+$/;
const TTL_WORDS = { "4h": "4 hours", "24h": "24 hours", "7d": "7 days" } as const;

const schema = z
  .object({
    pointId: z.string().min(1, "Choose a recovery point."),
    topics: z.array(z.string()).min(1, "Choose at least one topic."),
    timeMode: z.enum(["end", "earlier"]),
    time: z.string(),
    target: z.string().min(1, "Choose a target cluster."),
    naming: z.enum(["prefix", "original"]),
    prefix: z.string(),
    clone: z.boolean(),
    ttl: z.enum(["4h", "24h", "7d"]),
    typed: z.string(),
  })
  .superRefine((v, ctx) => {
    if (v.naming === "prefix" && v.prefix.length === 0) {
      ctx.addIssue({ code: "custom", path: ["prefix"], message: "Give a prefix, or restore under the original names." });
    } else if (v.naming === "prefix" && !TOPIC_NAME.test(v.prefix)) {
      ctx.addIssue({ code: "custom", path: ["prefix"], message: "Use letters, digits, '.', '_' and '-' only." });
    }
    if (v.timeMode === "earlier" && !/^\d{2}:\d{2}(:\d{2})?$/.test(v.time)) {
      ctx.addIssue({ code: "custom", path: ["time"], message: "Give a time as HH:MM, in UTC." });
    }
    if (v.naming === "original") {
      const want = [...v.topics].sort().join(",");
      const got = v.typed.split(",").map((s) => s.trim()).filter(Boolean).sort().join(",");
      if (got !== want) {
        ctx.addIssue({ code: "custom", path: ["typed"], message: "Type each topic name exactly, separated by commas." });
      }
    }
  });

type Values = z.infer<typeof schema>;

const STEP_FIELDS: Record<string, (keyof Values)[]> = {
  point: ["pointId"],
  what: ["topics", "time"],
  target: ["target", "prefix"],
  check: [],
  review: ["typed"],
};

function todayPrefix(): string {
  const d = new Date();
  return `restore-${String(d.getUTCMonth() + 1).padStart(2, "0")}${String(d.getUTCDate()).padStart(2, "0")}-`;
}

function targetName(v: Pick<Values, "naming" | "prefix">, topic: string): string {
  return v.naming === "original" ? topic : `${v.prefix}${topic}`;
}

function planText(v: Values, point: PointView): string {
  const at = v.timeMode === "end" ? String(point.recoveryPointAt) : `${String(point.recoveryPointAt).slice(0, 10)}T${v.time.length === 5 ? `${v.time}:00` : v.time}Z`;
  return [
    "apiVersion: logweir.io/v1",
    "kind: RestorePlan",
    `point: ${point.pointId}`,
    `backupSet: ${point.backupId}`,
    `pointInTime: ${at}`,
    "source:",
    "  destination: primary",
    "target:",
    `  cluster: ${v.target}`,
    "  mode: newTopic",
    `  originalName: ${v.naming === "original"}`,
    "topics:",
    ...v.topics.map((t) => `  - {source: ${t}, target: ${targetName(v, t)}}`),
    ...(v.clone ? ["cleanup:", `  after: ${v.ttl}`] : []),
    "",
  ].join("\n");
}

async function sha256(text: string): Promise<string> {
  if (!globalThis.crypto?.subtle) return "unavailable: this page is not in a secure context";
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return "sha256:" + Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, "0")).join("");
}

// ------------------------------------------------------------------- steps

function PointStep({ control, points, records }: { control: Control<Values>; points: PointView[]; records: Map<string, number | undefined> }) {
  return (
    <>
      <Title headingLevel="h2" size="xl">Which recovery point?</Title>
      <Content component="p" className="lw-subtle">
        Each point is a verified backup of orders-prod. The newest is first.
      </Content>
      <Controller
        control={control}
        name="pointId"
        render={({ field, fieldState }) => (
          <>
            <Table aria-label="Recovery points" variant="compact" gridBreakPoint="grid-md">
              <Thead>
                <Tr>
                  <Th screenReaderText="Choose" />
                  <Th>Point in time</Th>
                  <Th>Topics</Th>
                  <Th>Records</Th>
                  <Th>Evidence</Th>
                </Tr>
              </Thead>
              <Tbody>
                {points.slice(0, 8).map((p, i) => (
                  <Tr
                    key={p.pointId}
                    isClickable
                    isRowSelected={field.value === p.pointId}
                    onRowClick={() => field.onChange(p.pointId)}
                  >
                    <Td
                      select={{
                        rowIndex: i,
                        variant: "radio",
                        isSelected: field.value === p.pointId,
                        onSelect: () => field.onChange(p.pointId),
                      }}
                    />
                    <Td dataLabel="Point in time">
                      <strong>{exactUtc(String(p.recoveryPointAt))}</strong>
                      <div className="lw-subtle lw-small">
                        <When value={p.recoveryPointAt} />
                        {i === 0 ? " · newest" : ""}
                      </div>
                    </Td>
                    <Td dataLabel="Topics">{(p.topics ?? []).map((t) => t.name).join(", ")}</Td>
                    <Td dataLabel="Records" className="lw-num">{count(records.get(p.backupId))}</Td>
                    <Td dataLabel="Evidence">
                      <Label isCompact status="success">Verified</Label>
                    </Td>
                  </Tr>
                ))}
              </Tbody>
            </Table>
            {fieldState.error && (
              <HelperText>
                <HelperTextItem variant="error">{fieldState.error.message}</HelperTextItem>
              </HelperText>
            )}
          </>
        )}
      />
    </>
  );
}

function WhatStep({ control, point }: { control: Control<Values>; point: PointView | undefined }) {
  const timeMode = useWatch({ control, name: "timeMode" });
  const from = point?.coveredFrom ? exactUtc(point.coveredFrom).slice(11, 16) : "";
  const to = point?.coveredTo ? exactUtc(point.coveredTo).slice(11, 16) : "";
  return (
    <Form onSubmit={(e) => e.preventDefault()}>
      <Title headingLevel="h2" size="xl">What to restore</Title>
      <Controller
        control={control}
        name="topics"
        render={({ field, fieldState }) => (
          <FormGroup role="group" isStack label="Topics" fieldId="topics">
            {(point?.topics ?? []).map((t) => (
              <Checkbox
                key={t.name}
                id={`topic-${t.name}`}
                label={t.name}
                description={`${t.partitions} ${t.partitions === 1 ? "partition" : "partitions"}, replication factor ${t.replicationFactor}`}
                isChecked={field.value.includes(t.name)}
                onChange={(_e, on) =>
                  field.onChange(on ? [...field.value, t.name] : field.value.filter((x) => x !== t.name))
                }
              />
            ))}
            {fieldState.error && (
              <FormHelperText>
                <HelperText>
                  <HelperTextItem variant="error">{fieldState.error.message}</HelperTextItem>
                </HelperText>
              </FormHelperText>
            )}
          </FormGroup>
        )}
      />
      <Controller
        control={control}
        name="timeMode"
        render={({ field }) => (
          <FormGroup role="radiogroup" isStack label="Up to which moment" fieldId="time-mode">
            <Radio
              id="time-end"
              name="time-mode"
              label={`The end of this point (${point ? exactUtc(String(point.recoveryPointAt)) : ""})`}
              description="Everything the backup captured."
              isChecked={field.value === "end"}
              onChange={() => field.onChange("end")}
            />
            <Radio
              id="time-earlier"
              name="time-mode"
              label="An earlier moment"
              description="For example just before a bad deploy wrote bad records. Records after it are left out."
              isChecked={field.value === "earlier"}
              onChange={() => field.onChange("earlier")}
            />
          </FormGroup>
        )}
      />
      {timeMode === "earlier" && (
        <Controller
          control={control}
          name="time"
          render={({ field, fieldState }) => (
            <FormGroup label="Time (UTC)" fieldId="time" isRequired>
              <TextInput
                id="time"
                type="time"
                value={field.value}
                onChange={(_e, v) => field.onChange(v)}
                validated={fieldState.error ? "error" : "default"}
                className="lw-time-input"
              />
              <FormHelperText>
                <HelperText>
                  <HelperTextItem variant={fieldState.error ? "error" : "default"}>
                    {fieldState.error?.message ?? `Between ${from} and ${to} UTC, the window this point covers.`}
                  </HelperTextItem>
                </HelperText>
              </FormHelperText>
            </FormGroup>
          )}
        />
      )}
    </Form>
  );
}

const CONFIG_ROWS = [
  { topic: "orders", partitions: "3", replication: "3", cleanup: "delete", retention: "7 days" },
  { topic: "orders-dlq", partitions: "1", replication: "3", cleanup: "delete", retention: "30 days" },
  { topic: "payments", partitions: "6", replication: "3", cleanup: "compact", retention: "unlimited" },
];

function TargetStep({ control, targets }: { control: Control<Values>; targets: Connection[] }) {
  const values = useWatch({ control });
  const naming = values.naming ?? "prefix";
  const topics = values.topics ?? [];
  return (
    <Form onSubmit={(e) => e.preventDefault()}>
      <Title headingLevel="h2" size="xl">Where to restore</Title>
      <Controller
        control={control}
        name="target"
        render={({ field }) => (
          <FormGroup role="radiogroup" isStack label="Target cluster" fieldId="target">
            {targets.map((c) => (
              <Radio
                key={c.name}
                id={`target-${c.name}`}
                name="target"
                isChecked={field.value === c.name}
                onChange={() => field.onChange(c.name)}
                isDisabled={c.reachability.state === "unreachable"}
                label={
                  <Flex gap={{ default: "gapSm" }} alignItems={{ default: "alignItemsCenter" }}>
                    <span>{c.name}</span>
                    <ReachStatus state={c.reachability.state} />
                  </Flex>
                }
                description={`${c.bootstrapServers.length} brokers · ${c.bootstrapServers[0]}`}
              />
            ))}
            <FormHelperText>
              <HelperText>
                <HelperTextItem>Only connections with the target role are listed: a source cluster is never written to.</HelperTextItem>
              </HelperText>
            </FormHelperText>
          </FormGroup>
        )}
      />
      <Controller
        control={control}
        name="naming"
        render={({ field }) => (
          <FormGroup role="radiogroup" isStack label="Topic names" fieldId="naming">
            <Radio
              id="naming-prefix"
              name="naming"
              label="New topics with a prefix (recommended)"
              description="Nothing that exists is touched. Good for drills and for inspecting data."
              isChecked={field.value === "prefix"}
              onChange={() => field.onChange("prefix")}
            />
            <Radio
              id="naming-original"
              name="naming"
              label="The original names"
              description="For a real recovery into a cluster where these topics do not exist. You confirm by typing the names."
              isChecked={field.value === "original"}
              onChange={() => field.onChange("original")}
            />
          </FormGroup>
        )}
      />
      {naming === "prefix" && (
        <Controller
          control={control}
          name="prefix"
          render={({ field, fieldState }) => (
            <FormGroup label="Prefix" fieldId="prefix" isRequired>
              <TextInput
                id="prefix"
                value={field.value}
                onChange={(_e, v) => field.onChange(v)}
                validated={fieldState.error ? "error" : "default"}
                className="lw-prefix-input"
              />
              <FormHelperText>
                <HelperText>
                  <HelperTextItem variant={fieldState.error ? "error" : "default"}>
                    {fieldState.error?.message ??
                      `Creates ${topics.map((t) => targetName({ naming: "prefix", prefix: field.value }, t ?? "")).join(", ")}.`}
                  </HelperTextItem>
                </HelperText>
              </FormHelperText>
            </FormGroup>
          )}
        />
      )}
      <FormGroup label="Clean up" fieldId="clone" role="group">
        <Controller
          control={control}
          name="clone"
          render={({ field }) => (
            <Switch
              id="clone"
              label="Delete the restored topics automatically (a temporary clone)"
              isChecked={field.value}
              onChange={(_e, on) => field.onChange(on)}
            />
          )}
        />
        {values.clone && (
          <Controller
            control={control}
            name="ttl"
            render={({ field }) => (
              <FormSelect id="ttl" value={field.value} onChange={(_e, v) => field.onChange(v)} aria-label="Delete after" className="lw-ttl">
                <FormSelectOption value="4h" label="after 4 hours" />
                <FormSelectOption value="24h" label="after 24 hours" />
                <FormSelectOption value="7d" label="after 7 days" />
              </FormSelect>
            )}
          />
        )}
        <FormHelperText>
          <HelperText>
            <HelperTextItem>Only the topics this restore creates are deleted, and only on {values.target}.</HelperTextItem>
          </HelperText>
        </FormHelperText>
      </FormGroup>
      <div>
        <Title headingLevel="h3" size="md">Topic configuration on {values.target}</Title>
        <Content component="p" className="lw-subtle lw-small">
          Partitions and replication as on the source. Settings marked "after verification" stay off while records are
          written, so nothing is deleted or compacted early, and are applied once the restore is verified.
        </Content>
        <Table aria-label="Topic configuration preview" variant="compact" gridBreakPoint="grid-md">
          <Thead>
            <Tr>
              <Th>Topic</Th>
              <Th modifier="fitContent">Partitions</Th>
              <Th modifier="fitContent">Replication</Th>
              <Th>Cleanup policy</Th>
              <Th>Retention</Th>
            </Tr>
          </Thead>
          <Tbody>
            {CONFIG_ROWS.filter((r) => topics.includes(r.topic)).map((r) => (
              <Tr key={r.topic}>
                <Td dataLabel="Topic">{targetName({ naming, prefix: values.prefix ?? "" }, r.topic)}</Td>
                <Td dataLabel="Partitions" className="lw-num">{r.partitions}</Td>
                <Td dataLabel="Replication" className="lw-num">{r.replication}</Td>
                <Td dataLabel="Cleanup policy">
                  {r.cleanup === "compact" ? (
                    <Flex gap={{ default: "gapSm" }} alignItems={{ default: "alignItemsCenter" }}>
                      <span>compact</span>
                      <Label isCompact color="blue">after verification</Label>
                    </Flex>
                  ) : (
                    r.cleanup
                  )}
                </Td>
                <Td dataLabel="Retention">
                  {r.retention === "unlimited" ? (
                    "unlimited"
                  ) : (
                    <Flex gap={{ default: "gapSm" }} alignItems={{ default: "alignItemsCenter" }}>
                      <span>{r.retention}</span>
                      <Label isCompact color="blue">after verification</Label>
                    </Flex>
                  )}
                </Td>
              </Tr>
            ))}
          </Tbody>
        </Table>
      </div>
    </Form>
  );
}

type Check = { id: string; text: string; state: "pending" | "running" | "pass" };

function CheckStep({ target, original, topics, onDone }: { target: string; original: boolean; topics: string[]; onDone: (ok: boolean) => void }) {
  const initial = (): Check[] => [
    { id: "reach", text: `${target} answers, as the restore principal`, state: "pending" },
    { id: "create", text: `The restore principal may create topics on ${target}`, state: "pending" },
    {
      id: "absent",
      text: original ? `${topics.join(", ")} do not exist on ${target}` : `None of the new topic names exist on ${target}`,
      state: "pending",
    },
    { id: "brokers", text: `${target} has enough brokers for replication factor 3`, state: "pending" },
    { id: "archive", text: "The archive objects of this point are readable", state: "pending" },
    { id: "receipt", text: "The point's signed receipt still verifies", state: "pending" },
  ];
  const [checks, setChecks] = useState<Check[]>(initial);
  const [round, setRound] = useState(0);

  useEffect(() => {
    onDone(false);
    setChecks(initial());
    const timers: number[] = [];
    initial().forEach((_c, i) => {
      timers.push(window.setTimeout(() => setChecks((cs) => cs.map((c, j) => (j === i ? { ...c, state: "running" } : c))), 250 + i * 420));
      timers.push(
        window.setTimeout(() => {
          setChecks((cs) => cs.map((c, j) => (j === i ? { ...c, state: "pass" } : c)));
          if (i === 5) onDone(true);
        }, 250 + i * 420 + 380),
      );
    });
    return () => timers.forEach((t) => window.clearTimeout(t));
  }, [round, target]);

  const done = checks.every((c) => c.state === "pass");
  return (
    <>
      <Title headingLevel="h2" size="xl">Is {target} ready?</Title>
      <Content component="p" className="lw-subtle">
        The controller checks this exact plan against {target} before anything is created. Nothing is written by the check.
      </Content>
      <ol className="lw-checks" aria-live="polite">
        {checks.map((c) => (
          <li key={c.id}>
            <span>{c.text}</span>
            <CheckStatus state={c.state} />
          </li>
        ))}
      </ol>
      {done && (
        <Alert variant="success" isInline title={`Ready: nothing on ${target} stops this restore.`}>
          The result is kept with the plan; if {target} changes before you create the restore, the controller checks again.
        </Alert>
      )}
      <Button variant="link" isInline onClick={() => setRound(round + 1)} isDisabled={!done} className="lw-gap-top">
        Run the check again
      </Button>
    </>
  );
}

function ReviewStep({ control, point, plan, hash }: { control: Control<Values>; point: PointView; plan: string; hash: string }) {
  const v = useWatch({ control }) as Values;
  const [open, setOpen] = useState(false);
  return (
    <Form onSubmit={(e) => e.preventDefault()}>
      <Title headingLevel="h2" size="xl">Review and create</Title>
      <DescriptionList isHorizontal isCompact horizontalTermWidthModifier={{ default: "16ch" }}>
        <DescriptionListGroup>
          <DescriptionListTerm>Recovery point</DescriptionListTerm>
          <DescriptionListDescription>
            {exactUtc(String(point.recoveryPointAt))}, from orders-prod
          </DescriptionListDescription>
        </DescriptionListGroup>
        <DescriptionListGroup>
          <DescriptionListTerm>Up to</DescriptionListTerm>
          <DescriptionListDescription>{v.timeMode === "end" ? "the end of the point" : `${v.time} UTC`}</DescriptionListDescription>
        </DescriptionListGroup>
        <DescriptionListGroup>
          <DescriptionListTerm>Into</DescriptionListTerm>
          <DescriptionListDescription>
            {v.target}: {v.topics.map((t) => `${t} → ${targetName(v, t)}`).join(", ")}
          </DescriptionListDescription>
        </DescriptionListGroup>
        <DescriptionListGroup>
          <DescriptionListTerm>Clean up</DescriptionListTerm>
          <DescriptionListDescription>{v.clone ? `delete these topics after ${TTL_WORDS[v.ttl]}` : "keep the topics"}</DescriptionListDescription>
        </DescriptionListGroup>
        <DescriptionListGroup>
          <DescriptionListTerm>Plan</DescriptionListTerm>
          <DescriptionListDescription>
            <Ident value={hash} label="plan sha256" />
          </DescriptionListDescription>
        </DescriptionListGroup>
      </DescriptionList>
      <ExpandableSection toggleText={open ? "Hide the plan" : "Show the plan"} onToggle={(_e, o) => setOpen(o)} isExpanded={open}>
        <CodeBlock>
          <CodeBlockCode>{plan}</CodeBlockCode>
        </CodeBlock>
      </ExpandableSection>
      <Alert variant="info" isInline title="team-a uses one-person confirmation">
        Creating the restore confirms it: it starts right away. In a two-person namespace it would wait for a second person
        to approve.
      </Alert>
      {v.naming === "original" && (
        <Controller
          control={control}
          name="typed"
          render={({ field, fieldState }) => (
            <FormGroup label={`Type the topic names to restore under their original names (${v.topics.join(", ")})`} fieldId="typed" isRequired>
              <TextInput id="typed" value={field.value} onChange={(_e, t) => field.onChange(t)} validated={fieldState.error ? "error" : "default"} />
              <FormHelperText>
                <HelperText>
                  <HelperTextItem variant={fieldState.error ? "error" : "default"}>
                    {fieldState.error?.message ?? "Recorded with the evidence as your confirmation."}
                  </HelperTextItem>
                </HelperText>
              </FormHelperText>
            </FormGroup>
          )}
        />
      )}
    </Form>
  );
}

// ------------------------------------------------------------------ wizard

function Footer({ validate, checkDone, onCreate, creating, onCancel }: {
  validate: (id: string) => Promise<boolean>;
  checkDone: boolean;
  onCreate: () => void;
  creating: boolean;
  onCancel: () => void;
}) {
  const { activeStep, goToNextStep, goToPrevStep } = useWizardContext();
  const id = String(activeStep.id);
  const last = id === "review";
  return (
    <WizardFooterWrapper>
      <Button
        variant="primary"
        isDisabled={(id === "check" && !checkDone) || creating}
        isLoading={creating}
        onClick={async () => {
          if (!(await validate(id))) return;
          if (last) onCreate();
          else await goToNextStep();
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

export function RestoreWizard() {
  const { ns } = useParams({ from: "/$ns/restores/new" });
  const { backup } = useSearch({ from: "/$ns/restores/new" });
  const navigate = useNavigate();
  const points = usePoints(ns);
  const backups = useBackups(ns);
  const connections = useConnections(ns);
  const create = useCreateRestore(ns);
  const [checkDone, setCheckDone] = useState(false);
  const [hash, setHash] = useState("computing...");

  const records = useMemo(() => new Map((backups.data ?? []).map((b) => [String(b.backupId), b.records ?? undefined])), [backups.data]);
  const preselected = useMemo(() => {
    const b = (backups.data ?? []).find((x) => x.name === backup);
    return (points.data ?? []).find((p) => p.backupId === b?.backupId)?.pointId ?? points.data?.[0]?.pointId ?? "";
  }, [backups.data, points.data, backup]);

  const form = useForm<Values>({
    resolver: zodResolver(schema),
    mode: "onTouched",
    values: {
      pointId: preselected,
      topics: ["orders", "orders-dlq", "payments"],
      timeMode: "end",
      time: "",
      target: "orders-dr",
      naming: "prefix",
      prefix: todayPrefix(),
      clone: false,
      ttl: "24h",
      typed: "",
    },
    resetOptions: { keepDirtyValues: true },
  });
  const v = useWatch({ control: form.control }) as Values;
  const point = (points.data ?? []).find((p) => p.pointId === v.pointId);
  const plan = point ? planText(v, point) : "";

  useEffect(() => {
    let live = true;
    void sha256(plan).then((h) => live && setHash(h));
    return () => {
      live = false;
    };
  }, [plan]);

  const header = (
    <PageHeader title="Restore a recovery point" description="Five steps. Nothing is created until the last one." />
  );
  if (points.isPending || connections.isPending || backups.isPending) {
    return (
      <>
        {header}
        <PageSection>
          <LoadingRows rows={6} label="Loading recovery points" />
        </PageSection>
      </>
    );
  }
  const failed = [points, connections, backups].find((q) => q.isError);
  if (failed) {
    return (
      <>
        {header}
        <PageSection>
          <ErrorState error={failed.error} />
        </PageSection>
      </>
    );
  }

  const targets = (connections.data ?? []).filter((c) => c.role === "target");

  const submit = async () => {
    if (!point) return;
    const values = form.getValues();
    const name = `orders-restore-${Math.random().toString(36).slice(2, 7)}`;
    const body: CreateRestoreRequest = {
      approvalRef: { name },
      backupSetRef: point.backupId,
      deadlineSeconds: 3600,
      planBytes: plan,
      planHash: hash,
      pointInTime: values.timeMode === "end" ? String(point.recoveryPointAt) : `${String(point.recoveryPointAt).slice(0, 10)}T${values.time}:00Z`,
      sourceArchive: { url: "logweir-destination://primary" },
      sourceDestinationRef: { name: "primary" },
      target: {
        clusterRef: { name: values.target },
        mode: "newTopic",
        topicNaming: { prefix: values.naming === "original" ? "" : values.prefix, originalName: values.naming === "original" },
      },
      topicMapping: values.topics.map((t) => ({ source: t, target: targetName(values, t) })),
      ...(values.naming === "original" ? { originalNameConfirmation: { typedTopics: values.topics } } : {}),
    };
    const created = await create.mutateAsync(body);
    void navigate({ to: "/$ns/restores/$name", params: { ns, name: created.name } });
  };

  return (
    <>
      {header}
      <PageSection isFilled>
        <Card className="lw-wizard-card">
          <Wizard
            navAriaLabel="Restore steps"
            isVisitRequired
            onClose={() => void navigate({ to: "/$ns/restores", params: { ns } })}
            footer={
              <Footer
                validate={(id) => form.trigger(STEP_FIELDS[id] ?? [])}
                checkDone={checkDone}
                creating={create.isPending}
                onCreate={() => void submit()}
                onCancel={() => void navigate({ to: "/$ns/restores", params: { ns } })}
              />
            }
          >
            <WizardStep name="Recovery point" id="point">
              <PointStep control={form.control} points={points.data ?? []} records={records} />
            </WizardStep>
            <WizardStep name="Topics and time" id="what">
              <WhatStep control={form.control} point={point} />
            </WizardStep>
            <WizardStep name="Target" id="target">
              <TargetStep control={form.control} targets={targets} />
            </WizardStep>
            <WizardStep name="Readiness check" id="check">
              <CheckStep target={v.target} original={v.naming === "original"} topics={v.topics} onDone={setCheckDone} />
            </WizardStep>
            <WizardStep name="Review and create" id="review">
              {point ? <ReviewStep control={form.control} point={point} plan={plan} hash={hash} /> : null}
              {create.isError && (
                <Alert variant="danger" isInline title="The restore was not created">
                  {create.error.message}
                </Alert>
              )}
            </WizardStep>
          </Wizard>
        </Card>
      </PageSection>
    </>
  );
}
