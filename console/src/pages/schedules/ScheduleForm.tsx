// The one schedule form: guided create and the future-policy edit.
//
// The standard path is short: what to back up, when, and where. A preset's
// cron line comes from the API's preview (the console compiles nothing), and a
// preset cannot be saved until the preview of exactly these values is back.
// The archive URL and the run policy (deadlines, catch-up, retries,
// concurrency) sit behind explicit choices.
import { useEffect, useMemo, useState, type ReactNode } from "react";
import { Controller, useForm, useWatch, type Control, type FieldPath } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import {
  ActionGroup,
  Alert,
  Button,
  Checkbox,
  Content,
  ExpandableSection,
  Flex,
  FlexItem,
  Form,
  FormGroup,
  FormHelperText,
  FormSection,
  FormSelect,
  FormSelectOption,
  HelperText,
  HelperTextItem,
  Label,
  LabelGroup,
  Radio,
  Skeleton,
  Switch,
  TextArea,
  TextInput,
  Title,
} from "@patternfly/react-core";
import CheckIcon from "@patternfly/react-icons/dist/esm/icons/check-icon";
import PlusIcon from "@patternfly/react-icons/dist/esm/icons/plus-icon";
import { useConnections, useDestinations } from "../../api/queries";
import { probeShortWords } from "../../api/connections";
import {
  useCadencePreview,
  useDiscoveredTopics,
  type DestinationSummary,
  type Schedule,
} from "../../api/schedules";
import { When } from "../../components/When";
import {
  BOUNDS,
  DAYS,
  PRESETS,
  groupList,
  list,
  movesLocation,
  placeFieldErrors,
  policySchema,
  previewQuery,
  type PolicyValues,
} from "./model";
import { NextRuns } from "./parts";
import { Readiness } from "./Readiness";
import "./schedules.css";

const ARCHIVE = "__archive__";
const ADVANCED: FieldPath<PolicyValues>[] = [
  "suspended",
  "concurrencyPolicy",
  "startingDeadlineSeconds",
  "catchUpPolicy",
  "maxRetries",
  "retryDelaySeconds",
  "activeDeadlineSeconds",
];

/** A form field: its label, its input, and one line under it that is the error when there is one. */
function Field({ id, label, help, error, children, required }: { id: string; label: string; help?: ReactNode; error?: string; children: ReactNode; required?: boolean }) {
  return (
    <FormGroup label={label} fieldId={id} isRequired={required}>
      {children}
      {(error || help) && (
        <FormHelperText>
          <HelperText>
            <HelperTextItem variant={error ? "error" : "default"} id={`${id}-helper`}>
              {error ?? help}
            </HelperTextItem>
          </HelperText>
        </FormHelperText>
      )}
    </FormGroup>
  );
}

function TextField({ control, name, label, help, placeholder, type = "text", list: datalist, required }: {
  control: Control<PolicyValues>;
  name: FieldPath<PolicyValues>;
  label: string;
  help?: ReactNode;
  placeholder?: string;
  type?: "text" | "number";
  list?: string;
  required?: boolean;
}) {
  return (
    <Controller
      control={control}
      name={name}
      render={({ field, fieldState }) => (
        <Field id={`sf-${name}`} label={label} help={help} error={fieldState.error?.message} required={required}>
          <TextInput
            id={`sf-${name}`}
            type={type}
            value={String(field.value ?? "")}
            onChange={(_e, v) => field.onChange(v)}
            onBlur={field.onBlur}
            placeholder={placeholder}
            validated={fieldState.error ? "error" : "default"}
            aria-describedby={`sf-${name}-helper`}
            list={datalist}
          />
        </Field>
      )}
    />
  );
}

function SelectField({ control, name, label, help, options }: {
  control: Control<PolicyValues>;
  name: FieldPath<PolicyValues>;
  label: string;
  help?: ReactNode;
  options: [string, string][];
}) {
  return (
    <Controller
      control={control}
      name={name}
      render={({ field, fieldState }) => (
        <Field id={`sf-${name}`} label={label} help={help} error={fieldState.error?.message}>
          <FormSelect
            id={`sf-${name}`}
            value={String(field.value ?? "")}
            onChange={(_e, v) => field.onChange(v)}
            validated={fieldState.error ? "error" : "default"}
            aria-describedby={`sf-${name}-helper`}
          >
            {options.map(([value, text]) => (
              <FormSelectOption key={value} value={value} label={text} />
            ))}
          </FormSelect>
        </Field>
      )}
    />
  );
}

const PARAM_LABEL = { minute: "Minute", hour: "Hour", dayOfWeek: "Day of the week", dayOfMonth: "Day of the month", n: "Every" } as const;

function CadenceFields({ control }: { control: Control<PolicyValues> }) {
  const mode = useWatch({ control, name: "mode" });
  const preset = PRESETS.find((p) => p.kind === mode);
  return (
    <>
      <SelectField
        control={control}
        name="mode"
        label="How often"
        options={[...PRESETS.map((p): [string, string] => [p.kind, p.label]), ["custom", "Custom cron expression"]]}
      />
      {preset ? (
        <Flex gap={{ default: "gapMd" }} flexWrap={{ default: "wrap" }}>
          {preset.params.map((p) => (
            <FlexItem key={p} className="lw-param">
              {p === "dayOfWeek" ? (
                <SelectField control={control} name={p} label={PARAM_LABEL[p]} options={DAYS.map((d, i): [string, string] => [String(i), d])} />
              ) : p === "n" ? (
                <SelectField
                  control={control}
                  name={p}
                  label={PARAM_LABEL[p]}
                  options={BOUNDS.n.values!.map((v): [string, string] => [String(v), `${v} hours`])}
                />
              ) : (
                <TextField control={control} name={p} label={PARAM_LABEL[p]} type="number" help={`${BOUNDS[p].min} to ${BOUNDS[p].max}`} />
              )}
            </FlexItem>
          ))}
        </Flex>
      ) : (
        <TextField
          control={control}
          name="cron"
          label="Cron expression"
          placeholder="30 2 * * 1-5"
          help="Five fields: minute, hour, day of month, month, day of week; read in the time zone below."
        />
      )}
      {mode === "everyNHours" && (
        <Content component="p" className="lw-subtle lw-small">
          Every N hours means the hours of the day divisible by N (0, 6, 12 and 18 for 6), not N hours after the
          schedule was created.
        </Content>
      )}
    </>
  );
}

const ZONES: string[] = (() => {
  try {
    return (Intl as unknown as { supportedValuesOf?: (k: string) => string[] }).supportedValuesOf?.("timeZone") ?? [];
  } catch {
    return [];
  }
})();

/** The draft's next runs, from the API, and the line a preset is saved as. */
function Preview({ control, onReady }: { control: Control<PolicyValues>; onReady: (canonical: string | null, errors: Partial<Record<keyof PolicyValues, string>>) => void }) {
  const values = useWatch({ control }) as PolicyValues;
  const query = useMemo(
    () => previewQuery(values),
    [values.mode, values.cron, values.minute, values.hour, values.dayOfWeek, values.dayOfMonth, values.n, values.timeZone],
  );
  // Ask once the typing stops, not on every key.
  const [asked, setAsked] = useState(query);
  useEffect(() => {
    const t = window.setTimeout(() => setAsked(query), 350);
    return () => window.clearTimeout(t);
  }, [JSON.stringify(query)]);
  const settled = JSON.stringify(asked) === JSON.stringify(query);
  const preview = useCadencePreview(asked);
  const placed = preview.isError ? placeFieldErrors(preview.error) : { fields: {}, unmatched: [] };
  useEffect(() => {
    onReady(settled && preview.data ? preview.data.schedule : null, placed.fields);
  }, [settled, preview.data, preview.error]);

  if (query === null) {
    return <Content component="p" className="lw-subtle">Complete the cadence to see when it runs.</Content>;
  }
  if (!settled || preview.isPending) {
    return (
      <div aria-busy="true">
        <Skeleton height="1.5rem" screenreaderText="Working out the next runs" />
      </div>
    );
  }
  if (preview.isError) {
    const message = Object.values(placed.fields)[0] ?? placed.unmatched[0] ?? (preview.error instanceof Error ? preview.error.message : "");
    return (
      <Alert isInline isPlain variant="danger" title="The API refused this cadence">
        {message}
      </Alert>
    );
  }
  const data = preview.data;
  return (
    <>
      <Content component="p" className="lw-subtle">
        Saved as <code className="lw-mono lw-nowrap">{data.schedule}</code>, read in {data.timeZone}. The next {data.runs.length} runs:
      </Content>
      <NextRuns runs={data.runs} zone={data.timeZone} label="The next runs of this draft" />
    </>
  );
}

function TopicPicker({ ns, control, source, onAdd }: { ns: string; control: Control<PolicyValues>; source: string; onAdd: (topic: string) => void }) {
  const found = useDiscoveredTopics(ns, source);
  const typed = list(useWatch({ control, name: "topics" }));
  if (!source || found.isPending) return null;
  if (found.isError || !found.data?.discovery) {
    return (
      <Content component="p" className="lw-subtle lw-small">
        {found.data?.failed ? "The newest topic discovery of this connection failed, so no names are offered. " : "No topic discovery has run for this connection, so no names are offered. "}
        Type them, or discover topics on the <a href={`#/${ns}/connections`}>Connections</a> page.
      </Content>
    );
  }
  const d = found.data.discovery;
  return (
    <div className="lw-gap-top">
      <Content component="p" className="lw-subtle lw-small">
        Seen by the latest discovery of {source}, <When value={d.observedAt} />
        {d.visibility?.state === "limited" ? ". Visibility was limited, so this is a subset: type any topic it does not offer." : "."}
        {d.stale ? " This inventory is out of date." : ""}
        {found.data.failed ? " A newer discovery failed." : ""}
        {found.data.more ? " These are the first 200 of the topics it saw: type any other." : ""}
      </Content>
      <LabelGroup numLabels={12} categoryName="Add a topic" isCompact className="lw-topic-offer">
        {found.data.topics.map((t) => {
          const added = typed.includes(t);
          return (
            <Label
              key={t}
              isCompact
              variant="outline"
              icon={added ? <CheckIcon /> : <PlusIcon />}
              onClick={added ? undefined : () => onAdd(t)}
            >
              {t}
            </Label>
          );
        })}
      </LabelGroup>
    </div>
  );
}

function destinationText(d: DestinationSummary): string {
  return `${d.name}${d.default ? " (namespace default)" : ""} · ${d.canonicalUrl}`;
}

type Pin = { name: string; uid: string };

/** The refusal for a chosen object that is gone, or was created again under its name. */
function identityChanged(field: keyof PolicyValues, kind: string, pin: Pin | undefined, fresh: { name: string; uid: string }[] | undefined): [keyof PolicyValues, string] | null {
  if (!pin || !fresh) return null;
  const now = fresh.find((x) => x.name === pin.name);
  if (!now) return [field, `The ${kind} ${pin.name} is not in this namespace any more. Nothing was sent; choose another.`];
  if (now.uid !== pin.uid) {
    return [field, `The ${kind} ${pin.name} was deleted and created again: a different object answers to the name now. Nothing was sent; check it, then submit again to use it.`];
  }
  return null;
}

export interface ScheduleFormProps {
  ns: string;
  initial: PolicyValues;
  /** The saved schedule, when this form edits one. */
  saved?: Schedule;
  submitText: string;
  pending: boolean;
  /** The refusal of the last submit; its field errors are placed beside their inputs. */
  error: unknown;
  status?: ReactNode;
  onSubmit: (values: PolicyValues, schedule: string) => void;
  onCancel: () => void;
}

export function ScheduleForm({ ns, initial, saved, submitText, pending, error, status, onSubmit, onCancel }: ScheduleFormProps) {
  const editing = saved !== undefined;
  const { control, handleSubmit, setValue, setError, getValues } = useForm<PolicyValues>({
    resolver: zodResolver(policySchema),
    defaultValues: initial,
    mode: "onTouched",
  });
  const connections = useConnections(ns);
  const destinations = useDestinations(ns);
  const values = useWatch({ control }) as PolicyValues;
  const [canonical, setCanonical] = useState<string | null>(null);
  const [previewErrors, setPreviewErrors] = useState<Partial<Record<keyof PolicyValues, string>>>({});
  const [more, setMore] = useState(false);
  // The object each name meant when it was chosen. A connection or destination
  // deleted and created again under the same name is a different cluster or
  // store, so a submit re-reads both and refuses to follow the name.
  const [pins, setPins] = useState<{ source?: Pin; destination?: Pin }>({});
  useEffect(() => {
    const c = connections.data?.find((x) => x.name === values.source);
    if (c && pins.source?.name !== c.name) setPins((p) => ({ ...p, source: { name: c.name, uid: c.uid } }));
  }, [values.source, connections.data]);
  useEffect(() => {
    const d = values.where === "destination" ? destinations.data?.find((x) => x.name === values.destination) : undefined;
    if (d && pins.destination?.name !== d.name) setPins((p) => ({ ...p, destination: { name: d.name, uid: d.uid } }));
  }, [values.destination, values.where, destinations.data]);

  // A refusal's field errors go next to their fields; one under "More options" opens it.
  useEffect(() => {
    const { fields } = placeFieldErrors(error);
    for (const [name, message] of Object.entries(fields)) setError(name as keyof PolicyValues, { type: "server", message });
    if (Object.keys(fields).some((f) => ADVANCED.includes(f as keyof PolicyValues))) setMore(true);
  }, [error]);
  useEffect(() => {
    for (const [name, message] of Object.entries(previewErrors)) setError(name as keyof PolicyValues, { type: "server", message });
  }, [previewErrors]);

  const preset = values.mode !== "custom";
  const waiting = preset && canonical === null;
  const moves = editing && movesLocation(saved, values);
  const sources = (connections.data ?? []).filter((c) => c.role !== "target");
  const dests = destinations.data ?? [];
  // What "More options" holds that is not the default, so a closed section hides nothing that is set.
  const setOptions = ADVANCED.filter((f) => {
    const v = values[f as keyof PolicyValues];
    return v !== "" && v !== false && !(f === "concurrencyPolicy" && v === "Forbid");
  });

  const readiness =
    values.selection !== "named"
      ? null
      : values.source && list(values.topics).length > 0 && (values.where === "archive" ? values.archiveUrl : values.destination)
        ? {
            sourceConnection: values.source,
            topics: list(values.topics),
            ...(values.where === "destination"
              ? { destination: values.destination }
              : { legacyArchive: { url: values.archiveUrl.trim(), ...(values.archiveSecret ? { credentialRef: { name: values.archiveSecret.trim() } } : {}) } }),
          }
        : null;

  const submit = handleSubmit(async (v) => {
    if (moves && !v.confirmMove) {
      setError("confirmMove", { type: "manual", message: "Tick this to write new runs to the new location, or keep the current one." });
      return;
    }
    const [freshSources, freshDestinations] = await Promise.all([
      editing ? Promise.resolve(undefined) : connections.refetch().then((r) => r.data),
      v.where === "destination" ? destinations.refetch().then((r) => r.data) : Promise.resolve(undefined),
    ]);
    const moved = [
      identityChanged("source", "connection", pins.source, freshSources),
      identityChanged("destination", "destination", v.where === "destination" ? pins.destination : undefined, freshDestinations),
    ].filter((x): x is [keyof PolicyValues, string] => x !== null);
    if (moved.length) {
      for (const [field, message] of moved) setError(field, { type: "manual", message });
      setPins((p) => Object.fromEntries(Object.entries(p).filter(([k]) => !moved.some(([f]) => f === k))));
      return;
    }
    onSubmit(v, preset ? (canonical ?? "") : v.cron.trim());
  });

  return (
    <Form onSubmit={(e) => { e.preventDefault(); void submit(); }} noValidate className="lw-schedule-form">
      {editing && (
        <Alert isInline variant="info" title={`Editing revision g${saved.generation}`}>
          Saving replaces the whole future policy: a field left blank goes back to its default. Runs already created keep
          the policy they copied; the next run uses what you save.
          {saved.retention ? " This schedule still carries the withdrawn retention setting; saving removes it (retention is set by a RetentionPolicy)." : ""}
        </Alert>
      )}
      <FormSection title="What to back up" titleElement="h2">
        {editing ? (
          <Field id="sf-source" label="Source connection" help="A different cluster is a different schedule, so the source cannot change.">
            <TextInput id="sf-source" value={values.source} readOnlyVariant="default" aria-describedby="sf-source-helper" />
          </Field>
        ) : (
          <SelectField
            control={control}
            name="source"
            label="Source connection"
            help={
              connections.isPending
                ? "Loading the connections"
                : sources.length === 0
                  ? "No source connection in this namespace yet: add one on the Connections page."
                  : "Each run reads from this cluster. It cannot be changed afterwards."
            }
            options={[
              ["", "Choose a connection"],
              // DUI-48.3: the probe in the connection page's own terms, never "not checked yet" for one that ran.
              ...sources.map((c): [string, string] => [c.name, `${c.name} (${probeShortWords(c.reachability)})`]),
            ]}
          />
        )}
        <Controller
          control={control}
          name="selection"
          render={({ field }) => (
            <FormGroup role="radiogroup" isStack label="Topics" fieldId="sf-selection">
              <Radio id="sf-selection-named" name="selection" label="Named topics" description="An explicit list. Every run backs up exactly these." isChecked={field.value === "named"} onChange={() => field.onChange("named")} />
              <Radio
                id="sf-selection-all"
                name="selection"
                label="All user topics"
                description="Resolved from a topic discovery when each run starts; each run records what it actually covered."
                isChecked={field.value === "all"}
                onChange={() => field.onChange("all")}
              />
            </FormGroup>
          )}
        />
        {values.selection === "named" ? (
          <>
            <TextField control={control} name="topics" label="Topic names" placeholder="orders, payments" help="Comma separated. Names, never patterns." required />
            <TopicPicker
              ns={ns}
              control={control}
              source={values.source}
              onAdd={(t) => setValue("topics", [...list(getValues("topics")), t].join(", "), { shouldValidate: true })}
            />
          </>
        ) : (
          <>
            <Controller
              control={control}
              name="incompleteDiscovery"
              render={({ field, fieldState }) => (
                <FormGroup role="radiogroup" isStack label="When discovery cannot prove it saw every topic" fieldId="sf-incompleteDiscovery" isRequired>
                  <Radio id="sf-incomplete-refuse" name="incompleteDiscovery" label="Refuse: fail the run" isChecked={field.value === "Refuse"} onChange={() => field.onChange("Refuse")} />
                  <Radio
                    id="sf-incomplete-visible"
                    name="incompleteDiscovery"
                    label="Back up the topics it could see, and record the coverage as visible topics only"
                    isChecked={field.value === "BackUpVisibleTopics"}
                    onChange={() => field.onChange("BackUpVisibleTopics")}
                  />
                  <FormHelperText>
                    <HelperText>
                      <HelperTextItem variant={fieldState.error ? "error" : "default"}>
                        {fieldState.error?.message ?? "There is no default: both would be wrong in a way you would not notice."}
                      </HelperTextItem>
                    </HelperText>
                  </FormHelperText>
                </FormGroup>
              )}
            />
            <TextField control={control} name="excludeTopics" label="Leave out these topics" help="Exact names, comma separated. Internal topics are always left out." />
            <TextField control={control} name="excludePrefixes" label="Leave out topics starting with" help="Literal prefixes, comma separated: dev- leaves out dev-orders, not orders-dev." />
          </>
        )}
        {/* DUI-25: the groups whose positions each run records, so a restore maps them and a switchover can move them. */}
        {editing ? (
          <Field
            id="sf-consumerGroups"
            label="Consumer groups"
            help={
              saved.consumerGroups?.length
                ? "Set when the schedule was created; saving keeps them. kubectl changes spec.consumerGroups."
                : "None: runs record no consumer positions, so no restore of them can switch consumers over. Set when the schedule was created; kubectl changes spec.consumerGroups."
            }
          >
            <TextArea
              id="sf-consumerGroups"
              value={(saved.consumerGroups ?? []).join("\n")}
              readOnlyVariant="default"
              autoResize
              aria-describedby="sf-consumerGroups-helper"
            />
          </Field>
        ) : (
          <Controller
            control={control}
            name="consumerGroups"
            render={({ field, fieldState }) => (
              <Field
                id="sf-consumerGroups"
                label="Consumer groups (optional)"
                error={fieldState.error?.message}
                help="One group id per line, exactly as your clients name it; never a pattern. Each run records where these groups stand, so a restore maps their positions and Switch consumers over can move them. Empty records none."
              >
                <TextArea
                  id="sf-consumerGroups"
                  value={field.value}
                  onChange={(_e, v) => field.onChange(v)}
                  onBlur={field.onBlur}
                  placeholder={"orders-service\nbilling-reconciler"}
                  autoResize
                  validated={fieldState.error ? "error" : "default"}
                  aria-describedby="sf-consumerGroups-helper"
                />
              </Field>
            )}
          />
        )}
      </FormSection>

      <FormSection title="When" titleElement="h2">
        <CadenceFields control={control} />
        <TextField
          control={control}
          name="timeZone"
          label="Time zone"
          placeholder="UTC"
          list="lw-zones"
          help="An IANA name such as Europe/Berlin. Blank means UTC."
        />
        <datalist id="lw-zones">
          {ZONES.map((z) => (
            <option key={z} value={z} />
          ))}
        </datalist>
        <section className="lw-preview" aria-label="When it runs">
          <Title headingLevel="h3" size="md">
            When it runs
          </Title>
          <Preview
            control={control}
            onReady={(line, errs) => {
              setCanonical(line);
              setPreviewErrors(errs);
            }}
          />
        </section>
      </FormSection>

      <FormSection title="Where runs are written" titleElement="h2">
        <Controller
          control={control}
          name="destination"
          render={({ field, fieldState }) => (
            <Field
              id="sf-destination"
              label="Destination"
              error={fieldState.error?.message}
              help={
                dests.length === 0 && !destinations.isPending
                  ? "No saved destination in this namespace: add one on the Destinations page, or write to an archive URL."
                  : "The bucket and credential are set once on the destination; each run uses what it has when the run starts."
              }
            >
              <FormSelect
                id="sf-destination"
                value={values.where === "archive" ? ARCHIVE : field.value}
                onChange={(_e, v) => {
                  if (v === ARCHIVE) {
                    setValue("where", "archive");
                    field.onChange("");
                  } else {
                    setValue("where", "destination");
                    field.onChange(v);
                  }
                }}
                validated={fieldState.error ? "error" : "default"}
                aria-describedby="sf-destination-helper"
              >
                <FormSelectOption value="" label={destinations.isPending ? "Loading destinations" : "Choose a destination"} isDisabled />
                {dests.map((d) => (
                  <FormSelectOption key={d.uid} value={d.name} label={destinationText(d)} />
                ))}
                <FormSelectOption value={ARCHIVE} label="An archive URL instead (advanced)" />
              </FormSelect>
            </Field>
          )}
        />
        {values.where === "archive" && (
          <>
            <TextField control={control} name="archiveUrl" label="Archive URL" placeholder="s3://bucket/prefix" required />
            <TextField
              control={control}
              name="archiveSecret"
              label="Credential Secret name"
              help="Only the name is sent. The Secret must carry a logweir-binding key for this location, or every run refuses it; a saved destination avoids this."
            />
          </>
        )}
        {moves && (
          <Controller
            control={control}
            name="confirmMove"
            render={({ field, fieldState }) => (
              <Alert isInline variant="warning" title="This edit moves where runs are written">
                <p>
                  New runs will be written to {values.where === "destination" ? `the destination ${values.destination}` : values.archiveUrl}{" "}
                  instead of {saved.destinationRef ? `the destination ${saved.destinationRef.name}` : saved.archive.url}. Recovery points already
                  taken stay where they are.
                </p>
                <Checkbox
                  id="sf-confirmMove"
                  label="Write new runs to the new location"
                  isChecked={field.value}
                  onChange={(_e, c) => field.onChange(c)}
                  isValid={!fieldState.error}
                  body={fieldState.error?.message}
                />
              </Alert>
            )}
          />
        )}
      </FormSection>

      <ExpandableSection
        toggleText={
          more ? "Fewer options" : `More options: suspension, deadlines, catch-up, retries, concurrency${setOptions.length ? ` (${setOptions.length} set)` : ""}`
        }
        isExpanded={more}
        onToggle={(_e, open) => setMore(open)}
      >
        <Content component="p" className="lw-subtle">
          Every one is optional; blank means the documented default.
        </Content>
        <Controller
          control={control}
          name="suspended"
          render={({ field }) => (
            <FormGroup fieldId="sf-suspended" label={editing ? "Suspension" : "Start"}>
              <Switch
                id="sf-suspended"
                label={editing ? "Suspended: admit no further slots" : "Start suspended"}
                isChecked={field.value}
                onChange={(_e, c) => field.onChange(c)}
              />
            </FormGroup>
          )}
        />
        <div className="lw-form-grid">
          <TextField control={control} name="startingDeadlineSeconds" type="number" label="Starting deadline (seconds)" help="How late a slot may still start. 60 to 604800; blank is 3600." />
          <SelectField
            control={control}
            name="catchUpPolicy"
            label="Missed slots"
            options={[
              ["", "Default: count and skip them"],
              ["None", "Count and skip them"],
              ["Latest", "Run the latest one once"],
            ]}
            help="Latest never runs more than one catch-up, and never a slot from before the revision in force."
          />
          <TextField control={control} name="maxRetries" type="number" label="Retries of a failed slot" help="0 to 3; blank is none. Each retry is a new backup." />
          <TextField control={control} name="retryDelaySeconds" type="number" label="Retry delay (seconds)" help="60 to 21600; blank is 300 when retries are set." />
          <TextField control={control} name="activeDeadlineSeconds" type="number" label="Run deadline (seconds)" help="60 to 86400; blank is 3600. All user topics needs at least 120." />
          <SelectField
            control={control}
            name="concurrencyPolicy"
            label="Overlapping runs"
            options={[
              ["", "Default: never overlap"],
              ["Forbid", "Never overlap"],
              ["Allow", "Allow overlapping slots"],
            ]}
          />
        </div>
      </ExpandableSection>

      {!editing && (
        <FormSection title="Readiness" titleElement="h2">
          <Readiness
            ns={ns}
            request={readiness}
            groupsSelected={groupList(values.consumerGroups ?? "").length > 0}
            why={
              values.selection === "all"
                ? "A readiness check needs named topics. All user topics are found when each run starts, so the run's own check records the result."
                : "Choose a source, name the topics and choose where runs are written to check readiness."
            }
          />
        </FormSection>
      )}

      {status}
      <ActionGroup>
        <Button type="submit" variant="primary" isLoading={pending} isDisabled={pending || waiting}>
          {submitText}
        </Button>
        <Button variant="link" onClick={onCancel}>
          Cancel
        </Button>
      </ActionGroup>
      {waiting && (
        <Content component="p" className="lw-subtle lw-small">
          {submitText} is available once the preview above is back: the API compiles the preset, and that line is what is saved.
        </Content>
      )}
    </Form>
  );
}
