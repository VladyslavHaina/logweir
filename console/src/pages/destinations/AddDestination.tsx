// Add a destination: one archive location written down once, with its
// credentials typed once. Location and transport are immutable afterwards.
import { useEffect, useState } from "react";
import { Controller, useForm, type Path, type UseFormReturn } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import {
  Button,
  Checkbox,
  Content,
  ExpandableSection,
  Form,
  FormSelect,
  FormSelectOption,
  Grid,
  GridItem,
  Modal,
  ModalBody,
  ModalFooter,
  ModalHeader,
  Radio,
  TextInput,
  Title,
} from "@patternfly/react-core";
import { placeProblems, useIntentKey } from "../../api/connections";
import {
  DEFAULT_DESTINATION_SENTENCE,
  DESTINATION_DEFAULTS,
  GRANT_ROLES,
  WRITE_ONLY_SENTENCE,
  WRITE_PROBE_SENTENCE,
  destinationBody,
  destinationField,
  destinationProblems,
  onScreen,
  sourcesOf,
  useCreateDestination,
  type DestinationValues,
} from "../../api/destinations";
import { Field, WriteOutcome, controlState, withProblems } from "../connections/parts";
import { GrantFields, clearGrantSecrets, type GrantsForm } from "./GrantFields";

const grant = z.object({
  source: z.enum(["keep", "absent", "existing", "new", "workloadIdentity", "controllerIdentity", "archiveReadGrant"]),
  accessKeyId: z.string(),
  secretAccessKey: z.string(),
  serviceAccount: z.string(),
  secret: z.string(),
});

export const grantsSchema = z.object({ archiveWrite: grant, archiveRead: grant, evidenceWrite: grant, evidenceRead: grant });

const schema = z
  .object({
    name: z.string(),
    description: z.string(),
    bucket: z.string(),
    prefix: z.string(),
    region: z.string(),
    endpoint: z.string(),
    addressing: z.enum(["pathStyle", "virtualHosted"]),
    security: z.enum(["tls", "insecureHttp"]),
    caName: z.string(),
    caKey: z.string(),
    writeProbe: z.enum(["createOnlyMarker", "disabled"]),
    isDefault: z.boolean(),
    grants: grantsSchema,
  })
  .superRefine(withProblems(destinationProblems));

type TextField = "name" | "description" | "bucket" | "prefix" | "region" | "endpoint" | "caName" | "caKey";

export function AddDestination({ ws, onClose, onCreated }: { ws: string; onClose: () => void; onCreated: (name: string) => void }) {
  const create = useCreateDestination(ws);
  const intent = useIntentKey("destination");
  const [unmatched, setUnmatched] = useState<string[]>([]);
  const [local, setLocal] = useState<unknown>(null);
  const form = useForm<DestinationValues>({ resolver: zodResolver(schema), defaultValues: DESTINATION_DEFAULTS });
  const { control, handleSubmit, setError, formState } = form;
  const grantsForm = form as unknown as UseFormReturn<GrantsForm>;
  const errors = formState.errors;
  const [optionalOpen, setOptionalOpen] = useState(false);
  const optionalProblem = Boolean(errors.grants?.archiveRead || errors.grants?.evidenceWrite || errors.grants?.evidenceRead);
  useEffect(() => {
    if (optionalProblem) setOptionalOpen(true);
  }, [optionalProblem]);
  // No typed credential outlives the form.
  useEffect(() => () => clearGrantSecrets(grantsForm, GRANT_ROLES), []); // runs once, on unmount

  const submit = handleSubmit((values) => {
    setUnmatched([]);
    setLocal(null);
    let key: string;
    try {
      key = intent.take();
    } catch (refusal) {
      setLocal(refusal);
      return;
    }
    const shown = sourcesOf(values.grants);
    const body = destinationBody(values);
    for (const role of GRANT_ROLES) {
      values.grants[role].accessKeyId = "";
      values.grants[role].secretAccessKey = "";
    }
    clearGrantSecrets(grantsForm, GRANT_ROLES);
    create.mutate(
      { key, body },
      {
        onSuccess: (answer) => {
          intent.settle();
          onCreated(answer.item.name);
        },
        onError: (error) => {
          const placed = placeProblems(error, destinationField);
          for (const [field, message] of Object.entries(onScreen(placed.fields, shown))) {
            setError(field as Path<DestinationValues>, { type: "server", message: message as string });
          }
          setUnmatched(placed.unmatched);
        },
      },
    );
  });

  const err = (f: TextField | "addressing" | "security" | "writeProbe" | "isDefault") => errors[f]?.message;
  const text = (f: TextField, label: string, help?: string, required = false) => (
    <Controller
      name={f}
      control={control}
      render={({ field }) => (
        <Field id={`destination-${f}`} label={label} isRequired={required} error={err(f)} help={help}>
          <TextInput {...controlState(`destination-${f}`, err(f), Boolean(help))} value={field.value} onChange={(_e, v) => field.onChange(v)} onBlur={field.onBlur} ref={field.ref} />
        </Field>
      )}
    />
  );

  return (
    <Modal isOpen onClose={onClose} variant="medium" aria-labelledby="add-destination-title" aria-describedby="add-destination-description">
      <ModalHeader
        title="Add a destination"
        labelId="add-destination-title"
        description="An archive location written down once. Schedules, backups and restores name it instead of each spelling a bucket, an endpoint and a credential."
        descriptorId="add-destination-description"
      />
      <ModalBody>
        <Form id="add-destination-form" onSubmit={(e) => void submit(e)} noValidate aria-busy={create.isPending}>
          {text("name", "Name", "Everything references this destination by name, so pick one an operator will recognise in a year. It cannot change.", true)}
          {text("description", "Description")}
          <Title headingLevel="h3" size="md">
            Location (cannot change later)
          </Title>
          <Grid hasGutter md={6}>
            <GridItem>{text("bucket", "Bucket", undefined, true)}</GridItem>
            <GridItem>{text("prefix", "Prefix", "Blank is the bucket root. logweir and everything under it is reserved for evidence.")}</GridItem>
            <GridItem>{text("region", "Region")}</GridItem>
            <GridItem>{text("endpoint", "Endpoint", "An origin: scheme, host and optional port. Blank means AWS S3.")}</GridItem>
          </Grid>
          <Controller
            name="addressing"
            control={control}
            render={({ field }) => (
              <Field id="destination-addressing" label="Addressing" role="radiogroup" isInline error={err("addressing")} help="How a request names the bucket, and nothing else. It does not decide whether the connection is encrypted.">
                <Radio id="destination-addressing-path" name="destination-addressing" label="Path-style (endpoint/bucket/key)" isChecked={field.value === "pathStyle"} onChange={() => field.onChange("pathStyle")} />
                <Radio id="destination-addressing-virtual" name="destination-addressing" label="Virtual-hosted (bucket.endpoint/key)" isChecked={field.value === "virtualHosted"} onChange={() => field.onChange("virtualHosted")} />
              </Field>
            )}
          />
          <Controller
            name="security"
            control={control}
            render={({ field }) => (
              <Field id="destination-security" label="Transport security (cannot change later)" role="radiogroup" isInline error={err("security")} help="The only control that decides whether the connection is encrypted, independent of the addressing.">
                <Radio id="destination-security-tls" name="destination-security" label="TLS" isChecked={field.value === "tls"} onChange={() => field.onChange("tls")} />
                <Radio id="destination-security-http" name="destination-security" label="Plaintext HTTP (insecureHttp; needs an explicit http endpoint)" isChecked={field.value === "insecureHttp"} onChange={() => field.onChange("insecureHttp")} />
              </Field>
            )}
          />
          <Grid hasGutter md={6}>
            <GridItem>{text("caName", "Private CA ConfigMap", "A ConfigMap, never a Secret: a CA certificate is public. TLS only.")}</GridItem>
            <GridItem>{text("caKey", "CA data key", "Blank means ca.crt.")}</GridItem>
          </Grid>
          <Title headingLevel="h3" size="md">
            Access
          </Title>
          <Content component="p" className="lw-subtle lw-small">
            {WRITE_ONLY_SENTENCE}
          </Content>
          <GrantFields form={grantsForm} role="archiveWrite" kind="create" />
          <ExpandableSection
            toggleText="Separate credentials for restores and evidence (optional)"
            isExpanded={optionalOpen}
            onToggle={(_e, open) => setOptionalOpen(open)}
            isIndented
          >
            {GRANT_ROLES.filter((r) => r !== "archiveWrite").map((role) => (
              <GrantFields key={role} form={grantsForm} role={role} kind="create" />
            ))}
          </ExpandableSection>
          <Controller
            name="writeProbe"
            control={control}
            render={({ field }) => (
              <Field id="destination-write-probe" label="Write probe" error={err("writeProbe")} help={WRITE_PROBE_SENTENCE}>
                <FormSelect {...controlState("destination-write-probe", err("writeProbe"))} value={field.value} onChange={(_e, v) => field.onChange(v)}>
                  <FormSelectOption value="createOnlyMarker" label="Create-only marker" />
                  <FormSelectOption value="disabled" label="Disabled" />
                </FormSelect>
              </Field>
            )}
          />
          <Controller
            name="isDefault"
            control={control}
            render={({ field }) => (
              <Field id="destination-default-group" label="Workspace default" role="group" error={err("isDefault")} help={DEFAULT_DESTINATION_SENTENCE}>
                <Checkbox
                  id="destination-default"
                  label="Make this the workspace default"
                  isChecked={field.value}
                  onChange={(_e, v) => field.onChange(v)}
                  aria-describedby="destination-default-group-helper"
                />
              </Field>
            )}
          />
          <div aria-live="polite">
            {create.isPending && (
              <Content component="p" className="lw-subtle">
                Sent, waiting for the service. Submitting again is disabled until it answers, so one click makes one request.
              </Content>
            )}
            {local !== null && <WriteOutcome error={local} subject="the destination" credentials unmatched={[]} />}
            {create.isError && <WriteOutcome error={create.error} subject="the destination" credentials unmatched={unmatched} />}
          </div>
        </Form>
      </ModalBody>
      <ModalFooter>
        <Button type="submit" form="add-destination-form" isLoading={create.isPending} isDisabled={create.isPending}>
          Add destination
        </Button>
        <Button variant="link" onClick={onClose}>
          Cancel
        </Button>
      </ModalFooter>
    </Modal>
  );
}
