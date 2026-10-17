// Rotate access: the complete four-grant block plus the CA reference, sent
// with the generation this page read. Each grant starts on "keep"; a grant set
// to absent here is removed. Location and transport security cannot change.
import { useEffect, useState } from "react";
import { Controller, useForm, useWatch, type Path, type UseFormReturn } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import {
  Alert,
  Button,
  Content,
  Form,
  FormSelect,
  FormSelectOption,
  Grid,
  GridItem,
  Modal,
  ModalBody,
  ModalFooter,
  ModalHeader,
  TextInput,
  Title,
} from "@patternfly/react-core";
import { ApiError } from "../../api/client";
import { placeProblems } from "../../api/connections";
import {
  GRANT_ROLES,
  WRITE_ONLY_SENTENCE,
  namedSecrets,
  rotateDefaults,
  rotationBody,
  rotationField,
  onScreen,
  rotationProblems,
  sourcesOf,
  useRotateAccess,
  type Destination,
  type RotateValues,
} from "../../api/destinations";
import { Field, WriteOutcome, controlState, withProblems } from "../connections/parts";
import { grantsSchema } from "./AddDestination";
import { GrantFields, clearGrantSecrets, type GrantsForm } from "./GrantFields";
import { Revealed } from "../../components/Revealed";

export function RotateAccess({
  ws,
  destination: live,
  onClose,
  onRotated,
  onReRead,
}: {
  ws: string;
  destination: Destination;
  onClose: () => void;
  /** `wroteSecret`: a grant was entered as a new credential, so a new Secret was written (DUI-66). */
  onRotated: (d: Destination, wroteSecret: boolean) => void;
  onReRead: () => void;
}) {
  // The destination this dialog opened with, and its generation, until the operator re-reads
  // it: a resend after a lost answer whose rotation landed is then refused (412), never a
  // second rotation.
  const [d, setD] = useState(live);
  const [adopting, setAdopting] = useState(false);
  useEffect(() => {
    if (adopting && live.generation !== d.generation) {
      setD(live);
      setAdopting(false);
    }
  }, [adopting, live, d.generation]);
  const rotate = useRotateAccess(ws, d.name);
  const [unmatched, setUnmatched] = useState<string[]>([]);
  const schema = z
    .object({ grants: grantsSchema, caChange: z.enum(["keep", "set", "clear"]), caName: z.string(), caKey: z.string() })
    .superRefine(withProblems((v: RotateValues) => rotationProblems(v, d)));
  const form = useForm<RotateValues>({ resolver: zodResolver(schema), defaultValues: rotateDefaults() });
  const { control, handleSubmit, setError, formState } = form;
  const grantsForm = form as unknown as UseFormReturn<GrantsForm>;
  const caChange = useWatch({ control, name: "caChange" });
  const errors = formState.errors;
  const secretNames = namedSecrets(d.access);
  useEffect(() => () => clearGrantSecrets(grantsForm, GRANT_ROLES), []); // on unmount

  const submit = handleSubmit((values) => {
    setUnmatched([]);
    // What each grant's source and the CA choice were when sent: they decide which inputs an error can be shown on.
    const shown = { grants: sourcesOf(values.grants), caChange: values.caChange };
    const body = rotationBody(values, d);
    for (const role of GRANT_ROLES) {
      values.grants[role].accessKeyId = "";
      values.grants[role].secretAccessKey = "";
    }
    clearGrantSecrets(grantsForm, GRANT_ROLES);
    rotate.mutate(body, {
      onSuccess: (item) => onRotated(item, GRANT_ROLES.some((r) => shown.grants[r].source === "new")),
      onError: (error) => {
        const placed = placeProblems(error, rotationField);
        const fields: Partial<Record<string, string>> = onScreen(placed.fields, shown.grants);
        // The CA's name and key are shown only while "Point it at a ConfigMap" is chosen.
        if (shown.caChange !== "set") {
          for (const ca of ["caName", "caKey"] as const) {
            const message = fields[ca];
            if (message) {
              fields.caChange = fields.caChange ? `${fields.caChange} ${message}` : message;
              delete fields[ca];
            }
          }
        }
        for (const [field, message] of Object.entries(fields)) setError(field as Path<RotateValues>, { type: "server", message: message as string });
        setUnmatched(placed.unmatched);
      },
    });
  });

  const moved = rotate.error instanceof ApiError && rotate.error.status === 412;

  return (
    <Modal isOpen onClose={onClose} variant="medium" aria-labelledby="rotate-title" aria-describedby="rotate-description">
      <ModalHeader
        title={`Rotate access for ${d.name}`}
        labelId="rotate-title"
        description="This sends the complete four-grant block. Each grant starts on keep, which sends it as it is; a grant set to none is removed."
        descriptorId="rotate-description"
      />
      <ModalBody>
        <Form id="rotate-form" onSubmit={(e) => void submit(e)} noValidate aria-busy={rotate.isPending}>
          <Content component="p" className="lw-subtle lw-small">
            It carries the generation this page read ({d.generation}). If the destination changed since, the service refuses it and nothing is
            written. The location and the transport security cannot change; only the credentials and the CA reference can.
          </Content>
          <Content component="p" className="lw-subtle lw-small">
            {WRITE_ONLY_SENTENCE}
          </Content>
          {GRANT_ROLES.map((role) => (
            <GrantFields key={role} form={grantsForm} role={role} kind="rotate" current={d.access[role]} secretNames={secretNames} />
          ))}
          <Title headingLevel="h4" size="md" className="lw-gap-top">
            Private CA
          </Title>
          <Grid hasGutter md={4}>
            <GridItem>
              <Controller
                name="caChange"
                control={control}
                render={({ field }) => (
                  <Field
                    id="rotate-ca-change"
                    label="Change"
                    error={errors.caChange?.message}
                    help={d.transport.caBundle ? `Now: ConfigMap ${d.transport.caBundle.configMapName}, key ${d.transport.caBundle.key}.` : "Now: the runner image's own trust store."}
                  >
                    <FormSelect {...controlState("rotate-ca-change", errors.caChange?.message)} value={field.value} onChange={(_e, v) => field.onChange(v)}>
                      <FormSelectOption value="keep" label="Leave it alone" />
                      <FormSelectOption value="set" label="Point it at a ConfigMap" />
                      <FormSelectOption value="clear" label="Clear it" />
                    </FormSelect>
                  </Field>
                )}
              />
            </GridItem>
            {caChange === "set" && (
              <>
                <GridItem>
                  <Controller
                    name="caName"
                    control={control}
                    render={({ field }) => (
                      <Field id="rotate-ca-name" label="ConfigMap" isRequired error={errors.caName?.message}>
                        <TextInput {...controlState("rotate-ca-name", errors.caName?.message, false)} value={field.value} onChange={(_e, v) => field.onChange(v)} />
                      </Field>
                    )}
                  />
                </GridItem>
                <GridItem>
                  <Controller
                    name="caKey"
                    control={control}
                    render={({ field }) => (
                      <Field id="rotate-ca-key" label="Data key" error={errors.caKey?.message} help="Blank means ca.crt.">
                        <TextInput {...controlState("rotate-ca-key", errors.caKey?.message)} value={field.value} onChange={(_e, v) => field.onChange(v)} />
                      </Field>
                    )}
                  />
                </GridItem>
              </>
            )}
          </Grid>
          <div aria-live="polite">
            {rotate.isPending && (
              <Content component="p" className="lw-subtle">
                Sent, waiting for the service. Submitting again is disabled until it answers.
              </Content>
            )}
            {moved ? (
              <Revealed when={rotate.error}>
                <Alert
                  isInline
                  variant="warning"
                  title="The destination changed since this page read it. Nothing was written."
                  actionLinks={
                    <Button
                      variant="link"
                      isInline
                      onClick={() => {
                        setAdopting(true);
                        onReRead();
                      }}
                    >
                      Re-read the destination
                    </Button>
                  }
                >
                  Re-read it, check the grants again, and rotate again. The credential fields were cleared; type them again.
                </Alert>
              </Revealed>
            ) : (
              rotate.isError && <WriteOutcome error={rotate.error} subject="the access rotation" verb="rotate" credentials unmatched={unmatched} />
            )}
          </div>
        </Form>
      </ModalBody>
      <ModalFooter>
        <Button type="submit" form="rotate-form" isLoading={rotate.isPending} isDisabled={rotate.isPending}>
          Rotate access
        </Button>
        <Button variant="link" onClick={onClose}>
          Cancel
        </Button>
      </ModalFooter>
    </Modal>
  );
}
