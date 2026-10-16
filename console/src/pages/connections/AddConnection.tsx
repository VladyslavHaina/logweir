// Add a connection: where the brokers are, how to authenticate, and the
// credential typed once. The service names the connection and turns the
// credential into a Secret bound to it; nothing on this page keeps it.
import { useEffect, useState } from "react";
import { Controller, useForm, useWatch } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import {
  Button,
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
  Switch,
  TextArea,
  TextInput,
} from "@patternfly/react-core";
import {
  AUTH_MODES,
  AUTH_WORDS,
  CONNECTION_DEFAULTS,
  CONNECTION_SECRET_FIELDS,
  SASL_MODES,
  connectionBody,
  connectionField,
  connectionProblems,
  placeProblems,
  useCreateConnection,
  useIntentKey,
  type ConnectionValues,
} from "../../api/connections";
import { Field, WriteOutcome, controlState, withProblems } from "./parts";

const schema = z
  .object({
    role: z.enum(["source", "target"]),
    servers: z.string(),
    mode: z.enum(AUTH_MODES),
    username: z.string(),
    password: z.string(),
    certificatePem: z.string(),
    privateKeyPem: z.string(),
    tls: z.boolean(),
    caName: z.string(),
    caKey: z.string(),
    registryUrl: z.string(),
    registryUsername: z.string(),
    registryPassword: z.string(),
  })
  .superRefine(withProblems(connectionProblems));

export const CREDENTIAL_NOTE =
  "The credential you type is sent once, to the Logweir service, which stores it in a Secret owned by and bound to this connection. It is never shown again, never kept by this page and never written to browser storage; no response can read it back.";

export const REGISTRY_NOTE =
  "Only when these topics' records carry Confluent wire-format schema IDs. A backup from a source connection with a registry captures every schema in it; a restore into a target connection with a registry imports the schemas the restored records need, keeping their IDs. A connection cannot be changed later: to add a registry, add a new connection. The password goes into this connection's own Secret, as the Kafka credential does.";

export function AddConnection({
  ws,
  onClose,
  onCreated,
}: {
  ws: string;
  onClose: () => void;
  onCreated: (name: string, replayed: boolean) => void;
}) {
  const create = useCreateConnection(ws);
  const intent = useIntentKey("connection");
  const [unmatched, setUnmatched] = useState<string[]>([]);
  const [local, setLocal] = useState<unknown>(null);
  const { control, register, handleSubmit, setValue, setError, formState } = useForm<ConnectionValues>({
    resolver: zodResolver(schema),
    defaultValues: CONNECTION_DEFAULTS,
  });
  const errors = formState.errors;
  const mode = useWatch({ control, name: "mode" });
  const tls = useWatch({ control, name: "tls" });
  const sasl = SASL_MODES.includes(mode);
  const credentialKind = sasl ? "sasl" : mode === "mtls" ? "mtls" : "none";

  const clearSecrets = () => {
    for (const f of CONNECTION_SECRET_FIELDS) setValue(f, "");
  };
  // A credential typed for one kind of authentication never rides along with
  // another (the registry's is its own), and none outlives the form.
  useEffect(() => {
    for (const f of CONNECTION_SECRET_FIELDS) if (f !== "registryPassword") setValue(f, "");
  }, [credentialKind, setValue]);
  useEffect(() => clearSecrets, []);

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
    const body = connectionBody(values);
    const sentMode = values.mode;
    for (const f of CONNECTION_SECRET_FIELDS) values[f] = "";
    clearSecrets();
    create.mutate(
      { key, body },
      {
        onSuccess: (answer) => {
          intent.settle();
          onCreated(answer.item.name, answer.replayed === true);
        },
        onError: (error) => {
          const placed = placeProblems(error, (path) => connectionField(path, sentMode));
          for (const [field, message] of Object.entries(placed.fields)) {
            setError(field as keyof ConnectionValues, { type: "server", message: message as string });
          }
          setUnmatched(placed.unmatched);
        },
      },
    );
  });

  const err = (f: keyof ConnectionValues) => errors[f]?.message;
  const caProblem = Boolean(err("caName") || err("caKey"));
  const [caExpanded, setCaExpanded] = useState(false);
  useEffect(() => {
    if (caProblem) setCaExpanded(true);
  }, [caProblem]);
  const registryProblem = Boolean(err("registryUrl") || err("registryUsername") || err("registryPassword"));
  const [registryExpanded, setRegistryExpanded] = useState(false);
  useEffect(() => {
    if (registryProblem) setRegistryExpanded(true);
  }, [registryProblem]);

  return (
    <Modal isOpen onClose={onClose} variant="medium" aria-labelledby="add-connection-title" aria-describedby="add-connection-description">
      <ModalHeader
        title="Add a connection"
        labelId="add-connection-title"
        description="A Kafka cluster Logweir backs up from or restores into. The controller probes it and records the cluster id the broker reports."
        descriptorId="add-connection-description"
      />
      <ModalBody>
        <Form id="add-connection-form" onSubmit={(e) => void submit(e)} noValidate aria-busy={create.isPending}>
          <Content component="p" className="lw-subtle">
            The service names the connection: conn- followed by 26 characters. The role is how lists, schedules and the restore wizard tell
            a source from a target.
          </Content>
          <Controller
            name="role"
            control={control}
            render={({ field }) => (
              <Field id="connection-role" label="Role" role="radiogroup" isInline isRequired error={err("role")} help="Backups read from a source; restores write into a target.">
                <Radio id="connection-role-source" name="connection-role" label="Source" isChecked={field.value === "source"} onChange={() => field.onChange("source")} />
                <Radio id="connection-role-target" name="connection-role" label="Target" isChecked={field.value === "target"} onChange={() => field.onChange("target")} />
              </Field>
            )}
          />
          <Controller
            name="servers"
            control={control}
            render={({ field }) => (
              <Field id="connection-servers" label="Bootstrap servers" isRequired error={err("servers")} help="host:port, separated by commas, as a client inside the cluster dials them: for example kafka-0.kafka.svc:9093.">
                <TextInput {...controlState("connection-servers", err("servers"))} value={field.value} onChange={(_e, v) => field.onChange(v)} onBlur={field.onBlur} ref={field.ref} />
              </Field>
            )}
          />
          <Grid hasGutter md={6}>
            <GridItem>
              <Controller
                name="mode"
                control={control}
                render={({ field }) => (
                  <Field id="connection-mode" label="Authentication" isRequired error={err("mode")} help="SASL/PLAIN and a TLS client certificate both need TLS on.">
                    <FormSelect {...controlState("connection-mode", err("mode"))} value={field.value} onChange={(_e, v) => field.onChange(v)} aria-label="Authentication">
                      {AUTH_MODES.map((m) => (
                        <FormSelectOption key={m} value={m} label={AUTH_WORDS[m]} />
                      ))}
                    </FormSelect>
                  </Field>
                )}
              />
            </GridItem>
            <GridItem>
              <Controller
                name="tls"
                control={control}
                render={({ field }) => (
                  <Field id="connection-tls-group" label="Transport" role="group" error={err("tls")} help="The only switch that turns TLS on. TLS with no authentication is not supported by saved connections: it is refused, never dialled without TLS.">
                    <Switch
                      id="connection-tls"
                      label="TLS"
                      isChecked={field.value}
                      onChange={(_e, v) => field.onChange(v)}
                      aria-describedby="connection-tls-group-helper"
                    />
                  </Field>
                )}
              />
            </GridItem>
          </Grid>
          {sasl && (
            <Grid hasGutter md={6}>
              <GridItem>
                <Controller
                  name="username"
                  control={control}
                  render={({ field }) => (
                    <Field id="connection-username" label="SASL username" isRequired error={err("username")} help="The principal Logweir presents.">
                      <TextInput {...controlState("connection-username", err("username"))} value={field.value} onChange={(_e, v) => field.onChange(v)} onBlur={field.onBlur} ref={field.ref} autoComplete="off" />
                    </Field>
                  )}
                />
              </GridItem>
              <GridItem>
                <Field id="connection-password" label="SASL password" isRequired error={err("password")} help="Entered once. Never shown again.">
                  <TextInput {...controlState("connection-password", err("password"))} type="password" autoComplete="new-password" {...register("password")} />
                </Field>
              </GridItem>
            </Grid>
          )}
          {mode === "mtls" && (
            <>
              <Field id="connection-certificate" label="Client certificate (PEM)" isRequired error={err("certificatePem")} help="The certificate, or its chain, pasted once.">
                <TextArea {...controlState("connection-certificate", err("certificatePem"))} rows={3} autoComplete="off" spellCheck={false} resizeOrientation="vertical" {...register("certificatePem")} />
              </Field>
              <Field id="connection-private-key" label="Client private key (PEM, unencrypted)" isRequired error={err("privateKeyPem")} help="Pasted once and sent once. It becomes a Secret bound to this connection.">
                <TextArea {...controlState("connection-private-key", err("privateKeyPem"))} rows={3} autoComplete="off" spellCheck={false} resizeOrientation="vertical" {...register("privateKeyPem")} />
              </Field>
            </>
          )}
          {(sasl || mode === "mtls") && (
            <Content component="p" className="lw-subtle lw-small">
              {CREDENTIAL_NOTE}
            </Content>
          )}
          <ExpandableSection
            toggleText="Private certificate authority (optional)"
            isIndented
            isExpanded={caExpanded}
            onToggle={(_e, open) => setCaExpanded(open)}
          >
            <Content component="p" className="lw-subtle lw-small">
              Only when the brokers' certificates are signed by an authority the runner image does not already trust: one key of a ConfigMap
              in this workspace holding the PEM certificate(s). It replaces the default trust store for this connection and needs TLS on.
            </Content>
            <Grid hasGutter md={6}>
              <GridItem>
                <Controller
                  name="caName"
                  control={control}
                  render={({ field }) => (
                    <Field id="connection-ca-name" label="ConfigMap" error={err("caName")} help={tls ? "Blank uses the runner image's own trust store." : "Needs TLS on."}>
                      <TextInput {...controlState("connection-ca-name", err("caName"))} value={field.value} onChange={(_e, v) => field.onChange(v)} onBlur={field.onBlur} ref={field.ref} />
                    </Field>
                  )}
                />
              </GridItem>
              <GridItem>
                <Controller
                  name="caKey"
                  control={control}
                  render={({ field }) => (
                    <Field id="connection-ca-key" label="Data key" error={err("caKey")} help="For example ca.crt.">
                      <TextInput {...controlState("connection-ca-key", err("caKey"))} value={field.value} onChange={(_e, v) => field.onChange(v)} onBlur={field.onBlur} ref={field.ref} />
                    </Field>
                  )}
                />
              </GridItem>
            </Grid>
          </ExpandableSection>
          <ExpandableSection
            toggleText="Schema Registry (optional)"
            isIndented
            isExpanded={registryExpanded}
            onToggle={(_e, open) => setRegistryExpanded(open)}
          >
            <Content component="p" className="lw-subtle lw-small">
              {REGISTRY_NOTE}
            </Content>
            <Controller
              name="registryUrl"
              control={control}
              render={({ field }) => (
                <Field id="connection-registry-url" label="Registry URL" error={err("registryUrl")} help="For example https://schema-registry.kafka.svc:8081. The connection's private CA, when set, is trusted for it too.">
                  <TextInput {...controlState("connection-registry-url", err("registryUrl"))} type="url" value={field.value} onChange={(_e, v) => field.onChange(v)} onBlur={field.onBlur} ref={field.ref} autoComplete="off" />
                </Field>
              )}
            />
            <Grid hasGutter md={6}>
              <GridItem>
                <Controller
                  name="registryUsername"
                  control={control}
                  render={({ field }) => (
                    <Field id="connection-registry-username" label="Registry username" error={err("registryUsername")} help="Blank for a registry without authentication.">
                      <TextInput {...controlState("connection-registry-username", err("registryUsername"))} value={field.value} onChange={(_e, v) => field.onChange(v)} onBlur={field.onBlur} ref={field.ref} autoComplete="off" />
                    </Field>
                  )}
                />
              </GridItem>
              <GridItem>
                <Field id="connection-registry-password" label="Registry password" error={err("registryPassword")} help="Entered once. Never shown again.">
                  <TextInput {...controlState("connection-registry-password", err("registryPassword"))} type="password" autoComplete="new-password" {...register("registryPassword")} />
                </Field>
              </GridItem>
            </Grid>
          </ExpandableSection>
          <div aria-live="polite">
            {create.isPending && (
              <Content component="p" className="lw-subtle">
                Sent, waiting for the service. Submitting again is disabled until it answers, so one click makes one request.
              </Content>
            )}
            {local !== null && <WriteOutcome error={local} subject="the connection" credentials unmatched={[]} />}
            {create.isError && <WriteOutcome error={create.error} subject="the connection" credentials unmatched={unmatched} />}
          </div>
        </Form>
      </ModalBody>
      <ModalFooter>
        <Button type="submit" form="add-connection-form" isLoading={create.isPending} isDisabled={create.isPending}>
          Add connection
        </Button>
        <Button variant="link" onClick={onClose}>
          Cancel
        </Button>
      </ModalFooter>
    </Modal>
  );
}
