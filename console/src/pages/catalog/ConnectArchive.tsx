// Connect an existing archive: one durable create (a RecoveryCatalog, a
// read-only view over a destination) under one idempotency key. The key is held
// for the request it was spent on: a double click, a lost answer and a resend
// are one request; a corrected field is a new request and gets a new key.
import { useState } from "react";
import { Link } from "@tanstack/react-router";
import { Controller, useForm, useWatch } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import {
  Alert,
  Button,
  Content,
  Form,
  FormGroup,
  FormHelperText,
  FormSelect,
  FormSelectOption,
  HelperText,
  HelperTextItem,
  Modal,
  ModalBody,
  ModalFooter,
  ModalHeader,
  ModalVariant,
  Radio,
  TextInput,
} from "@patternfly/react-core";
import { ApiError } from "../../api/client";
import {
  LocalRefusal,
  mintKey,
  outcomeOf,
  useCatalogs,
  useConnectArchive,
  useDestinationChoices,
  type CatalogResponse,
  type ConnectArchiveRequest,
} from "../../api/backups";
import { CATALOG_MODE_HELP, CONNECT_SENTENCE, INTENT_USED_SENTENCE, SYNC_MODES, catalogOf, connectFieldOf, isIntentConflict } from "./model";
import { Prose } from "../../components/Prose";
import { Revealed } from "../../components/Revealed";

/** A DNS-1123 subdomain, the catalog name's rule. */
const DNS_SUBDOMAIN = /^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$/;

export const connectSchema = z.object({
  name: z
    .string()
    .trim()
    .min(1, "A catalog needs a name; it is also what makes this submission repeatable.")
    .max(253, "At most 253 characters.")
    .regex(DNS_SUBDOMAIN, "Lower-case letters, digits, '-' and '.', starting and ending with a letter or digit."),
  destination: z.string().trim().min(1, "Name the saved destination whose bucket holds the archive."),
  syncMode: z.enum(SYNC_MODES),
});

export type ConnectValues = z.infer<typeof connectSchema>;

/** The body this form sends. */
export function connectBody(values: ConnectValues): ConnectArchiveRequest {
  return { name: values.name.trim(), destinationRef: { name: values.destination.trim() }, syncMode: values.syncMode };
}

/** The key for this body: the one held when the body is the one it was spent on, a new one otherwise. */
export function keyFor(held: { key: string; body: string } | null, body: ConnectArchiveRequest, mint: () => string): { key: string; body: string } {
  const now = JSON.stringify([body.name, body.destinationRef?.name ?? "", body.syncMode]);
  return held && held.body === now ? held : { key: mint(), body: now };
}

/** What a refusal on this route means, in words. */
export function connectRefusalWords(error: unknown, name: string): string {
  if (outcomeOf(error) === "unknown") {
    return `No answer reached this page, so whether catalog ${name} was created is unknown. Your input is kept. Submitting again is safe: it resends the key this submission is holding, and the API answers a repeat of that key with the catalog the first request made rather than creating a second.`;
  }
  if (isIntentConflict(error)) return INTENT_USED_SENTENCE;
  if (error instanceof ApiError && error.code === "state_conflict") {
    return `A catalog named ${name} already exists and this request did not create it, so nothing was changed and nothing was adopted. Choose another name.`;
  }
  if (error instanceof ApiError && error.fieldErrors.length > 0) return `Catalog ${name} was not created: fix the fields marked below. Your input is kept.`;
  return error instanceof Error ? error.message : String(error);
}

export function ConnectArchive({ ns, open, onClose }: { ns: string; open: boolean; onClose: () => void }) {
  const connect = useConnectArchive(ns);
  const choices = useDestinationChoices(ns);
  const [held, setHeld] = useState<{ key: string; body: string } | null>(null);
  const [made, setMade] = useState<CatalogResponse | null>(null);
  const [localError, setLocalError] = useState<string | null>(null);
  const [unmatched, setUnmatched] = useState<string[]>([]);
  const form = useForm<ConnectValues>({
    resolver: zodResolver(connectSchema),
    defaultValues: { name: "", destination: "", syncMode: "full" },
  });
  const { errors } = form.formState;
  const list = choices.data ?? null;
  // DUI-10: one destination has one catalog per namespace; a second one never syncs.
  const catalogs = useCatalogs(ns);
  const chosen = useWatch({ control: form.control, name: "destination" });
  const holder = catalogOf(catalogs.data, chosen.trim());
  const [duplicated, setDuplicated] = useState<string | null>(null);

  const submit = form.handleSubmit((values) => {
    if (connect.isPending) return;
    const body = connectBody(values);
    let next: { key: string; body: string };
    try {
      next = keyFor(held, body, () => mintKey("catalog"));
    } catch (error) {
      setLocalError(error instanceof LocalRefusal ? error.message : String(error));
      return;
    }
    setHeld(next);
    setDuplicated(catalogOf(catalogs.data, values.destination.trim()));
    setLocalError(null);
    setUnmatched([]);
    connect.mutate(
      { body, key: next.key },
      {
        onSuccess: (r) => {
          // A durable result ends the draft: leaving the inputs filled invites a second catalog for one destination.
          setMade(r);
          setHeld(null);
          form.reset({ name: "", destination: "", syncMode: "full" });
        },
        onError: (error) => {
          const missed: string[] = [];
          for (const fe of error instanceof ApiError ? error.fieldErrors : []) {
            const field = connectFieldOf(fe.field);
            if (field) form.setError(field, { type: "server", message: fe.message });
            else missed.push(`${fe.field}: ${fe.message}`);
          }
          setUnmatched(missed);
        },
      },
    );
  });

  const helper = (id: string, error: string | undefined, help: string) => (
    <FormHelperText>
      <HelperText id={`${id}-help`}>
        <HelperTextItem variant={error ? "error" : "default"}>
          <Prose text={error ?? help} />
        </HelperTextItem>
      </HelperText>
    </FormHelperText>
  );

  return (
    <Modal variant={ModalVariant.medium} isOpen={open} onClose={onClose} aria-labelledby="connect-title" aria-describedby="connect-body">
      <ModalHeader title="Connect an existing archive" labelId="connect-title" />
      <ModalBody id="connect-body">
        <Content component="p">{CONNECT_SENTENCE}</Content>
        {made ? (
          <Alert
            isInline
            variant={duplicated ? "warning" : made.replayed ? "info" : "success"}
            title={made.replayed ? "Already connected" : duplicated ? "Connected, as a duplicate that will not sync" : "Connected"}
            className="lw-gap-top"
          >
            {made.replayed
              ? "That submission had already created this catalog, so the API answered with the one it made the first time rather than creating a second. "
              : ""}
            <Link to="/$ns/catalog/$name" params={{ ns, name: made.item.name }} onClick={onClose}>
              {made.item.name}
            </Link>{" "}
            exists now. Syncing is the controller's; this page does not wait for it.
            {duplicated && (
              <p>
                Catalog {duplicated} already indexes this destination, so this one runs no sync and publishes no view. Use {duplicated}, or
                delete one of the two.
              </p>
            )}
          </Alert>
        ) : (
          <Form id="connect-form" onSubmit={(e) => void submit(e)} className="lw-gap-top" noValidate>
            <FormGroup label="Catalog name" fieldId="catalog-name" isRequired>
              <TextInput
                id="catalog-name"
                {...form.register("name")}
                validated={errors.name ? "error" : "default"}
                aria-describedby="catalog-name-help"
              />
              {helper("catalog-name", errors.name?.message, "Protection and retention policies reference a catalog by this name.")}
            </FormGroup>
            <FormGroup label="Destination" fieldId="catalog-destination" isRequired>
              <Controller
                control={form.control}
                name="destination"
                render={({ field }) =>
                  list !== null && list.length > 0 ? (
                    <FormSelect
                      id="catalog-destination"
                      value={field.value}
                      onChange={(_e, v) => field.onChange(v)}
                      onBlur={field.onBlur}
                      validated={errors.destination ? "error" : "default"}
                      aria-describedby="catalog-destination-help"
                    >
                      <FormSelectOption value="" label="Choose a saved destination" isPlaceholder />
                      {list.map((d) => (
                        <FormSelectOption key={d.name} value={d.name} label={`${d.name}: ${d.canonicalUrl}${d.default ? " (default)" : ""}`} />
                      ))}
                    </FormSelect>
                  ) : (
                    <TextInput
                      id="catalog-destination"
                      value={field.value}
                      onChange={(_e, v) => field.onChange(v)}
                      onBlur={field.onBlur}
                      validated={errors.destination ? "error" : "default"}
                      aria-describedby="catalog-destination-help"
                    />
                  )
                }
              />
              {helper(
                "catalog-destination",
                errors.destination?.message,
                list !== null && list.length === 0
                  ? "No saved destination exists in this namespace yet: create one on Destinations first; it names the bucket the archive is in."
                  : choices.isError
                    ? "The saved destinations could not be read, so type the destination's name. Its credential is the one the sync uses; a read-only one is enough."
                    : "The saved destination whose bucket holds the archive. Its credential is the one the sync uses; a read-only one is enough.",
              )}
              {holder && (
                <Alert
                  isInline
                  isPlain
                  variant="warning"
                  className="lw-gap-top"
                  title={`${chosen.trim()} already has catalog ${holder}`}
                >
                  One destination has one catalog per namespace: a second one runs no sync and publishes no view. Use {holder}, or delete it
                  first.
                </Alert>
              )}
              {list !== null && list.length === 0 && (
                <a href={`#/${ns}/destinations`} className="lw-small">
                  Open Destinations
                </a>
              )}
            </FormGroup>
            <FormGroup label="Sync mode" fieldId="catalog-mode" role="radiogroup" isStack>
              <Controller
                control={form.control}
                name="syncMode"
                render={({ field }) => (
                  <>
                    <Radio
                      id="catalog-mode-full"
                      name="syncMode"
                      label="Full: rescan every catalog record"
                      description="What connecting an existing archive wants."
                      isChecked={field.value === "full"}
                      onChange={() => field.onChange("full")}
                    />
                    <Radio
                      id="catalog-mode-index"
                      name="syncMode"
                      label="Index: read the day shards, newest first"
                      description="Reads only shards a Logweir installation wrote."
                      isChecked={field.value === "index"}
                      onChange={() => field.onChange("index")}
                    />
                  </>
                )}
              />
              {helper("catalog-mode", errors.syncMode?.message, CATALOG_MODE_HELP)}
            </FormGroup>
          </Form>
        )}
        {localError && (
          <Revealed when={localError}>
            <Alert isInline variant="danger" title="Nothing was sent" className="lw-gap-top">
              {localError}
            </Alert>
          </Revealed>
        )}
        {connect.isPending && (
          <Content component="p" className="lw-subtle lw-gap-top" role="status">
            Sent, waiting for the API. Submitting again is disabled until it answers, so one click makes one request.
          </Content>
        )}
        {connect.isError && !made && (
          <Revealed when={connect.error}>
            <Alert
              isInline
              variant={outcomeOf(connect.error) === "unknown" ? "warning" : "danger"}
              title={outcomeOf(connect.error) === "unknown" ? "The outcome is unknown" : "The catalog was not created"}
              className="lw-gap-top"
            >
              <p><Prose text={connectRefusalWords(connect.error, connect.variables?.body.name ?? "")} /></p>
              {unmatched.length > 0 && (
                <ul>
                  {unmatched.map((m) => (
                    <li key={m}>
                      <Prose text={m} />
                    </li>
                  ))}
                </ul>
              )}
              {connect.error instanceof ApiError && (
                <p className="lw-subtle lw-small">
                  {connect.error.status} {connect.error.code}
                  {connect.error.requestId ? ` · request ${connect.error.requestId}` : ""}
                </p>
              )}
            </Alert>
          </Revealed>
        )}
      </ModalBody>
      <ModalFooter>
        {made ? (
          <>
            <Button variant="secondary" onClick={() => setMade(null)}>
              Connect another
            </Button>
            <Button variant="link" onClick={onClose}>
              Done
            </Button>
          </>
        ) : (
          <>
            <Button variant="primary" type="submit" form="connect-form" isLoading={connect.isPending} isDisabled={connect.isPending}>
              Connect archive
            </Button>
            <Button variant="link" onClick={onClose}>
              Cancel
            </Button>
          </>
        )}
      </ModalFooter>
    </Modal>
  );
}
