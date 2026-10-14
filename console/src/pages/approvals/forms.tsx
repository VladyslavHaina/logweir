// The two forms that take an approver's own files: a strict namespace's
// countersigned sidecar, and an unbound namespace's approval.json and
// approval.sig. Each is sent as the text the CLI wrote, never trimmed, parsed or
// re-encoded. Text that is, or is named like, a private key is refused here,
// cleared from the field and never kept; nothing is sent.
//
// A draft is kept in this page's memory until the Approval exists (through an
// error, a lost answer or a visit to another page), never in browser storage.
import { useEffect, useRef, useState } from "react";
import { Controller, useForm, type Control } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import {
  ActionGroup,
  Alert,
  Button,
  ClipboardCopy,
  Content,
  Form,
  FormGroup,
  FormHelperText,
  HelperText,
  HelperTextItem,
  TextArea,
} from "@patternfly/react-core";
import {
  COUNTERSIGN_COMMAND,
  keyMaterialFields,
  LocalRefusal,
  outcomeOf,
  placeFieldErrors,
  PRIVATE_KEY_REFUSAL,
  refuseKeyMaterial,
  UNKNOWN_OUTCOME_SENTENCE,
  useCountersign,
  useRecordApprovalFiles,
  type ApprovalFiles,
  type Subject,
  type Written,
} from "../../api/approvals";
import { Refusal } from "./parts";
import { ErrorText } from "../../components/Prose";

type Field = "approvalBytes" | "sidecarBytes";

/** The rule, as the old console stated it beside both forms. */
export const KEY_RULE = `${PRIVATE_KEY_REFUSAL[0]!.toUpperCase()}${PRIVATE_KEY_REFUSAL.slice(1)}: a file named the way a key file is named, or text spelling the words that open a private-key PEM, is refused here and cleared, and nothing is sent.`;

const drafts = new Map<string, Partial<Record<Field, string>>>();

/** The draft kept for one form, or an empty one. Exported for the tests. */
export function draftOf(key: string): Partial<Record<Field, string>> {
  return { ...(drafts.get(key) ?? {}) };
}

function keepDraft(key: string, values: Partial<Record<Field, string>>) {
  const kept: Partial<Record<Field, string>> = {};
  for (const [field, text] of Object.entries(values) as [Field, string | undefined][]) {
    if (typeof text === "string" && text.length > 0 && refuseKeyMaterial("", text) === null) kept[field] = text;
  }
  drafts.set(key, kept);
}

/** Text to copy to the approver's own machine, verbatim: one line, expandable to all of it. */
export function CopyBlock({ id, text, label }: { id: string; text: string; label: string }) {
  return (
    <div className="lw-copy">
      <div className="lw-subtle lw-small" id={`${id}-label`}>
        {label}
      </div>
      <ClipboardCopy
        id={id}
        isReadOnly
        isCode
        variant={text.length > 90 ? "expansion" : "inline"}
        hoverTip={`Copy ${label.toLowerCase()}`}
        clickTip="Copied"
        textAriaLabel={label}
        toggleAriaLabel={`Show all of ${label.toLowerCase()}`}
      >
        {text}
      </ClipboardCopy>
    </div>
  );
}

/** One document field: a text area and a button that reads a chosen file into it. */
function DocumentField({
  control,
  name,
  label,
  help,
  fileLabel,
  onFile,
  disabled,
}: {
  control: Control<Record<Field, string>>;
  name: Field;
  label: string;
  help: string;
  fileLabel: string;
  onFile: (file: File) => void;
  disabled: boolean;
}) {
  const input = useRef<HTMLInputElement>(null);
  const id = `approval-${name}`;
  return (
    <Controller
      control={control}
      name={name}
      render={({ field, fieldState }) => (
        <FormGroup label={label} fieldId={id} isRequired>
          <TextArea
            id={id}
            value={field.value}
            onChange={(_e, value) => field.onChange(value)}
            onBlur={field.onBlur}
            rows={6}
            resizeOrientation="vertical"
            className="lw-mono"
            spellCheck={false}
            autoComplete="off"
            isDisabled={disabled}
            validated={fieldState.error ? "error" : "default"}
            aria-describedby={`${id}-helper`}
          />
          <input
            ref={input}
            type="file"
            hidden
            onChange={(event) => {
              const file = event.target.files?.[0];
              event.target.value = "";
              if (file) onFile(file);
            }}
          />
          <Button variant="link" isInline onClick={() => input.current?.click()} isDisabled={disabled} className="lw-choose-file">
            {fileLabel}
          </Button>
          <FormHelperText>
            <HelperText>
              <HelperTextItem variant={fieldState.error ? "error" : "default"} id={`${id}-helper`}>
                {fieldState.error?.message ?? help}
              </HelperTextItem>
            </HelperText>
          </FormHelperText>
        </FormGroup>
      )}
    />
  );
}

/** What became of the last send, in the words a person reads. */
function SendOutcome({ error, unmatched }: { error: unknown; unmatched: string[] }) {
  if (!error) return null;
  const outcome = outcomeOf(error);
  if (outcome === "unknown") {
    return (
      <Alert variant="warning" isInline title="The answer did not arrive" className="lw-gap-top" component="h3">
        {UNKNOWN_OUTCOME_SENTENCE}
      </Alert>
    );
  }
  if (outcome === "invalid") {
    return unmatched.length > 0 ? (
      <Refusal title="Not recorded: the service refused these values">
        <ul className="lw-plain-list">
          {unmatched.map((u) => (
            <li key={u}>{u}</li>
          ))}
        </ul>
      </Refusal>
    ) : null;
  }
  if (outcome === "local") return null;
  return (
    <Refusal title="Not recorded" id="approval-refused">
      <ErrorText error={error} />
    </Refusal>
  );
}

function useDocumentForm(draftKey: string, fields: Field[], messages: Record<Field, string>) {
  const schema = z.object(Object.fromEntries(fields.map((f) => [f, z.string().min(1, messages[f])])) as Record<Field, z.ZodString>);
  const kept = draftOf(draftKey);
  const form = useForm<Record<Field, string>>({
    resolver: zodResolver(schema) as never,
    defaultValues: { approvalBytes: kept.approvalBytes ?? "", sidecarBytes: kept.sidecarBytes ?? "" },
  });
  const [fileNames, setFileNames] = useState<Record<Field, string>>({ approvalBytes: "", sidecarBytes: "" });
  const [unmatched, setUnmatched] = useState<string[]>([]);
  useEffect(() => {
    const sub = form.watch((values) => keepDraft(draftKey, values as Partial<Record<Field, string>>));
    return () => sub.unsubscribe();
  }, [form, draftKey]);

  /** Clears refused key material from the field and the draft, and says why beside it. */
  const refuse = (keyed: Field[]) => {
    for (const f of keyed) {
      form.setValue(f, "");
      form.setError(f, { type: "local", message: PRIVATE_KEY_REFUSAL });
      setFileNames((n) => ({ ...n, [f]: "" }));
    }
    keepDraft(draftKey, form.getValues());
  };

  /** A chosen file: refused by its name before it is read, by its text after. */
  const choose = async (field: Field, file: File) => {
    if (refuseKeyMaterial(file.name, "") !== null) {
      refuse([field]);
      return;
    }
    const text = await file.text();
    if (refuseKeyMaterial("", text) !== null) {
      refuse([field]);
      return;
    }
    form.setValue(field, text, { shouldValidate: true });
    setFileNames((n) => ({ ...n, [field]: file.name }));
  };

  /** Places a failed send's field errors beside their fields; clears key material the page refused. */
  const placeError = (error: unknown) => {
    if (error instanceof LocalRefusal) {
      if (error.message === PRIVATE_KEY_REFUSAL) refuse(error.fields);
      else for (const f of error.fields) form.setError(f, { type: "local", message: error.message });
      setUnmatched([]);
      return;
    }
    const placed = placeFieldErrors(error, fields);
    for (const [f, message] of Object.entries(placed.fields) as [Field, string][]) form.setError(f, { type: "server", message });
    setUnmatched(placed.unmatched);
  };

  return { form, fileNames, unmatched, refuse, choose, placeError };
}

/**
 * THE STRICT APPROVER'S PANEL: the console's confirmation, exactly as stored, to
 * copy; the one command that countersigns it with the approver's personal key;
 * and a field for the sidecar that command wrote. The document is never parsed
 * here: what the approver reviews is what their own CLI prints from the bytes.
 */
export function CountersignForm({
  ns,
  subject,
  packet,
  onDone,
}: {
  ns: string;
  subject: Subject;
  packet: { approvalBytes: string; sidecarBytes: string } | null;
  onDone: (written: Written) => void;
}) {
  const draftKey = `${ns}/${subject.name}/countersign`;
  const send = useCountersign(ns, subject);
  const { form, fileNames, unmatched, refuse, choose, placeError } = useDocumentForm(draftKey, ["sidecarBytes"], {
    approvalBytes: "",
    sidecarBytes: "Paste the sidecar logweir drill countersign wrote, or choose its file.",
  });
  const submit = form.handleSubmit((values) => {
    const keyed = refuseKeyMaterial(fileNames.sidecarBytes, values.sidecarBytes);
    if (keyed !== null) {
      refuse(["sidecarBytes"]);
      return;
    }
    send.mutate(
      { text: values.sidecarBytes, fileName: fileNames.sidecarBytes },
      {
        onSuccess: (written) => {
          drafts.delete(draftKey);
          onDone(written);
        },
        onError: placeError,
      },
    );
  });
  return (
    <Form onSubmit={(e) => void submit(e)} noValidate id="countersign-form" aria-busy={send.isPending}>
      <Content component="ol" className="lw-steps">
        <li>
          <strong>Copy the console&apos;s confirmation</strong> to the machine that holds your approver key: the document and the console&apos;s signature, exactly as stored.
          {packet ? (
            <div className="lw-gap-top lw-approval-docs">
              <CopyBlock id="confirmation-document" text={packet.approvalBytes} label="The document (approvalBytes)" />
              <CopyBlock id="confirmation-sidecar" text={packet.sidecarBytes} label="The console's signature (sidecarBytes)" />
            </div>
          ) : (
            <Content component="p" className="lw-subtle" id="packet-unreadable">
              This sign-in may not read the confirmation&apos;s documents (approvalPacketRead). Ask for them, or read them with kubectl.
            </Content>
          )}
        </li>
        <li>
          <strong>Countersign it there</strong> with your personal key:
          <div className="lw-gap-top">
            <CopyBlock id="countersign-command" text={COUNTERSIGN_COMMAND} label="The command" />
          </div>
        </li>
        <li>
          <strong>Paste the sidecar it wrote</strong>, or choose the file.
        </li>
      </Content>
      <DocumentField
        control={form.control}
        name="sidecarBytes"
        label="approval.sig (countersigned)"
        help="The sidecar logweir drill countersign wrote: the console's signature and yours, over the same bytes."
        fileLabel="Choose approval.sig"
        onFile={(file) => void choose("sidecarBytes", file)}
        disabled={send.isPending}
      />
      <p className="lw-subtle lw-small" id="key-rule">
        {KEY_RULE}
      </p>
      <ActionGroup>
        <Button type="submit" variant="primary" isLoading={send.isPending} isDisabled={send.isPending}>
          Countersign
        </Button>
      </ActionGroup>
      <SendOutcome error={send.error} unmatched={unmatched} />
    </Form>
  );
}

/** An unbound namespace's v1 approval: the two files `logweir drill approve` wrote. */
export function ApprovalFilesForm({ ns, subject, onDone }: { ns: string; subject: Subject; onDone: (written: Written) => void }) {
  const draftKey = `${ns}/${subject.name}/files`;
  const send = useRecordApprovalFiles(ns, subject);
  const { form, fileNames, unmatched, refuse, choose, placeError } = useDocumentForm(draftKey, ["approvalBytes", "sidecarBytes"], {
    approvalBytes: "Choose approval.json or paste its text.",
    sidecarBytes: "Choose approval.sig or paste its text.",
  });
  const submit = form.handleSubmit((values) => {
    const files: ApprovalFiles = {
      approvalBytes: values.approvalBytes,
      sidecarBytes: values.sidecarBytes,
      approvalFileName: fileNames.approvalBytes,
      sidecarFileName: fileNames.sidecarBytes,
    };
    const keyed = keyMaterialFields(files);
    if (keyed.length > 0) {
      refuse(keyed);
      return;
    }
    send.mutate(files, {
      onSuccess: (written) => {
        drafts.delete(draftKey);
        onDone(written);
      },
      onError: placeError,
    });
  });
  return (
    <Form onSubmit={(e) => void submit(e)} noValidate id="approval-files-form" aria-busy={send.isPending}>
      <Content component="p">
        Paste or choose the two files <code className="lw-mono">logweir drill approve</code> wrote over this Restore&apos;s plan on the approver&apos;s own machine. They are sent exactly as they are here: the controller hashes the bytes the approver signed.
      </Content>
      <DocumentField
        control={form.control}
        name="approvalBytes"
        label="approval.json"
        help="Choose the file, or paste its text. It is sent exactly as it is here."
        fileLabel="Choose approval.json"
        onFile={(file) => void choose("approvalBytes", file)}
        disabled={send.isPending}
      />
      <DocumentField
        control={form.control}
        name="sidecarBytes"
        label="approval.sig"
        help="The signature sidecar the same command wrote beside it."
        fileLabel="Choose approval.sig"
        onFile={(file) => void choose("sidecarBytes", file)}
        disabled={send.isPending}
      />
      <p className="lw-subtle lw-small" id="key-rule">
        {KEY_RULE}
      </p>
      <ActionGroup>
        <Button type="submit" variant="primary" isLoading={send.isPending} isDisabled={send.isPending}>
          Record the approval
        </Button>
      </ActionGroup>
      <SendOutcome error={send.error} unmatched={unmatched} />
    </Form>
  );
}
