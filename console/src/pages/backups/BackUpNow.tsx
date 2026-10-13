// Back up now, from the Backups list: choose a schedule, then one run with that
// schedule's CURRENT revision under one idempotency key. A double click, a
// retry after no answer, or a second confirm all resend that key and get the
// same run back; only "Back up again" mints a new one.
import { useState } from "react";
import { Link } from "@tanstack/react-router";
import {
  Alert,
  Button,
  Checkbox,
  Content,
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
} from "@patternfly/react-core";
import { useSchedules } from "../../api/queries";
import { ApiError } from "../../api/client";
import { LocalRefusal, mintKey, outcomeOf, useBackUpNow, type ManualBackupResponse } from "../../api/backups";
import type { Schedule } from "../../api/types";

/** What a 409 on this route means; its only answer is a new key. */
export function conflictWords(error: unknown): string | null {
  if (!(error instanceof ApiError) || error.status !== 409) return null;
  if (error.code === "policy_changed") {
    const now = error.policy?.currentGeneration;
    return `The schedule's policy changed after this page read it${typeof now === "number" ? ` (it is at revision g${now} now)` : ""}, so nothing was created: a run copies the revision it names. Back up again reads the schedule afresh and sends a new key.`;
  }
  if (error.code === "idempotency_conflict") {
    return "This click's request key was already used for a different request, so the API refused rather than guess which you meant. Back up again sends a new key.";
  }
  if (error.code === "state_conflict") {
    return "The name this request derives is taken by a run this console did not create; nothing was created and nothing was adopted. Back up again sends a new key, which derives a different name.";
  }
  return null;
}

/** The sentence a write with no answer gets: it may or may not exist, and resending the same key is safe. */
export const UNKNOWN_RUN_OUTCOME =
  "No answer reached this page, so whether the backup was started is unknown. Sending again is safe, and it is the right thing to do: it resends the key this click is holding, and the API answers a repeat of that key with the run the first request made rather than starting a second one.";

export function BackUpNow({ ns, open, onClose }: { ns: string; open: boolean; onClose: () => void }) {
  const schedules = useSchedules(ns);
  const run = useBackUpNow(ns);
  const [name, setName] = useState("");
  const [key, setKey] = useState<string | null>(null);
  const [confirmed, setConfirmed] = useState(false);
  const [result, setResult] = useState<ManualBackupResponse | null>(null);
  const [localError, setLocalError] = useState<string | null>(null);
  const list = schedules.data ?? [];
  const chosen: Schedule | undefined = list.find((s) => s.name === name);
  const conflict = conflictWords(run.error);
  const outcome = run.isError ? outcomeOf(run.error) : null;
  const active = chosen?.status.activeRuns ?? [];

  const again = () => {
    setKey(null);
    setResult(null);
    setConfirmed(false);
    run.reset();
    void schedules.refetch();
  };
  const send = () => {
    if (!chosen) return;
    let held = key;
    try {
      held ??= mintKey("manual");
    } catch (error) {
      setLocalError(error instanceof LocalRefusal ? error.message : String(error));
      return;
    }
    setKey(held);
    run.mutate({ schedule: chosen.name, generation: chosen.generation, key: held }, { onSuccess: (r) => setResult(r) });
  };
  const pick = (value: string) => {
    // Another schedule is another request: it never reuses the key spent on the first.
    setName(value);
    setKey(null);
    setConfirmed(false);
    run.reset();
  };

  return (
    <Modal variant={ModalVariant.small} isOpen={open} onClose={onClose} aria-labelledby="bun-list-title" aria-describedby="bun-list-body">
      <ModalHeader title="Back up now" labelId="bun-list-title" />
      <ModalBody id="bun-list-body">
        <Content component="p">
          One backup, right now, with the topics, archive and deadline of a schedule's current revision. It does not move
          the schedule: no slot is used and its next run stays as it was.
        </Content>
        <FormGroup label="Schedule" fieldId="bun-list-schedule" isRequired className="lw-gap-top">
          <FormSelect
            id="bun-list-schedule"
            value={name}
            onChange={(_e, v) => pick(v)}
            isDisabled={Boolean(result) || run.isPending}
            aria-describedby="bun-list-schedule-help"
          >
            <FormSelectOption value="" label={schedules.isPending ? "Reading the schedules" : "Choose a schedule"} isPlaceholder />
            {list.map((s) => (
              <FormSelectOption
                key={s.name}
                value={s.name}
                label={`${s.name}: ${s.topics.length} ${s.topics.length === 1 ? "topic" : "topics"} from ${s.sourceRef.name}${s.suspended ? " (suspended)" : ""}`}
              />
            ))}
          </FormSelect>
          <FormHelperText>
            <HelperText id="bun-list-schedule-help">
              <HelperTextItem variant={schedules.isError ? "error" : "default"}>
                {schedules.isError
                  ? `The schedules could not be read: ${schedules.error.message}`
                  : list.length === 0 && !schedules.isPending
                    ? "This namespace has no schedule yet. Create one on Schedules first; a manual run copies a schedule's policy."
                    : "The run copies this schedule's revision as this page read it."}
              </HelperTextItem>
            </HelperText>
          </FormHelperText>
        </FormGroup>
        {chosen?.suspended && !result && (
          <Alert isInline variant="warning" title="This schedule is suspended" className="lw-gap-top">
            A manual backup is still allowed, and it does not resume the schedule.
            <Checkbox
              id="bun-list-confirm"
              label="Back up anyway; the schedule stays suspended"
              isChecked={confirmed}
              onChange={(_e, c) => setConfirmed(c)}
              className="lw-gap-top"
            />
          </Alert>
        )}
        {active.length > 0 && !result && (
          <Alert isInline isPlain variant="info" title={`${active.length} ${active.length === 1 ? "run is" : "runs are"} in progress for this schedule`} className="lw-gap-top">
            A manual backup is neither blocked nor counted by the overlap rule, so this would read the same topics at the same time.
          </Alert>
        )}
        {result && (
          <Alert isInline variant={result.replayed ? "info" : "success"} title={result.replayed ? "Already started" : "Backup started"} className="lw-gap-top">
            {result.replayed ? "That request had already started this run, so no second run was made. " : ""}
            <Link to="/$ns/backups/$name" params={{ ns, name: result.item.name }} onClick={onClose}>
              {result.item.name}
            </Link>{" "}
            copies revision g{result.item.scheduleRef?.generation ?? "?"} of {result.item.scheduleRef?.name ?? result.item.schedule ?? "its schedule"}.
          </Alert>
        )}
        {localError && (
          <Alert isInline variant="danger" title="Nothing was sent" className="lw-gap-top">
            {localError}
          </Alert>
        )}
        {run.isError && (
          <Alert
            isInline
            variant={outcome === "unknown" ? "warning" : "danger"}
            title={outcome === "unknown" ? "The outcome is unknown" : "The backup was not started"}
            className="lw-gap-top"
          >
            <p>{outcome === "unknown" ? UNKNOWN_RUN_OUTCOME : (conflict ?? run.error.message)}</p>
            {run.error instanceof ApiError && (
              <p className="lw-subtle lw-small">
                {run.error.status} {run.error.code}
                {run.error.requestId ? ` · request ${run.error.requestId}` : ""}
              </p>
            )}
          </Alert>
        )}
      </ModalBody>
      <ModalFooter>
        {result || conflict ? (
          <>
            <Button variant="secondary" onClick={again}>
              Back up again
            </Button>
            <Button variant="link" onClick={onClose}>
              {result ? "Done" : "Cancel"}
            </Button>
          </>
        ) : (
          <>
            <Button
              variant="primary"
              onClick={send}
              isLoading={run.isPending}
              isDisabled={!chosen || run.isPending || (chosen.suspended && !confirmed)}
            >
              {outcome === "unknown" ? "Send the same request again" : "Back up now"}
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
