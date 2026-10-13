// Back up now, from one schedule: one run with the schedule's CURRENT revision,
// under one idempotency intent. A double click, a retry after no answer, or a
// second confirm all resend that intent and get the same run back; only
// "Back up again" starts a second one.
import { useState } from "react";
import { Link } from "@tanstack/react-router";
import { Alert, Button, Checkbox, Content, Modal, ModalBody, ModalFooter, ModalHeader, ModalVariant } from "@patternfly/react-core";
import { ApiError } from "../../api/client";
import { mintIntent, useRunFromSchedule, type ManualBackupResponse, type Schedule } from "../../api/schedules";
import { ProblemError } from "../../api/schedules";
import { WriteAlert } from "./parts";

/** What a 409 on this route means, and that its only answer is a new intent. */
export function conflictWords(error: unknown): string | null {
  if (!(error instanceof ApiError) || error.status !== 409) return null;
  if (error.code === "policy_changed") {
    const now = error instanceof ProblemError ? error.policy?.currentGeneration : undefined;
    return `The schedule's policy changed after this page read it${typeof now === "number" ? ` (it is at revision g${now} now)` : ""}, so nothing was created: a run copies the revision it names. Close this, read the schedule again, then back up again.`;
  }
  if (error.code === "idempotency_conflict") {
    return "This click's request key was already used for a different request, so the API refused rather than guess which you meant. Back up again sends a new key.";
  }
  if (error.code === "state_conflict") {
    return "The name this request derives is taken by a run this console did not create; nothing was created and nothing was adopted. Back up again sends a new key, which derives a different name.";
  }
  return null;
}

export function BackUpNow({ ns, schedule, open, onClose }: { ns: string; schedule: Schedule; open: boolean; onClose: () => void }) {
  const run = useRunFromSchedule(ns);
  const [intent, setIntent] = useState(() => mintIntent("manual"));
  const [confirmed, setConfirmed] = useState(false);
  const [result, setResult] = useState<ManualBackupResponse | null>(null);
  const active = schedule.status.activeRuns ?? [];
  const conflict = conflictWords(run.error);

  const again = () => {
    setIntent(mintIntent("manual"));
    setResult(null);
    setConfirmed(false);
    run.reset();
  };
  const send = () =>
    run.mutate({ schedule: schedule.name, generation: schedule.generation, intent }, { onSuccess: (r) => setResult(r) });

  return (
    <Modal variant={ModalVariant.small} isOpen={open} onClose={onClose} aria-labelledby="bun-title" aria-describedby="bun-body">
      <ModalHeader title={`Back up ${schedule.name} now`} labelId="bun-title" />
      <ModalBody id="bun-body">
        <Content component="p">
          One backup, right now, of the topics, archive and deadline of revision g{schedule.generation ?? "?"}. It does not
          move the schedule: no slot is used and the next scheduled run stays as it was.
        </Content>
        {schedule.suspended && !result && (
          <Alert isInline variant="warning" title="This schedule is suspended">
            A manual backup is still allowed, and it does not resume the schedule.
            <Checkbox id="bun-confirm" label="Back up anyway; the schedule stays suspended" isChecked={confirmed} onChange={(_e, c) => setConfirmed(c)} className="lw-gap-top" />
          </Alert>
        )}
        {active.length > 0 && !result && (
          <Alert isInline isPlain variant="info" title={`${active.length} scheduled ${active.length === 1 ? "run is" : "runs are"} in progress`}>
            A manual backup is neither blocked nor counted by the overlap rule, so this would read the same topics at the same time.
          </Alert>
        )}
        {result && (
          <Alert isInline variant={result.replayed ? "info" : "success"} title={result.replayed ? "Already started" : "Backup started"}>
            {result.replayed ? "That request had already started this run, so no second run was made. " : ""}
            <Link to="/$ns/backups/$name" params={{ ns, name: result.item.name }} onClick={onClose}>
              {result.item.name}
            </Link>{" "}
            copies revision g{result.item.scheduleRef?.generation ?? result.schedule?.generation ?? "?"}
            {result.schedule?.suspended ? "; the schedule is still suspended" : ""}.
          </Alert>
        )}
        {run.isError && (
          <WriteAlert
            error={run.error}
            kind="create"
            subject="the backup"
            verb="started"
            onRetry={conflict ? again : send}
            retryText={conflict ? "Back up again" : "Send the same request again"}
          >
            {conflict && <p>{conflict}</p>}
          </WriteAlert>
        )}
      </ModalBody>
      <ModalFooter>
        {result ? (
          <>
            <Button variant="secondary" onClick={again}>
              Back up again
            </Button>
            <Button variant="link" onClick={onClose}>
              Done
            </Button>
          </>
        ) : (
          <>
            <Button variant="primary" onClick={send} isLoading={run.isPending} isDisabled={run.isPending || (schedule.suspended && !confirmed) || Boolean(conflict)}>
              Back up now
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
