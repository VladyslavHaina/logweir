// For R5's restore wizard and restore page: a clone (PROD-11.1a; decisions/PROD-04.2-
// cutover.md §5), a scratch restore whose topics are kept for a declared time and then
// deleted by the controller's cleanup Job.
//
// * `CloneLifetime`: the field. `null` keeps scratch mode's default (the topics go when
//   the run ends); a number is `target.teardown: {ttl_seconds: N}`, 300 to 2,592,000
//   (`cloneTtlError` is the bound, `cloneTeardown` the plan's value for R5's renderer).
// * `CloneStatus`: `status.clone` from the operation view (`OperationView.clone`) with
//   the teardown it ends in, and `cloneListWords` for a Restores row.
import { Alert, Content, DescriptionList, DescriptionListDescription, DescriptionListGroup, DescriptionListTerm, FormGroup, FormHelperText, FormSelect, FormSelectOption, HelperText, HelperTextItem, Radio } from "@patternfly/react-core";
import InfoIcon from "@patternfly/react-icons/dist/esm/icons/info-circle-icon";
import type { components } from "../../api/schema";
import { type Clone } from "../../api/switchovers";
import { shellArg } from "../../lib/shell";
import { CLONE_TTL_SECONDS, cloneTtlError } from "../restores/plan";
import { StatusLabel } from "../../components/Status";
import { When, duration, relative } from "../../components/When";

type Teardown = components["schemas"]["TeardownView"];

/** The bounds `CloneTtlOutOfRange` holds a lifetime to, in seconds (the plan renderer's). */
export const CLONE_TTL = CLONE_TTL_SECONDS;

export const CLONE_CHOICES = [
  { seconds: 3600, words: "1 hour" },
  { seconds: 4 * 3600, words: "4 hours" },
  { seconds: 24 * 3600, words: "24 hours" },
  { seconds: 7 * 86_400, words: "7 days" },
  { seconds: 30 * 86_400, words: "30 days" },
] as const;

export { cloneTtlError };

/** The plan's `target.teardown` for a clone, or null to leave scratch mode's default. */
export function cloneTeardown(seconds: number | null): { ttl_seconds: number } | null {
  return seconds === null ? null : { ttl_seconds: seconds };
}

const words = (s: number) => CLONE_CHOICES.find((c) => c.seconds === s)?.words ?? duration(s);

export function CloneLifetime({
  value,
  onChange,
  target,
  disabled,
  error,
}: {
  value: number | null;
  onChange: (seconds: number | null) => void;
  target: string;
  /** Why a clone is not offered (a restore under the original names is not a scratch restore). */
  disabled?: string;
  error?: string;
}) {
  const shown = error ?? cloneTtlError(value);
  return (
    <FormGroup role="radiogroup" isStack label="When the run ends" fieldId="clone-lifetime">
      <Radio
        id="clone-lifetime-delete"
        name="clone-lifetime"
        label="Delete the restored topics at once"
        description="A rehearsal: the topics go when the run ends, pass or fail."
        isChecked={value === null}
        onChange={() => onChange(null)}
      />
      <Radio
        id="clone-lifetime-keep"
        name="clone-lifetime"
        label="Keep them as a clone for a while"
        description={disabled ?? `For inspecting the data on ${target}. A run that does not pass is still deleted at once.`}
        isChecked={value !== null}
        isDisabled={!!disabled}
        onChange={() => onChange(CLONE_CHOICES[2].seconds)}
      />
      {value !== null && !disabled && (
        <>
          <FormSelect
            id="clone-ttl"
            value={String(value)}
            onChange={(_e, v) => onChange(Number(v))}
            aria-label="Delete the clone after"
            className="lw-ttl"
            validated={shown ? "error" : "default"}
          >
            {!CLONE_CHOICES.some((c) => c.seconds === value) && <FormSelectOption value={String(value)} label={`after ${words(value)}`} />}
            {CLONE_CHOICES.map((c) => (
              <FormSelectOption key={c.seconds} value={String(c.seconds)} label={`after ${c.words}`} />
            ))}
          </FormSelect>
          <FormHelperText>
            <HelperText>
              <HelperTextItem variant={shown ? "error" : "default"}>
                {shown ?? `Signed into the plan as target.teardown: {ttl_seconds: ${value}}. The approver sees it.`}
              </HelperTextItem>
            </HelperText>
          </FormHelperText>
          <ul className="lw-facts">
            <li>
              <InfoIcon className="lw-tone-info" aria-hidden="true" />
              <span>{target} must be in the trust policy's allowed clusters for scratch restores, and not the source; the restore is refused before anything is created otherwise.</span>
            </li>
            <li>
              <InfoIcon className="lw-tone-info" aria-hidden="true" />
              <span>Only the topics this restore creates are deleted, and only while each still has the ID recorded at creation.</span>
            </li>
            <li>
              <InfoIcon className="lw-tone-info" aria-hidden="true" />
              <span>Restored records keep their x-original-offset and x-original-timestamp headers, so the clone stays verifiable.</span>
            </li>
            <li>
              <InfoIcon className="lw-tone-info" aria-hidden="true" />
              <span>A clone cannot be switched over: consumer groups stay where they are.</span>
            </li>
          </ul>
        </>
      )}
    </FormGroup>
  );
}

// ---------------------------------------------------------------- the status

/** A clone in a Restores row: "clone, deletes in 23 hours". */
export function cloneListWords(c: Clone, now = Date.now()): string {
  switch (c.phase) {
    case "Active":
      return c.expiresAt ? `clone, deletes ${relative(c.expiresAt, now)}` : "clone";
    case "Removing":
      return "clone, deleting now";
    case "Removed":
      return "clone, removed";
    case "NotKept":
      return "clone, not kept";
    case "RemovalFailed":
      return "clone, removal failed";
    default:
      return "clone";
  }
}

function PhaseLabel({ c }: { c: Clone }) {
  switch (c.phase) {
    case "Active":
      return <StatusLabel tone="info" text={c.expiresAt ? `Deletes ${relative(c.expiresAt)}` : "Kept"} title="Active" />;
    case "Removing":
      return <StatusLabel tone="progress" text="Deleting" title="Removing" />;
    case "Removed":
      return <StatusLabel tone="success" text="Removed" title="Removed" />;
    case "NotKept":
      return <StatusLabel tone="neutral" text="Not kept" title="NotKept" />;
    case "RemovalFailed":
      return <StatusLabel tone="danger" text="Removal failed" title={c.reason ?? "RemovalFailed"} />;
    default:
      return <StatusLabel tone="neutral" text="Not recorded" title={c.phase ?? "no phase"} />;
  }
}

/** `status.clone`: when the clone's topics go, and what removed them. */
export function CloneStatus({ clone: c, teardown, restoreName }: { clone: Clone; teardown?: Teardown | null; restoreName: string }) {
  return (
    <>
      <div className="lw-gap-below">
        <PhaseLabel c={c} />
      </div>
      <DescriptionList isCompact className="lw-dl" orientation={{ sm: "vertical", md: "horizontal" }}>
        <DescriptionListGroup>
          <DescriptionListTerm>Kept for</DescriptionListTerm>
          <DescriptionListDescription>{c.ttlSeconds != null ? words(c.ttlSeconds) : <span className="lw-subtle">Not recorded</span>}</DescriptionListDescription>
        </DescriptionListGroup>
        <DescriptionListGroup>
          <DescriptionListTerm>Expires</DescriptionListTerm>
          <DescriptionListDescription>
            <When value={c.expiresAt} exact />
          </DescriptionListDescription>
        </DescriptionListGroup>
        <DescriptionListGroup>
          <DescriptionListTerm>What is deleted</DescriptionListTerm>
          <DescriptionListDescription>
            Only the topics this run created, as its signed scorecard records them, on the cluster it records, and only while each topic's ID
            is the one recorded at creation. A topic recreated since is left (CloneTargetReplaced).
          </DescriptionListDescription>
        </DescriptionListGroup>
        <DescriptionListGroup>
          <DescriptionListTerm>Clean-up</DescriptionListTerm>
          <DescriptionListDescription>
            {c.phase === "Removed"
              ? "Done."
              : c.phase === "NotKept"
                ? "The run did not pass, so its topics were removed at once."
                : c.phase === "Removing"
                  ? `Running (attempt ${c.attempts ?? 1} of 3).`
                  : c.phase === "RemovalFailed"
                    ? `Stopped after ${c.attempts ?? 0} attempt(s).`
                    : "Not run yet. Deleting this restore runs it at once."}
            {c.reason && <div className="lw-reason">{c.reason}</div>}
          </DescriptionListDescription>
        </DescriptionListGroup>
        {teardown && (teardown.deleted?.length || teardown.failed?.length || teardown.attestationKey) && (
          <DescriptionListGroup>
            <DescriptionListTerm>Deleted</DescriptionListTerm>
            <DescriptionListDescription>
              {teardown.deleted?.length ? teardown.deleted.join(", ") : <span className="lw-subtle">None</span>}
              {teardown.deletedTruncated && <span className="lw-subtle"> (and more)</span>}
              {teardown.failed?.map((f) => (
                <div key={f.topic} className="lw-small">
                  Left {f.topic} <span className="lw-reason">{f.error}</span>
                </div>
              ))}
              {teardown.attestationKey && <div className="lw-subtle lw-small lw-break">Attestation {teardown.attestationKey}</div>}
            </DescriptionListDescription>
          </DescriptionListGroup>
        )}
      </DescriptionList>
      {c.phase === "RemovalFailed" && (
        <Alert isInline variant="danger" className="lw-gap-top" title="The clone's topics may still exist">
          <Content component="p">
            Check the target for topics with the clone's prefix, delete those this run created (the scorecard's target.creation and the
            teardown's failed list name them), then remove the finalizer:
          </Content>
          <code className="lw-mono lw-break">{`kubectl -n <ns> patch restore ${shellArg(restoreName, "name", "<restore>")} --type=json -p '[{"op":"remove","path":"/metadata/finalizers"}]'`}</code>
        </Alert>
      )}
    </>
  );
}
