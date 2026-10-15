// Choosing a saved connection (for a schedule's source or a restore's target):
// every connection is offered with its role and its probe, the identity held
// is the UID, and a connection that is gone is refused, never replaced.
import { Alert, FormSelect, FormSelectOption } from "@patternfly/react-core";
import {
  preferredConnection,
  probeShortWords,
  probeState,
  resolveSelection,
  savedConnections,
  type Connection,
  type Selection,
} from "../../api/connections";
import { Field, ProbeLabels } from "./parts";

export function optionCaption(c: Connection): string {
  return `${c.name} (${c.role || "no role"}): connection probe ${probeShortWords(c.reachability)}`;
}

export function ConnectionSelect({
  id,
  label,
  connections,
  selection,
  prefer,
  onSelect,
  error,
  help,
}: {
  id: string;
  label: string;
  connections: readonly Connection[];
  selection: Partial<Selection> | null;
  prefer?: string;
  onSelect: (s: Selection) => void;
  error?: string;
  help?: string;
}) {
  const all = savedConnections(connections);
  const resolved = resolveSelection(all, selection);
  const refused = resolved.state === "recreated" || resolved.state === "missing";
  // A refusal selects nothing, so a submit cannot go to a connection nobody chose.
  const chosen = resolved.state === "selected" ? resolved.connection : refused ? null : preferredConnection(all, prefer);
  return (
    <Field id={id} label={label} error={error} help={help}>
      {refused && (
        <Alert isInline variant="danger" title={resolved.state === "recreated" ? "The connection this form had chosen was recreated" : "The connection this form had chosen is gone"}>
          {resolved.state === "recreated"
            ? `A different object now answers to the name ${resolved.name} (uid ${resolved.recreatedUid}, was ${resolved.uid}): a recreated connection is a different set of brokers reached with a different credential, so the choice is refused rather than moved onto it. Choose the connection you mean.`
            : `No connection here carries uid ${resolved.uid} (${resolved.name}). It was not replaced by another: choose a connection.`}
        </Alert>
      )}
      <FormSelect
        id={id}
        value={chosen?.uid ?? ""}
        validated={error ? "error" : "default"}
        aria-describedby={`${id}-helper`}
        onChange={(_e, uid) => {
          const c = all.find((x) => x.uid === uid);
          if (c) onSelect({ uid: c.uid, name: c.name });
        }}
      >
        {(refused || all.length === 0) && <FormSelectOption value="" label={all.length === 0 ? "No connection in this namespace" : "Choose a connection"} isPlaceholder />}
        {all.map((c) => (
          <FormSelectOption key={c.uid} value={c.uid} label={optionCaption(c)} />
        ))}
      </FormSelect>
      {chosen && <ProbeLabels state={probeState(chosen.reachability)} />}
      {resolved.state === "selected" && resolved.renamedFrom && (
        <p className="lw-subtle lw-small">
          This connection was renamed since it was chosen: it was {resolved.renamedFrom} and is now {resolved.connection.name}. It is the same object, so
          nothing about the run changes.
        </p>
      )}
      {resolved.state === "selected" && resolved.pinned && (
        <p className="lw-subtle lw-small">
          This reference named a connection by name alone. It resolved to uid {resolved.connection.uid} and is held to that uid from here on.
        </p>
      )}
    </Field>
  );
}
