import { ClipboardCopy } from "@patternfly/react-core";

/** A long identifier (hash, key id, point id): shortened, full value copied on click. */
export function Ident({ value, label }: { value: string; label: string }) {
  return (
    <ClipboardCopy
      variant="inline-compact"
      isCode
      truncation={{ maxCharsDisplayed: 22, position: "middle" }}
      hoverTip={`Copy ${label}`}
      clickTip="Copied"
      aria-label={label}
    >
      {value}
    </ClipboardCopy>
  );
}
