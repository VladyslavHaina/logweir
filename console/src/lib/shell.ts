// The words the console puts into a command an operator copies. A string from the
// server or the user enters a command only when it has a known shape, and then
// single-quoted; anything else becomes a <placeholder> the operator fills in by hand.
// No shape admits a quote, a space, `$`, a backtick, `;`, `&`, `|`, `<`, `>` or a
// newline, and no word may start with "-", so a value can neither end the word nor
// become an option.

export type ShellArgKind = "name" | "storageKey" | "url" | "region" | "kafka";

const SHAPES: Record<ShellArgKind, { pattern: RegExp; max: number }> = {
  // A DNS-1123 subdomain: a Kubernetes object name, an S3 bucket name.
  name: { pattern: /^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$/, max: 253 },
  // An object-store key from the charset Logweir's own keys use.
  storageKey: { pattern: /^[A-Za-z0-9][A-Za-z0-9._/=+-]*$/, max: 1024 },
  // An http(s) endpoint: host, optional port, optional plain path.
  url: { pattern: /^https?:\/\/[A-Za-z0-9.-]+(:[0-9]{1,5})?(\/[A-Za-z0-9._~/-]*)?$/, max: 2048 },
  // A cloud region.
  region: { pattern: /^[a-z0-9][a-z0-9-]*$/, max: 64 },
  // A Kafka topic or group id, optionally `topic:partition`.
  kafka: { pattern: /^[A-Za-z0-9._][A-Za-z0-9._-]*(:[0-9]+)?$/, max: 260 },
};

/** Whether `value` has the shape `kind` admits. */
export function hasShape(value: unknown, kind: ShellArgKind): value is string {
  const shape = SHAPES[kind];
  return typeof value === "string" && value.length <= shape.max && shape.pattern.test(value);
}

/** A placeholder an operator fills in: `<words>`, from letters, digits, spaces and `._:-` only. */
export function isPlaceholder(value: unknown): value is string {
  return typeof value === "string" && /^<[A-Za-z0-9 ._:-]{1,80}>$/.test(value);
}

/** One shell argument: `value` single-quoted when it has the shape, else `placeholder`. */
export function shellArg(value: unknown, kind: ShellArgKind, placeholder = `<${kind}>`): string {
  // The shapes admit no quote, so the single quotes need no escaping.
  return hasShape(value, kind) ? `'${value}'` : isPlaceholder(placeholder) ? placeholder : `<${kind}>`;
}

/**
 * An `s3://bucket/key` argument: quoted whole when both parts have their shape; otherwise
 * the part that has none is a placeholder (a bucket the page could not learn may already be
 * one, such as `<bucket of destination primary>`), and nothing is quoted around it.
 */
export function s3Arg(bucket: unknown, key: unknown): string {
  const b = hasShape(bucket, "name") ? bucket : null;
  const k = hasShape(key, "storageKey") ? key : null;
  if (b !== null && k !== null) return `'s3://${b}/${k}'`;
  return `s3://${b ?? (isPlaceholder(bucket) ? bucket : "<bucket>")}/${k ?? "<key>"}`;
}
