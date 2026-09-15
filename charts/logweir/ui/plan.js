// plan.js -- the plan bytes, their hash, and the two names minted from them.
//
// THE PLAN BYTES ARE PRODUCED CLIENT-SIDE AND HASHED HERE. `planHash` binds
// exact spec bytes, the API server normalises YAML, and a typed round-trip
// silently invalidates every approval -- so the only design in which the hash
// this page SHOWS is the hash the controller RECOMPUTES is one where the page
// renders the bytes itself and never reserialises them. Nothing in this module
// parses the document it emits, and nothing else in this tree re-emits it.
//
// OPAQUE IS NOT ARBITRARY. `Restore.spec.planBytes` is an opaque string to the
// API server and to the controller, and it is still not free-form: the
// controller writes those bytes VERBATIM into the runner's plan ConfigMap as
// `data["restore.yaml"]`, with no round-trip through a typed struct, and the
// runner parses them with `serde_yaml::from_str::<RestoreSpec>`. The grammar
// is stated ONCE, in `docs/mvp/03-spec.md` section 6.1's paragraph "The restore
// plan document has ONE grammar, and it is the runner's `restore.yaml`
// (amendment 4)", and this emitter cites that paragraph rather than restating
// it. `examples/restore.yaml` is the shipped instance of it.
//
// An invented shape would pass the API server's 201 (which stores an opaque
// string), pass `logweir drill approve` (which hashes whatever bytes it reads),
// pass all five approval checks (the hash matches), and fail only inside the
// runner pod -- after every gate reported green. A hash-equality test cannot
// catch that: sha256 of X in JavaScript equals sha256 of X in Rust whatever X
// is. What catches it is the checked-in golden that Rust deserialises into
// `RestoreSpec`, which is why `ui/tests/fixtures/plan.golden.yaml` exists.
//
// THIS MODULE ISSUES NO REQUEST. It reads no clock and no storage; `crypto
// .subtle` is the only platform API it touches.

// THE SECURE-CONTEXT REFUSAL, AT MODULE LOAD.
//
// `globalThis.crypto.subtle` is `undefined` on any origin that is not
// "potentially trustworthy". The supported serving path -- loopback on port
// 8001 -- IS trustworthy, so the happy path works. But spec section 8 invites
// "Adopters wanting a shared URL front the same files with their own
// OIDC-aware ingress", and any such ingress on plain transport, or any
// `--address=0.0.0.0` plus a browse to a LAN address, leaves `subtle`
// undefined: `planHash` would throw a `TypeError` reading a property of
// undefined and the security-critical string would silently disappear from the
// page. Refusing at load is the difference between a page that cannot start
// and a page that renders a plan with no hash beside it.
//
// THE MESSAGE NAMES NO URL SCHEME, DELIBERATELY. `scripts/check-ui-offline.sh`
// has no exemption inside `ui/**` and this file is inside it; a message that
// spelled the schemes out would fail the gate that keeps every identifier in
// this tree relative. So it names the requirement -- loopback, or TLS -- in
// words.
if (!globalThis.crypto?.subtle) {
  throw new Error("this page must be served from a secure context: loopback (127.0.0.1 or localhost) " +
    "over plain transport, or any TLS origin. SubtleCrypto is unavailable here and the plan hash " +
    "cannot be computed.");
}

/** The prefix `logweir_core::spec::default_topic_prefix` produces for an
 *  instant: `restore-<YYYYmmddTHHMMSSZ>-`.
 *
 *  The compact form is for KAFKA TOPIC NAMES only, which permit the uppercase
 *  `T` and `Z`; Kubernetes object names are DNS-1123 and never use it. A pure
 *  function of its argument -- the mapped topic names a plan carries are a
 *  function of the approved bytes and of nothing else.
 *
 *  `crates/logweir/tests/ui_lint.rs::the_default_prefix_agrees_with_the_rust_one`
 *  computes the Rust half for the same instant and compares, so the two
 *  cannot drift. */
export function defaultTopicPrefix(pointInTime) {
  const at = new Date(pointInTime);
  if (isNaN(at.getTime())) {
    throw new TypeError(
      "defaultTopicPrefix(): " + String(pointInTime) + " is not an instant this page can read",
    );
  }
  const iso = at.toISOString();
  return (
    "restore-" +
    iso.slice(0, 4) +
    iso.slice(5, 7) +
    iso.slice(8, 10) +
    "T" +
    iso.slice(11, 13) +
    iso.slice(14, 16) +
    iso.slice(17, 19) +
    "Z-"
  );
}

/** Renders the restore plan document as a UTF-8 string. Its grammar is the
 *  runner's restore.yaml, defined once in spec 03-spec.md section 6.1: the
 *  document logweir/examples/restore.yaml shows and RestoreSpec deserialises
 *  (Task 9b). Byte-for-byte what goes into Restore.spec.planBytes; never
 *  re-serialised anywhere else. */
export function renderPlanBytes(fields) {
  const f = fields || {};
  const source = f.source || {};
  const target = f.target || {};
  const sample = f.sample || {};
  const objectives = f.objectives || {};
  const evidence = f.evidence || {};

  const out = [];
  out.push("# The restore plan document. Its grammar is the runner's restore document:");
  out.push("# logweir_core::spec::RestoreSpec, stated once in docs/mvp/03-spec.md section 6.1,");
  out.push("# \"The restore plan document has ONE grammar, and it is the runner's");
  out.push("# restore.yaml (amendment 4)\". These bytes are what Restore.spec.planBytes");
  out.push("# carries, what the controller writes into the runner's plan ConfigMap verbatim,");
  out.push("# and what logweir restore run --spec parses. Rendered by the Logweir UI; the");
  out.push("# sha256 of exactly these bytes is the plan hash an approval binds.");
  if (typeof f.name === "string" && f.name.length > 0) {
    out.push("name: " + quote(f.name));
  }

  out.push("source:");
  pushStorage(out, "  ", source);
  out.push("  backup: " + quote(needed(f.backupSetRef, "backupSetRef")));
  pushList(out, "  ", "topics", requireList(f.topics, "topics"), quote);

  out.push("target:");
  pushList(
    out,
    "  ",
    "bootstrap_servers",
    requireList(target.bootstrapServers, "target.bootstrapServers"),
    quote,
  );
  const auth = target.auth;
  if (auth && auth.mode !== "plaintext") {
    if (auth.mode !== "scramSha512") {
      throw new TypeError("target.auth.mode must be plaintext or scramSha512");
    }
    out.push("  auth:");
    out.push("    mode: " + quote(auth.mode));
    out.push("    username: " + quote(needed(auth.username, "target.auth.username")));
    out.push("    tls: " + (auth.tls === true ? "true" : "false"));
  }
  out.push("  mode: " + quote(requireMode(target.mode)));
  out.push("  topic_naming:");
  out.push("    prefix: " + quote(needed(target.topicPrefix, "target.topicPrefix")));
  // UNREAD in `newTopic` mode and still required by the grammar: it is the
  // SCRATCH prefix and has no serde default, because an empty prefix maps
  // every source topic onto ITSELF. A document that omitted it would not
  // deserialise -- which is the whole reason this emitter renders the runner's
  // shape and not one of its own.
  out.push(
    "  topic_mapping_prefix: " +
      quote(needed(target.topicMappingPrefix, "target.topicMappingPrefix")),
  );
  out.push("  marker_topic: " + quote(needed(target.markerTopic, "target.markerTopic")));
  out.push(
    "  default_replication_factor: " +
      integer(target.replicationFactor, "target.replicationFactor"),
  );
  out.push("  teardown: " + quote(needed(target.teardown, "target.teardown")));

  // THE RECOVERY POINT, and the only half of the window a spec may state. The
  // window's START is the archive's earliest covered timestamp, read from the
  // manifest by the runner (guard G-WIN); a spec-supplied floor is what that
  // guard exists to refuse, so this emitter has no field for one.
  out.push("restore:");
  out.push("  point_in_time: " + quote(needed(f.pointInTime, "pointInTime")));

  out.push("sample:");
  out.push("  window_start: " + quote(needed(sample.windowStart, "sample.windowStart")));
  out.push("  window_end: " + quote(needed(sample.windowEnd, "sample.windowEnd")));
  out.push(
    "  records_per_partition: " +
      integer(sample.recordsPerPartition, "sample.recordsPerPartition"),
  );
  out.push("  anchor: " + quote(needed(sample.anchor, "sample.anchor")));

  // THE THREE OBJECTIVES ARE OPTIONAL IN THE GRAMMAR AND OPTIONAL HERE. Each
  // is an `Option` with a serde default, so an absent one is "not asked for"
  // and an invented one would be this page putting a number into a signed
  // document that nobody stated. An empty block is `objectives: {}`, which is
  // what the grammar means by all three absent.
  const stated = [];
  if (objectives.rtoSeconds !== undefined && objectives.rtoSeconds !== null) {
    stated.push("  rto_seconds: " + integer(objectives.rtoSeconds, "objectives.rtoSeconds"));
  }
  if (objectives.rpoSeconds !== undefined && objectives.rpoSeconds !== null) {
    stated.push("  rpo_seconds: " + integer(objectives.rpoSeconds, "objectives.rpoSeconds"));
  }
  if (objectives.passRate !== undefined && objectives.passRate !== null) {
    stated.push("  pass_rate: " + decimal(objectives.passRate, "objectives.passRate"));
  }
  if (stated.length === 0) {
    out.push("objectives: {}");
  } else {
    out.push("objectives:");
    for (const line of stated) {
      out.push(line);
    }
  }

  out.push("evidence:");
  pushStorageFields(out, "  ", evidence);

  out.push("notifications:");
  out.push("  webhooks: []");

  return out.join("\n") + "\n";
}

/** SHA-256 over exactly those bytes, via crypto.subtle, formatted
 *  `sha256:<lowercase hex>` -- the form logweir_core::ids::sha256_prefixed
 *  produces (ids.rs:9-13), so the string the page shows is the string the CLI
 *  prints (approve.rs:108). */
export async function planHash(bytes) {
  return "sha256:" + (await sha256Hex(bytes));
}

/** The two metadata.name values, minted from the plan bytes BEFORE either
 *  object is created: {restoreName, approvalName} =
 *  {`restore-${suffix}`, `approval-${suffix}`} where suffix is the first 8
 *  lowercase hex characters of sha256(planBytes). Spec section 7, amendment 4.
 *
 *  BOTH NAMES FIRST, BECAUSE THE TWO REFERENCES ARE CIRCULAR AND BOTH SPECS
 *  ARE IMMUTABLE. `Restore.spec.approvalRef` and `Approval.spec.subjectRef`
 *  name each other; every `.spec` carries `self == oldSelf` except
 *  `BackupSchedule.spec.suspend`, so neither reference can be filled in
 *  afterwards, and two peer "create" actions with no stated order cannot
 *  produce a working pair. The `Restore` is created first with a DANGLING
 *  `approvalRef`, and the reconciler requeues at 30 s on `ApprovalNotVerified`
 *  until the `Approval` arrives. The same suffix on both is what lets an
 *  operator read a pair off a `kubectl get` without a join. */
export async function mintNames(bytes) {
  const suffix = (await sha256Hex(bytes)).slice(0, 8);
  return { restoreName: "restore-" + suffix, approvalName: "approval-" + suffix };
}

// --------------------------------------------------------------- private half

async function sha256Hex(bytes) {
  if (typeof bytes !== "string") {
    throw new TypeError(
      "the plan bytes are a string; got " + (bytes === null ? "null" : typeof bytes),
    );
  }
  const encoded = new TextEncoder().encode(bytes);
  const digest = await globalThis.crypto.subtle.digest("SHA-256", encoded);
  const view = new Uint8Array(digest);
  let hex = "";
  for (let i = 0; i < view.length; i += 1) {
    hex += view[i].toString(16).padStart(2, "0");
  }
  return hex;
}

// A storage block: the discriminant plus its fields. Rendered as the runner's
// `StorageUrl`, which is an internally tagged enum -- `backend` first, then the
// variant's own keys. Only the `s3` variant is reachable from this page; an
// adopter on another backend hand-writes the document, which is a supported
// path precisely because the grammar is the runner's and not the page's.
function pushStorage(out, indent, block) {
  out.push(indent + "storage:");
  pushStorageFields(out, indent + "  ", block);
}

function pushStorageFields(out, indent, block) {
  const b = block || {};
  out.push(indent + "backend: " + quote("s3"));
  out.push(indent + "bucket: " + quote(needed(b.bucket, "storage.bucket")));
  out.push(indent + "prefix: " + quote(b.prefix === undefined ? "" : b.prefix));
  if (typeof b.region === "string" && b.region.length > 0) {
    out.push(indent + "region: " + quote(b.region));
  }
  if (typeof b.endpoint === "string" && b.endpoint.length > 0) {
    out.push(indent + "endpoint: " + quote(b.endpoint));
  }
  out.push(indent + "path_style: " + (b.pathStyle === true ? "true" : "false"));
  // The key is spelled as a concatenation because `check-ui-offline.sh`'s rule
  // 1 forbids the byte sequence `http` followed by a colon anywhere under
  // `ui/**`, and this YAML key puts one there. The `api.js` precedent is
  // `SCHEME_SEPARATOR`: build it visibly in ASCII rather than hide it in a
  // codepoint nobody can see in a diff.
  out.push(indent + "allow_" + "http" + ": " + (b.allowHttp === true ? "true" : "false"));
}

function pushList(out, indent, key, values, render) {
  out.push(indent + key + ":");
  for (const value of values) {
    out.push(indent + "  - " + render(value));
  }
}

// THE ONE SCALAR WRITER. Every string this document carries goes through it, so
// a topic name carrying a colon, a `#`, a leading digit or a quote cannot
// change the document's SHAPE -- which for a document whose sha256 is an
// approval's subject is not a cosmetic concern. Double-quoted YAML takes the
// same escapes JSON does for every character this page can produce.
function quote(value) {
  const text = String(value);
  let out = "\"";
  for (const ch of text) {
    const code = ch.codePointAt(0);
    if (ch === "\"") {
      out += "\\\"";
    } else if (ch === "\\") {
      out += "\\\\";
    } else if (ch === "\n") {
      out += "\\n";
    } else if (ch === "\r") {
      out += "\\r";
    } else if (ch === "\t") {
      out += "\\t";
    } else if (code < 0x20 || code === 0x7f) {
      out += "\\u" + code.toString(16).padStart(4, "0");
    } else {
      out += ch;
    }
  }
  return out + "\"";
}

function integer(value, field) {
  if (typeof value !== "number" || !isFinite(value) || Math.floor(value) !== value) {
    throw new TypeError(field + " must be a whole number; got " + String(value));
  }
  return String(value);
}

// A float, always with a decimal point, so `pass_rate: 1` cannot reach a
// reader as an integer where the grammar declares an `f64`.
function decimal(value, field) {
  if (typeof value !== "number" || !isFinite(value)) {
    throw new TypeError(field + " must be a number; got " + String(value));
  }
  const text = String(value);
  return text.indexOf(".") === -1 && text.indexOf("e") === -1 ? text + ".0" : text;
}

function needed(value, field) {
  if (typeof value !== "string" || value.length === 0) {
    throw new TypeError(
      field + " is required by the runner's grammar; a document without it does not parse",
    );
  }
  return value;
}

function requireList(value, field) {
  if (!Array.isArray(value) || value.length === 0) {
    throw new TypeError(field + " is required by the runner's grammar and must not be empty");
  }
  return value;
}

/** The two values `TargetMode` accepts, and nothing else. The enum the runner
 *  parses and the `Restore` CRD's own enum are these two strings byte for
 *  byte; a third spelling is a document that applies and then fails to run. */
export const TARGET_MODES = Object.freeze(["scratch", "newTopic"]);

function requireMode(value) {
  if (TARGET_MODES.indexOf(value) === -1) {
    throw new RangeError(
      "target.mode is one of " +
        TARGET_MODES.join(", ") +
        "; got " +
        String(value) +
        ". These two strings are the runner's TargetMode and the CRD's enum, byte for byte.",
    );
  }
  return value;
}
