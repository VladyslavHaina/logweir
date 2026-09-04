# ADR 0004: Kafka client for `logweir-kafka`

## Status

Accepted.

## Context

`logweir-kafka` is the only crate in the Logweir workspace that dials a Kafka
broker. It must, in v0.1, perform exactly four kinds of read against the
target cluster:

1. `Metadata` — cluster id, topic names, partition counts.
2. `ListOffsets` — high-watermark (end) offsets per partition.
3. `DescribeConfigs` for `ConfigResource::TOPIC` — per-topic configuration
   (e.g. `cleanup.policy`), used by the drill's config-parity checks.
4. A consumer, for canary reads that feed
   `logweir_kafka::fingerprint::record_fingerprint`.

Global Constraint 16 forbids hand-rolled Kafka protocol code for anything an
existing crate already does, so the question is which existing client crate
to depend on, not whether to write one. Two realistic candidates:

- **`rdkafka`** — a mature binding over `librdkafka` (the reference C client).
  Full admin API, including `DescribeConfigs`. Pulls in a C build (cmake or
  the legacy configure/make path) and vendors/links a static `librdkafka`,
  which is why it is excluded from the pure layer (`--no-default-features`,
  Global Constraint 1) and gated behind a non-default `client` feature.
- **`rskafka`** — a pure-Rust, minimal produce/consume client with no C
  dependency, which would let the eventual musl release target (dropped in
  this ADR, see Consequences) reach `logweir-kafka` too.

The task brief pre-declares `rdkafka` as the default outcome (`rskafka` "is a
deliberately minimal produce/consume client and [UNVERIFIED] does not expose
`DescribeConfigs`") and caps the confirming spike at 2 hours (spec §12 spike
1): connect to a live KRaft broker on `localhost:9092` (the compose stack
`just e2e-up` brings up) with `rskafka@0.5` and attempt `DescribeConfigs` for
`ConfigResource::TOPIC` against `logweir.scratch`.

## Spike outcome

**The live-broker leg of the spike could not run in this environment.**
Docker's daemon here has a proxy configured that does not answer, so no
image can be pulled and `just e2e-up` cannot bring up a broker. This is an
environment limitation, not a finding about either client.

In its place, the spike was run against `rskafka`'s **source**, which settles
the question the live round-trip was meant to answer without needing a
broker at all: whether `rskafka`'s wire layer can even *encode* a
`DescribeConfigs` request. A broker's response cannot supply a request type
the client library never constructs, so this check is broker-independent —
its answer is fully determined by what shipped in the `rskafka` crate,
not by anything a server does.

`rskafka = "0.5"` was added to a throwaway crate and built (a pure-Rust
build; no C toolchain involved), then its vendored source was read from the
local cargo registry cache
(`~/.cargo/registry/src/*/rskafka-0.5.0`):

- `src/protocol/messages/` contains one file per implemented request/response
  pair: `api_versions.rs`, `create_topics.rs`, `delete_records.rs`,
  `delete_topics.rs`, `fetch.rs`, `list_offsets.rs`, `metadata.rs`,
  `produce.rs`, `sasl_msg.rs`. **There is no `describe_configs.rs`** — no
  wire message for `DescribeConfigs` (API key 32) exists anywhere in the
  crate, request or response.
- `src/protocol/api_key.rs` defines the full `ApiKey` enum, including
  `DescribeConfigs`, but this is only the wire tag used to decode API-key
  numbers seen on the wire (e.g. in error frames); it is not evidence of an
  implemented request path, and none of the `messages/*.rs` files reference
  it for a describe-configs request/response pair.
- `src/client/controller.rs` (the admin-shaped client, `Controller`) exposes
  exactly two public async methods: `create_topic` and `delete_topic`. No
  `describe_configs` method exists on any client type in the crate.
- `README.md:8` states the crate's own scope: "This crate aims to be a
  minimal Kafka implementation for simple workloads that wish to use Kafka as
  a distributed [...]" — consistent with the brief's characterization.

**Pass condition** ("`rskafka` returns per-topic config entries against a
KRaft broker") therefore **fails**, and it fails for a reason no broker
round-trip could have changed: the client has no code path that sends a
`DescribeConfigs` request in the first place.

## Decision

Take the brief's pre-declared fallback: **`rdkafka` with a vendored static
`librdkafka`**, gated behind the non-default `client` feature (`default =
["client"]` in `logweir-kafka`'s own `Cargo.toml`, but never enabled for a
workspace build that must satisfy Global Constraint 1 with
`--no-default-features`). `rdkafka_reader.rs` implements `ClusterReader` and
`TopicDeleter` over `rdkafka::consumer::BaseConsumer` and
`rdkafka::admin::AdminClient`.

The admin path (`topic_configs`, `delete_topics`) runs on a per-call
current-thread `tokio` runtime built with `Builder::new_current_thread()`.
This is deliberate, not an accident: `AdminClient<C, R = DefaultRuntime>`
resolves `DefaultRuntime` to rdkafka's `NaiveRuntime` without the `tokio`
cargo feature, whose timers are `std::thread::sleep` — `describe_configs`
and `delete_topics` would then block the calling thread for the *entire*
configured timeout instead of being driven by a real reactor and returning
as soon as the broker replies. Enabling rdkafka's `tokio` feature fixes
that, but then requires `tokio`'s `rt` + `time` features (not `rt` alone) to
build the runtime that drives it, plus `net` — `Builder::enable_all()`
cfg-gates `enable_io()` on `net`/`process`/`signal`, so omitting it leaves the
runtime with no reactor and makes `cargo test -p logweir-kafka --features
client` behave differently from a workspace build, where feature unification
would otherwise silently turn `net` back on via some other crate. This
pairing (rdkafka's `tokio` feature + tokio's `rt`+`time`+`net`) is recorded
here so a future dependency change does not "simplify" it away.

## Consequences

- **musl target dropped from v0.1.** `rdkafka` vendors and compiles
  `librdkafka` from C via cmake; this does not cross-compile to musl without
  substantially more work than this task's scope. Task 22's release matrix
  therefore does not carry a musl target for v0.1. The target becomes
  reachable again only if a future all-Rust reader lands (see below) and
  proves out on musl.
- **No second reader implementation in v0.1.** Even had the spike passed,
  the brief is explicit that v0.1 ships one reader only, to avoid
  invalidating Task 22's release matrix and this task's feature layout. That
  question does not arise here regardless, since the spike failed outright.
- **No v0.1.1 follow-up opened.** The brief's "open a v0.1.1 follow-up" step
  applies only to the pass branch (an all-Rust reader becomes viable later).
  Since `rskafka` 0.5 does not implement `DescribeConfigs` at the protocol
  level at all — not merely "unconfirmed," but wire-message code that does
  not exist — there is nothing to file a viability follow-up about unless a
  future `rskafka` release adds the message. A later task revisiting this
  should re-run the spike against whatever `rskafka` version is current at
  that time.
- **Live-broker verification of `rdkafka_reader.rs` remains outstanding.**
  This ADR's spike answers the *client-selection* question from source
  alone, but it does not exercise `RdKafkaReader::connect`, `cluster_id`,
  `list_topics`, `end_offsets`, `topic_configs`, `consume_range` or
  `delete_topics` against a real broker — that requires the compose stack
  this environment cannot start. See task-10-report.md's "unproven without a
  live broker" section for the itemised list.

## Alternatives considered

- **Hand-rolled protocol code** for `DescribeConfigs` — forbidden outright by
  Global Constraint 16, since `rdkafka` already implements it.
- **`rskafka` now, with a hand-written `DescribeConfigs` shim on top** — this
  would itself be hand-rolled protocol code for something `rdkafka` already
  provides, so it is excluded by the same constraint, not merely
  de-prioritized.
