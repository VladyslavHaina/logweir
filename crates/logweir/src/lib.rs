//! logweir — the drill orchestrator. Every module here is a LIBRARY module so
//! `crates/logweir/tests/*.rs` can link them (Tasks 15-20 depend on this).
#![forbid(unsafe_code)]
pub mod approve;
/// GC18's phase −1: `logweir backup run`, the source-side capture.
pub mod backup;
/// PLAT-15.1 / decision D3 §5: the durable recovery catalog — the signed point
/// record `logweir backup run` writes beside every receipt, and the two
/// operator subcommands that read and backfill it.
pub mod catalog;
pub mod cli;
pub mod doctor;
pub mod drill;
/// The ONE engine-binary resolution `doctor` and `drill run` both consult.
pub mod engine_bin;
pub mod exit;
/// Short-lived Kubernetes installation identity bootstrap. This remains in
/// the signer-capable runner binary; the long-lived controller never links it.
pub mod identity;
pub mod ids;
pub mod metrics;
/// Outbound notification — the sinks, their bounds, their redaction, their
/// dedup keys, and `logweir notify deliver` (D3 §3.4, PLAT-14.2). This was
/// `drill::phase7_verify`'s second half; that path still re-exports it.
pub mod notify;
/// Interface **I14**: `logweir cluster-probe`, the liveness probe the
/// `KafkaCluster` reconciler runs as a Job. It is a subcommand of THIS binary
/// and not an engine command (GC3): the engine's four reachable subcommands are
/// unchanged by it.
pub mod probe;
pub mod schema;
pub mod show;
/// ONE execution signer, and it is `pub` so the second document type that
/// signs with it — the catalog point record (D3 §5.2) — can name the handle in
/// the signature of its own testable seam rather than growing a parallel
/// signing abstraction beside it. Global Constraint 27 is a claim about
/// LINKAGE (`scripts/check-one-signer.sh`), which visibility does not widen:
/// there is still exactly one type in this workspace that holds a parsed
/// private key, and it is still the only thing that can sign.
pub mod signer;
/// PLAT-07.1: the ONE read of a projected private-CA location, shared by
/// every command that dials Kafka over TLS.
pub mod tls_ca;
pub mod verify;
