//! logweir — the drill orchestrator. Every module here is a LIBRARY module so
//! `crates/logweir/tests/*.rs` can link them.
#![forbid(unsafe_code)]
pub mod approve;
/// GC18's phase −1: `logweir backup run`, the source-side capture.
pub mod backup;
/// The durable recovery catalog (decision D3 §5): the signed point
/// record `logweir backup run` writes beside every receipt, and the two
/// operator subcommands that read and backfill it.
pub mod catalog;
/// `logweir check run`, the ONE check runner (decision D2 §4.2).
/// Discovery, operation readiness, restore preflight, destination access and
/// evidence fetch are five plan KINDS of one subcommand, over one argv
/// surface, one frame format and one closed error vocabulary — never five
/// commands.
pub mod check;
pub mod cli;
/// The runner refuses a projected credential
/// whose binding does not name the connection it was projected for.
pub mod credential_binding;
pub mod doctor;
pub mod drill;
/// The ONE engine-binary resolution `doctor` and `drill run` both consult.
pub mod engine_bin;
/// The engine identity every scorecard and receipt signs — the
/// runner image's `/etc/logweir/engine-identity` first, the two environment
/// variables where there is no image.
pub mod engine_identity;
pub mod exit;
/// Short-lived Kubernetes installation identity bootstrap. This remains in
/// the signer-capable runner binary; the long-lived controller never links it.
pub mod identity;
pub mod ids;
pub mod metrics;
/// Interface **I14**: `logweir cluster-probe`, the liveness probe the
/// `KafkaCluster` reconciler runs as a Job. It is a subcommand of THIS binary
/// and not an engine command (GC3): the engine's four reachable subcommands are
/// unchanged by it.
pub mod probe;
/// `logweir restore cleanup`: a TTL clone's cleanup, deleting only the
/// topics its signed scorecard records, while each one's ID is unchanged
/// (PROD-11.1a).
pub mod restore_cleanup;
pub mod schema;
pub mod show;
/// ONE execution signer, `pub` so the catalog point record (D3 §5.2) can name
/// the handle in its own testable seam rather than grow a parallel signing
/// abstraction. Global Constraint 27 is about LINKAGE
/// (`scripts/check-one-signer.sh`), which visibility does not widen: one type
/// in this workspace holds a parsed private key.
pub mod signer;
/// The ONE read of a projected private-CA location, shared by
/// every command that dials Kafka over TLS.
pub mod tls_ca;
/// `logweir trust export` and `logweir trust migrate-roster` (D3 §7.1, §7.5).
/// `logweir trust migrate-roster`. Two pure text transforms over one
/// Kubernetes object each — no cluster client, no key, no signature.
pub mod trust;
pub mod verify;
