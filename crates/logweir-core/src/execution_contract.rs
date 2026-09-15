//! Pure names for the controller-to-runner Restore execution contract.
//!
//! The Kubernetes Job template is immutable after creation.  New Restore Jobs
//! therefore carry the exact public input digests in environment variables;
//! the runner compares them with the bytes it read from projected volumes
//! before it constructs any data-plane client.  Keeping the names here makes
//! the controller and runner share one wire contract without introducing a
//! dependency between their crates.

pub const VERSION: &str = "1";
pub const VERSION_ARG: &str = "--execution-contract-version";

pub const VERSION_ENV: &str = "LOGWEIR_EXECUTION_CONTRACT_VERSION";
pub const SUBJECT_API_VERSION_ENV: &str = "LOGWEIR_EXECUTION_SUBJECT_API_VERSION";
pub const SUBJECT_KIND_ENV: &str = "LOGWEIR_EXECUTION_SUBJECT_KIND";
pub const SUBJECT_NAME_ENV: &str = "LOGWEIR_EXECUTION_SUBJECT_NAME";
pub const SUBJECT_NAMESPACE_ENV: &str = "LOGWEIR_EXECUTION_SUBJECT_NAMESPACE";
pub const SUBJECT_UID_ENV: &str = "LOGWEIR_EXECUTION_SUBJECT_UID";
pub const APPROVAL_NAME_ENV: &str = "LOGWEIR_EXECUTION_APPROVAL_NAME";
pub const APPROVAL_UID_ENV: &str = "LOGWEIR_EXECUTION_APPROVAL_UID";
pub const PLAN_SHA256_ENV: &str = "LOGWEIR_EXECUTION_PLAN_SHA256";
pub const APPROVAL_SHA256_ENV: &str = "LOGWEIR_EXECUTION_APPROVAL_SHA256";
pub const APPROVAL_SIDECAR_SHA256_ENV: &str = "LOGWEIR_EXECUTION_APPROVAL_SIDECAR_SHA256";
pub const APPROVER_KEY_SHA256_ENV: &str = "LOGWEIR_EXECUTION_APPROVER_KEY_SHA256";
pub const ALLOWED_CLUSTERS_SHA256_ENV: &str = "LOGWEIR_EXECUTION_ALLOWED_CLUSTERS_SHA256";

pub const ALL_ENV: [&str; 13] = [
    VERSION_ENV,
    SUBJECT_API_VERSION_ENV,
    SUBJECT_KIND_ENV,
    SUBJECT_NAME_ENV,
    SUBJECT_NAMESPACE_ENV,
    SUBJECT_UID_ENV,
    APPROVAL_NAME_ENV,
    APPROVAL_UID_ENV,
    PLAN_SHA256_ENV,
    APPROVAL_SHA256_ENV,
    APPROVAL_SIDECAR_SHA256_ENV,
    APPROVER_KEY_SHA256_ENV,
    ALLOWED_CLUSTERS_SHA256_ENV,
];
