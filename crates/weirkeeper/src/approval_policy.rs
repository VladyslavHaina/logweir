//! The installation's approval-policy document, as this controller reads it —
//! PLAT-19.2.
//!
//! The document itself, its validation and every check made over it are
//! `logweir_core::approval_policy`'s. This module is only the READ: one
//! environment variable naming one mounted file, read once at startup, the
//! same arrangement `main` uses for the archive URL and the runner image — the
//! read is in `main`, the decision is a pure function here that a test can
//! hand an absent variable, an empty one, an unreadable file or a bad
//! document without touching process state.
//!
//! # Absent is legacy, and bad is a refusal to start
//!
//! No variable (or an empty one) is an installation that configured no
//! policy: every namespace resolves to `legacy-governed-v1`, which is exactly
//! the pre-PLAT-19.2 behaviour — D0's "existing installations retain their
//! approval requirement until explicitly changed".
//!
//! A variable that names a file this process cannot read, or a document that
//! does not validate, is NOT that: the operator asked for a policy and would
//! get none. The controller therefore refuses to start, naming the file and
//! the field, rather than silently running every namespace as legacy governed
//! — which would be fail-closed for an Ordinary binding but would also quietly
//! accept v1 approvals in a namespace whose operator bound it Governed
//! precisely to require separation of duties.
//!
//! # Why a restart and not a watch
//!
//! D0: "Selecting a different policy is an explicit installation-admin rollout
//! and audit event, not a namespace operator edit." The chart renders the
//! document into an immutable, content-addressed ConfigMap whose name the
//! Deployment references, so a changed policy is a new ReplicaSet — a rollout,
//! visible in `kubectl rollout history` — and a running controller is always
//! running the document its pod template names.

use std::path::Path;

use logweir_core::approval_policy::ApprovalPolicySet;

/// The environment variable naming the mounted approval-policy document.
pub const APPROVAL_POLICY_FILE_ENV: &str = "LOGWEIR_APPROVAL_POLICY_FILE";

/// The installation's approval policies, from the value `main` read out of
/// [`APPROVAL_POLICY_FILE_ENV`] and a reader for the file it names.
///
/// # Errors
///
/// A message naming the file and what is wrong with it; `main` refuses to
/// start on it.
pub fn configured_policy(
    variable: Result<String, std::env::VarError>,
    read: impl FnOnce(&Path) -> std::io::Result<String>,
) -> Result<ApprovalPolicySet, String> {
    let Ok(path) = variable else {
        return Ok(ApprovalPolicySet::default());
    };
    let path = path.trim();
    if path.is_empty() {
        return Ok(ApprovalPolicySet::default());
    }
    let text = read(Path::new(path)).map_err(|e| {
        format!("{APPROVAL_POLICY_FILE_ENV} names {path}, which could not be read: {e}")
    })?;
    ApprovalPolicySet::parse(&text).map_err(|e| format!("{path}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use logweir_core::approval_policy::{ApprovalMode, EffectivePolicy};

    #[test]
    fn absent_and_empty_are_the_legacy_installation() {
        for variable in [
            Err(std::env::VarError::NotPresent),
            Ok(String::new()),
            Ok("  ".into()),
        ] {
            let set = configured_policy(variable, |_| unreachable!("nothing is read"))
                .unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(set.resolve("any"), EffectivePolicy::Legacy);
        }
    }

    #[test]
    fn an_unreadable_file_or_a_bad_document_is_a_refusal_naming_the_file() {
        let unreadable = configured_policy(Ok("/nope/policy.yaml".into()), |_| {
            Err(std::io::Error::new(std::io::ErrorKind::NotFound, "gone"))
        });
        assert!(unreadable
            .err()
            .is_some_and(|e| e.contains("/nope/policy.yaml") && e.contains("gone")));
        let bad = configured_policy(Ok("/p.yaml".into()), |_| Ok("bogus: 1\n".into()));
        assert!(bad.err().is_some_and(|e| e.contains("/p.yaml")));
    }

    #[test]
    fn a_good_document_binds_its_namespaces() {
        let set = configured_policy(Ok("/p.yaml".into()), |_| {
            Ok("allowOrdinaryConfirmation: true\npolicies:\n  - name: o\n    mode: Ordinary\nnamespaces:\n  team-a: o\n".into())
        })
        .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(set.resolve("team-a").mode(), ApprovalMode::Ordinary);
    }
}
