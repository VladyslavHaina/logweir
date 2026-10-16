//! Workspaces: the groups inside the one namespace an install lives in
//! (PROD-19.1).
//!
//! An install is one Helm release in one namespace. A workspace (`dev`, `prd`,
//! …) is a name in the chart's `workspaces` list, which this process gets as
//! [`WORKSPACES_ENV`], and the label [`LABEL`] on every Logweir object. The
//! label is required and has no default: a label selector cannot match
//! "absent or `dev`", so an unlabelled object would run while no console
//! showed it.
//!
//! Two refusals, both terminal for the object that carries them:
//! * [`REASON_UNKNOWN`]: the label is absent, or names a workspace that is not
//!   in the list ([`Workspaces::of`]);
//! * [`REASON_MISMATCH`]: a spec reference names an object of another
//!   workspace ([`same`]).

use std::fmt;

use kube::Resource;

/// The label every Logweir object carries.
pub const LABEL: &str = "logweir.dev/workspace";

/// The environment variable that lists the install's workspaces.
pub const WORKSPACES_ENV: &str = "LOGWEIR_WORKSPACES";

/// The condition reason for an object whose label is absent or unlisted.
pub const REASON_UNKNOWN: &str = "WorkspaceUnknown";

/// The condition reason for a reference into another workspace.
pub const REASON_MISMATCH: &str = "WorkspaceMismatch";

/// The most workspaces one install may list (the chart's bound).
pub const MAX_WORKSPACES: usize = 256;

/// A workspace name: a DNS label.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Workspace(String);

impl Workspace {
    /// A workspace name, or why it is not one.
    ///
    /// # Errors
    ///
    /// A sentence naming the value.
    pub fn new(value: &str) -> Result<Self, String> {
        if is_dns_label(value) {
            Ok(Self(value.to_string()))
        } else {
            Err(format!(
                "`{value}` is not a workspace name (a DNS label: lower-case letters, digits and \
                 `-`, at most 63 characters)"
            ))
        }
    }

    /// The name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Workspace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The install's workspaces, as [`WORKSPACES_ENV`] lists them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Workspaces(Vec<Workspace>);

impl Workspaces {
    /// Parse [`WORKSPACES_ENV`]: a comma list of 1 to [`MAX_WORKSPACES`]
    /// unique DNS labels. Pure, so a test can hand it any value.
    ///
    /// # Errors
    ///
    /// Unset, empty, an entry that is not a DNS label, a duplicate, or too
    /// many: a sentence naming the variable. The process refuses to start.
    pub fn configured(raw: Result<String, std::env::VarError>) -> Result<Self, String> {
        let raw = raw.unwrap_or_default();
        let mut list: Vec<Workspace> = Vec::new();
        for entry in raw.split(',').map(str::trim) {
            if entry.is_empty() {
                continue;
            }
            let workspace =
                Workspace::new(entry).map_err(|why| format!("{WORKSPACES_ENV}: {why}"))?;
            if list.contains(&workspace) {
                return Err(format!("{WORKSPACES_ENV} lists `{entry}` twice"));
            }
            list.push(workspace);
        }
        if list.is_empty() {
            return Err(format!("{WORKSPACES_ENV} lists no workspace"));
        }
        if list.len() > MAX_WORKSPACES {
            return Err(format!(
                "{WORKSPACES_ENV} lists {} workspaces; at most {MAX_WORKSPACES}",
                list.len()
            ));
        }
        Ok(Self(list))
    }

    /// The workspaces, in the configured order.
    #[must_use]
    pub fn list(&self) -> &[Workspace] {
        &self.0
    }

    /// The workspace an object belongs to.
    ///
    /// # Errors
    ///
    /// [`Refusal::Unknown`] when the label is absent or not in the list.
    pub fn of<K: Resource>(&self, object: &K) -> Result<Workspace, Refusal> {
        let label = label_of(object);
        match label {
            Some(value) if self.0.iter().any(|w| w.as_str() == value) => {
                Ok(Workspace(value.to_string()))
            }
            _ => Err(Refusal::Unknown {
                label: label.map(str::to_string),
            }),
        }
    }
}

/// Why an object is refused for its workspace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// The label is absent (`None`) or names an unlisted workspace.
    Unknown {
        /// The label's value, when there is one.
        label: Option<String>,
    },
    /// A reference reaches into another workspace.
    Mismatch {
        /// The referent's kind.
        referent_kind: String,
        /// The referent's name.
        referent_name: String,
        /// The referent's workspace, when it has a label.
        referent_workspace: Option<String>,
        /// The referrer's kind.
        referrer_kind: String,
        /// The referrer's workspace, when it has a label.
        referrer_workspace: Option<String>,
    },
}

impl Refusal {
    /// The condition reason.
    #[must_use]
    pub const fn reason(&self) -> &'static str {
        match self {
            Refusal::Unknown { .. } => REASON_UNKNOWN,
            Refusal::Mismatch { .. } => REASON_MISMATCH,
        }
    }
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let named = |w: &Option<String>| match w {
            Some(w) => format!("workspace `{w}`"),
            None => "no workspace".to_string(),
        };
        match self {
            Refusal::Unknown { label: None } => write!(
                f,
                "the object has no `{LABEL}` label; label it with one of this install's workspaces"
            ),
            Refusal::Unknown { label: Some(label) } => write!(
                f,
                "workspace `{label}` is not one of this install's workspaces ({WORKSPACES_ENV})"
            ),
            Refusal::Mismatch {
                referent_kind,
                referent_name,
                referent_workspace,
                referrer_kind,
                referrer_workspace,
            } => write!(
                f,
                "{referent_kind} `{referent_name}` is in {}; this {referrer_kind} is in {}",
                named(referent_workspace),
                named(referrer_workspace),
            ),
        }
    }
}

/// Whether `referent` may be named by `referrer`: both carry the same
/// workspace label.
///
/// # Errors
///
/// [`Refusal::Mismatch`], naming both workspaces. Two objects with no label
/// are not in the same workspace: [`Workspaces::of`] refuses each of them
/// first, and this does not treat an absence as a match.
pub fn same<A, B>(referrer: &A, referent: &B) -> Result<(), Refusal>
where
    A: Resource<DynamicType = ()>,
    B: Resource<DynamicType = ()>,
{
    let mine = label_of(referrer);
    let theirs = label_of(referent);
    if mine.is_some() && mine == theirs {
        return Ok(());
    }
    Err(Refusal::Mismatch {
        referent_kind: B::kind(&()).into_owned(),
        referent_name: referent.meta().name.clone().unwrap_or_default(),
        referent_workspace: theirs.map(str::to_string),
        referrer_kind: A::kind(&()).into_owned(),
        referrer_workspace: mine.map(str::to_string),
    })
}

/// The object's workspace label, as written.
#[must_use]
pub fn label_of<K: Resource>(object: &K) -> Option<&str> {
    object
        .meta()
        .labels
        .as_ref()?
        .get(LABEL)
        .map(String::as_str)
}

fn is_dns_label(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
        && bytes[0] != b'-'
        && bytes[bytes.len() - 1] != b'-'
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::batch::v1::Job;
    use k8s_openapi::api::core::v1::Secret;

    fn listed(raw: &str) -> Workspaces {
        Workspaces::configured(Ok(raw.to_string())).unwrap()
    }

    /// A kind-agnostic helper, so two core kinds with `Default` stand in for
    /// the CRDs: a Job (the referrer) and a Secret (the referent).
    fn labelled<K: Resource + Default>(name: &str, workspace: Option<&str>) -> K {
        let mut object = K::default();
        object.meta_mut().name = Some(name.to_string());
        if let Some(workspace) = workspace {
            object.meta_mut().labels = Some([(LABEL.to_string(), workspace.to_string())].into());
        }
        object
    }

    fn job(workspace: Option<&str>) -> Job {
        labelled("nightly", workspace)
    }

    fn secret(workspace: Option<&str>) -> Secret {
        labelled("source", workspace)
    }

    #[test]
    fn the_list_is_ordered_trimmed_and_exact() {
        let w = listed(" dev, tst,stg ,prd");
        let names: Vec<&str> = w.list().iter().map(Workspace::as_str).collect();
        assert_eq!(names, ["dev", "tst", "stg", "prd"]);
        assert_eq!(listed("default").list().len(), 1);
    }

    #[test]
    fn a_malformed_list_refuses_to_start() {
        let unset = Workspaces::configured(Err(std::env::VarError::NotPresent)).unwrap_err();
        assert!(unset.contains(WORKSPACES_ENV), "{unset}");
        for bad in [
            "",
            " , ",
            "dev,Dev",
            "dev,dev",
            "dev,team_a",
            "-a",
            "a-",
            "dev,*",
            &"a".repeat(64),
        ] {
            let error = Workspaces::configured(Ok(bad.to_string()))
                .expect_err(&format!("`{bad}` must refuse"));
            assert!(error.contains(WORKSPACES_ENV), "{error}");
        }
        let too_many: Vec<String> = (0..=MAX_WORKSPACES).map(|i| format!("w{i}")).collect();
        assert!(Workspaces::configured(Ok(too_many.join(","))).is_err());
        let most: Vec<String> = (0..MAX_WORKSPACES).map(|i| format!("w{i}")).collect();
        assert!(Workspaces::configured(Ok(most.join(","))).is_ok());
    }

    #[test]
    fn of_admits_a_listed_label() {
        let w = listed("dev,prd");
        assert_eq!(w.of(&job(Some("prd"))).unwrap().as_str(), "prd");
    }

    #[test]
    fn of_refuses_an_absent_or_unlisted_label() {
        let w = listed("dev,prd");
        let absent = w.of(&job(None)).unwrap_err();
        assert_eq!(absent, Refusal::Unknown { label: None });
        assert_eq!(absent.reason(), "WorkspaceUnknown");
        assert!(absent.to_string().contains(LABEL), "{absent}");

        let unlisted = w.of(&job(Some("qa"))).unwrap_err();
        assert_eq!(unlisted.reason(), "WorkspaceUnknown");
        assert!(unlisted.to_string().contains("`qa`"), "{unlisted}");
        // Exact: neither a prefix nor a case variant is the workspace.
        assert!(w.of(&job(Some("pr"))).is_err());
        assert!(w.of(&job(Some("PRD"))).is_err());
    }

    #[test]
    fn same_admits_a_reference_in_the_same_workspace() {
        assert_eq!(same(&job(Some("dev")), &secret(Some("dev"))), Ok(()));
    }

    #[test]
    fn same_refuses_a_reference_into_another_workspace_naming_both() {
        let refusal = same(&job(Some("prd")), &secret(Some("dev"))).unwrap_err();
        assert_eq!(refusal.reason(), "WorkspaceMismatch");
        assert_eq!(
            refusal.to_string(),
            "Secret `source` is in workspace `dev`; this Job is in workspace `prd`"
        );
    }

    #[test]
    fn same_never_matches_an_absent_label() {
        for (mine, theirs) in [(None, None), (Some("dev"), None), (None, Some("dev"))] {
            let refusal = same(&job(mine), &secret(theirs)).unwrap_err();
            assert_eq!(refusal.reason(), "WorkspaceMismatch", "{mine:?} {theirs:?}");
        }
        let refusal = same(&job(None), &secret(Some("dev"))).unwrap_err();
        assert!(refusal.to_string().ends_with("this Job is in no workspace"));
    }
}
