//! `logweir trust export` and `logweir trust migrate-roster` — the two
//! operator commands of PLAT-19.1 (decision D3 §7.1 and §7.5).
//!
//! # Both of them are pure text transforms, and that is deliberate
//!
//! Neither dials Kubernetes. This binary has no `kube` client and must not
//! grow one for two commands whose whole job is to reshape one object: the one
//! place it talks to the API server at all is
//! [`crate::identity`], which runs INSIDE a hook pod with a projected
//! ServiceAccount token, and an operator running `trust export` on a laptop
//! has no such token. `kubectl` is already the credential, the context
//! selector and the audit trail, so both commands read an object on stdin:
//!
//! ```text
//! kubectl --context <ctx> get trustpolicy org-default -o json \
//!   | logweir trust export --policy org-default --stdin > trustpolicy.yaml
//!
//! kubectl --context <ctx> get trustroster default -o json \
//!   | logweir trust migrate-roster --stdin --name org-default --default \
//!   > trustpolicy.yaml
//! ```
//!
//! That also makes them testable without a cluster, which a command whose
//! output an operator is about to `kubectl apply` should be.
//!
//! # Export writes PUBLIC MATERIAL ONLY, and it is a rebuild rather than a copy
//!
//! [`export`] does not strip fields from the object it was handed. It reads
//! the fields it knows and writes a document made of those, so a field this
//! build has never heard of cannot be forwarded — which is the only shape in
//! which "never emits private material" survives a future schema. On top of
//! that, [`refuse_private_material`] refuses outright if any PEM in the input
//! is a private key, because an object that carries one is a cluster that has
//! a much worse problem than a failed export, and writing it to a file the
//! operator is about to commit would compound it.
//!
//! `spkiPem` is SubjectPublicKeyInfo — a public key — in both kinds. Nothing
//! in this API group has a field a private key could go in, so the refusal
//! guards against a hand-edited object rather than against a shape the CRD
//! allows.
//!
//! # Migration is IDEMPOTENT and REVIEWABLE
//!
//! Idempotent: the output is a function of the roster's bytes and the flags,
//! with no clock read and no generated name, so two runs over the same roster
//! produce byte-identical YAML. `retiredAt`, `revokedAt` and
//! `revocationEffectiveFrom` are never synthesised — a migration that invented
//! a lifecycle event would be asserting something nobody recorded.
//!
//! A roster key that is on **both** lists is REFUSED, naming the key id: CRD
//! rule G8 gives a policy key exactly one usage, so such a key cannot be
//! expressed at all and emitting it would produce a file the API server
//! rejects at `kubectl apply` time, after the operator had reviewed it. The
//! roster itself keeps working unchanged — `weirkeeper::trust::synthesize_legacy`
//! still merges the two lists in memory, which is what keeps an unmigrated
//! cluster running.
//!
//! Reviewable: the output is a plain `TrustPolicy` document with a header
//! comment naming what was translated, meant to be read and then applied by a
//! human. Nothing here applies it, and the roster is not deleted — D3 §7.5's
//! rollback path is "the old controller reads `TrustRoster/default`, which is
//! still present and unchanged".
//!
//! # It links no signer
//!
//! Neither command signs anything, and neither names `SigningKey` or
//! `sign_detached`. `scripts/check-one-signer.sh` puts `logweir` on
//! `ALLOWED_LINK` because `backup run` and `drill run` do sign, so the gate
//! cannot prove this module's abstinence for us — the module simply has no
//! reason to reach for it, and there is no key path, key flag or key file in
//! either command's arguments.

use std::io::Read as _;

use serde::Serialize;
use serde_json::Value;

use crate::exit::ExitCode;

/// The `apiVersion` both commands read and write.
pub const API_VERSION: &str = "logweir.dev/v1alpha1";
/// The kind [`export`] and [`migrate_roster`] write.
pub const TRUST_POLICY_KIND: &str = "TrustPolicy";
/// The kind [`migrate_roster`] reads.
pub const TRUST_ROSTER_KIND: &str = "TrustRoster";

/// The substring that decides. **THE CHECK IS THIS, AND NOT THE LIST BELOW.**
///
/// Review finding F5: the first version of this guard enumerated four PEM
/// spellings — PKCS#8, PKCS#1, SEC1 and the encrypted form — and an enumeration
/// is a list somebody has to keep complete. `BEGIN OPENSSH PRIVATE KEY`,
/// `BEGIN DSA PRIVATE KEY`, `BEGIN PGP PRIVATE KEY BLOCK` and
/// `BEGIN SSH2 ENCRYPTED PRIVATE KEY` all walked straight past it.
///
/// Every one of those — and every one of the original four — contains the two
/// words below, because that is what the PEM label grammar makes them contain.
/// So the check is the substring, and it can only be widened by a format that
/// stops saying "private key" at all.
pub const PRIVATE_PEM_SUBSTRING: &str = "PRIVATE KEY";

/// The spellings [`refuse_private_material`] can NAME when it refuses.
///
/// **NOT THE CHECK** — see [`PRIVATE_PEM_SUBSTRING`]. This list exists so the
/// refusal says `BEGIN OPENSSH PRIVATE KEY` rather than "a private key",
/// which is the difference between an operator knowing which file they pasted
/// and an operator guessing. A spelling missing from here is a less specific
/// message, never a missed refusal — which is the whole point of splitting the
/// two.
pub const PRIVATE_PEM_MARKERS: [&str; 8] = [
    "BEGIN PRIVATE KEY",
    "BEGIN RSA PRIVATE KEY",
    "BEGIN EC PRIVATE KEY",
    "BEGIN DSA PRIVATE KEY",
    "BEGIN ENCRYPTED PRIVATE KEY",
    "BEGIN OPENSSH PRIVATE KEY",
    "BEGIN SSH2 ENCRYPTED PRIVATE KEY",
    "BEGIN PGP PRIVATE KEY BLOCK",
];

/// Where a command reads its object from.
#[derive(Debug, Clone)]
pub enum Input {
    /// Standard input, as `kubectl get -o json` writes it.
    Stdin,
    /// A file on disk.
    File(std::path::PathBuf),
}

/// `logweir trust export` arguments.
#[derive(Debug)]
pub struct ExportArgs {
    /// The policy name the input must carry. A mismatch is a refusal, not a
    /// rename: exporting `team-b` into a file called `org-default.yaml` is how
    /// a backup restores the wrong trust.
    pub policy: String,
    /// Where the object comes from.
    pub input: Input,
}

/// `logweir trust migrate-roster` arguments.
#[derive(Debug)]
pub struct MigrateArgs {
    /// `metadata.name` of the `TrustPolicy` to emit.
    pub name: String,
    /// Whether to set `spec.default: true`.
    pub default: bool,
    /// The namespaces to name explicitly. Empty means "name none", which with
    /// `--default` is the cluster-wide fallback and without it is a policy
    /// that governs nothing until an administrator edits it — reported on
    /// stderr rather than silently.
    pub namespaces: Vec<String>,
    /// Where the roster comes from.
    pub input: Input,
}

/// What went wrong.
#[derive(Debug)]
pub enum TrustError {
    /// The input could not be read.
    Read(String),
    /// The input is not a JSON or YAML document.
    Parse(String),
    /// The input is the wrong kind, or the wrong object.
    WrongObject(String),
    /// The input carries private key material.
    PrivateMaterial(String),
    /// A roster key appears on both `approverKeys` and `signingKeys`, which
    /// no `TrustPolicy` key may express.
    UsageOverlap(String),
    /// A required field is missing or malformed.
    Field(String),
    /// The output could not be serialised.
    Render(String),
}

impl std::fmt::Display for TrustError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read(m) => write!(f, "the input could not be read: {m}"),
            Self::Parse(m) => write!(
                f,
                "the input is not a Kubernetes object in JSON or YAML: {m}. Pipe \
                 `kubectl get … -o json` into this command."
            ),
            Self::WrongObject(m) => write!(f, "{m}"),
            Self::PrivateMaterial(m) => write!(
                f,
                "REFUSING to write anything: {m}. `logweir trust export` writes public key \
                 material only, and an object carrying a private key is a disclosure to handle \
                 before it is a backup to take."
            ),
            Self::UsageOverlap(key_id) => write!(
                f,
                "roster key {key_id} is on BOTH approverKeys and signingKeys, and a TrustPolicy \
                 key declares exactly one usage (CEL rule G8): a key that both attests and \
                 authorises is a key whose holder can approve their own work. REFUSING to emit \
                 a document the API server would reject at apply time. Issue a separate keyId \
                 per usage — mint a new signing key, add its public half to the policy as \
                 EvidenceSigning, and keep this one as GovernedApproval. The roster is \
                 untouched and keeps working until you do (docs/keys.md, migration)."
            ),
            Self::Field(m) => write!(f, "{m}"),
            Self::Render(m) => write!(f, "the document could not be serialised: {m}"),
        }
    }
}

impl std::error::Error for TrustError {}

/// `logweir trust export` — write a `TrustPolicy`'s public material as a
/// reviewable, re-appliable document.
///
/// # Errors
///
/// A [`TrustError`]; every one of them exits [`ExitCode::Operational`].
pub fn export(args: &ExportArgs) -> Result<String, TrustError> {
    let raw = read(&args.input)?;
    refuse_private_material(&raw)?;
    let object = parse(&raw)?;
    expect_kind(&object, TRUST_POLICY_KIND)?;

    let name = object
        .pointer("/metadata/name")
        .and_then(Value::as_str)
        .ok_or_else(|| TrustError::Field("the object carries no metadata.name".to_string()))?;
    if name != args.policy {
        return Err(TrustError::WrongObject(format!(
            "--policy names {} and the object on the input is {name}; refusing rather than \
             renaming it, because a backup restored under the wrong name is the wrong trust",
            args.policy
        )));
    }

    // A REBUILD AND NOT A FILTER. Only the fields named here are written, so a
    // field a future schema adds cannot be forwarded by accident — see the
    // module header.
    let spec = object
        .get("spec")
        .ok_or_else(|| TrustError::Field("the object carries no spec".to_string()))?;
    let keys = spec
        .get("keys")
        .and_then(Value::as_array)
        .ok_or_else(|| TrustError::Field("spec.keys is absent or not a list".to_string()))?;
    let mut out_keys = Vec::with_capacity(keys.len());
    for key in keys {
        out_keys.push(public_key_entry(key)?);
    }

    let mut out_spec = serde_json::Map::new();
    out_spec.insert(
        "default".into(),
        Value::Bool(
            spec.get("default")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        ),
    );
    if let Some(ns) = spec.get("namespaces") {
        out_spec.insert("namespaces".into(), ns.clone());
    }
    if let Some(ids) = spec.get("allowedTargetClusterIds") {
        out_spec.insert("allowedTargetClusterIds".into(), ids.clone());
    }
    out_spec.insert("keys".into(), Value::Array(out_keys));

    let document = document(name, Value::Object(out_spec));
    let header = format!(
        "# Exported from TrustPolicy/{name} by `logweir trust export`.\n\
         # PUBLIC KEY MATERIAL ONLY — status, managedFields, resourceVersion and uid are\n\
         # deliberately absent, so this file re-applies cleanly onto any cluster.\n\
         # Re-applying it never removes a key: spec.keys is append-only (CEL rule G1).\n"
    );
    render(&header, &document)
}

/// One `spec.keys[]` entry, rebuilt from the fields this build knows.
fn public_key_entry(key: &Value) -> Result<Value, TrustError> {
    let mut out = serde_json::Map::new();
    // REQUIRED, and named individually so a missing one says which.
    for field in [
        "keyId",
        "spkiPem",
        "algorithm",
        "usages",
        "notBefore",
        "notAfter",
        "state",
    ] {
        let value = key.get(field).ok_or_else(|| {
            TrustError::Field(format!(
                "a spec.keys entry carries no {field}; this is not a TrustPolicy this build can \
                 export"
            ))
        })?;
        out.insert(field.to_string(), value.clone());
    }
    let principal = key
        .get("principal")
        .ok_or_else(|| TrustError::Field("a spec.keys entry carries no principal".to_string()))?;
    let mut out_principal = serde_json::Map::new();
    let id = principal.get("id").ok_or_else(|| {
        TrustError::Field("a spec.keys entry's principal carries no id".to_string())
    })?;
    out_principal.insert("id".into(), id.clone());
    if let Some(display) = principal.get("display") {
        out_principal.insert("display".into(), display.clone());
    }
    out.insert("principal".into(), Value::Object(out_principal));
    // OPTIONAL LIFECYCLE INSTANTS, copied only when present. A `retiredAt`
    // invented here would be a lifecycle event nobody recorded.
    for field in [
        "retiredAt",
        "revokedAt",
        "revocationReason",
        "revocationEffectiveFrom",
    ] {
        if let Some(value) = key.get(field) {
            out.insert(field.to_string(), value.clone());
        }
    }
    Ok(Value::Object(out))
}

/// `logweir trust migrate-roster` — a `TrustRoster` as a reviewable
/// `TrustPolicy` (D3 §7.5).
///
/// # Errors
///
/// A [`TrustError`].
pub fn migrate_roster(args: &MigrateArgs) -> Result<String, TrustError> {
    let raw = read(&args.input)?;
    refuse_private_material(&raw)?;
    let object = parse(&raw)?;
    expect_kind(&object, TRUST_ROSTER_KIND)?;

    let spec = object
        .get("spec")
        .ok_or_else(|| TrustError::Field("the roster carries no spec".to_string()))?;
    let approver = list(spec, "approverKeys")?;
    let signing = list(spec, "signingKeys")?;

    // ONE ENTRY PER KEY ID, ONE USAGE PER ENTRY, IN
    // `approverKeys`-THEN-`signingKeys` ORDER. A key on both lists is a
    // refusal, not a merge — see this module's header and
    // `TrustError::UsageOverlap`.
    let mut order: Vec<String> = Vec::new();
    let mut merged: std::collections::BTreeMap<String, (Value, Vec<&str>)> =
        std::collections::BTreeMap::new();
    for (usage, entries) in [("GovernedApproval", approver), ("EvidenceSigning", signing)] {
        for entry in entries {
            let key_id = entry
                .get("keyId")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    TrustError::Field(
                        "a roster entry carries no keyId; interface I17 requires one on every \
                         entry of both lists"
                            .to_string(),
                    )
                })?
                .to_string();
            match merged.get_mut(&key_id) {
                Some((_, usages)) => {
                    // ONE KEY, ONE USAGE — CRD rule G8, and this is where an
                    // operator finds out. `spec.keys` is an associative list
                    // keyed by `keyId`, so a key on BOTH roster lists cannot be
                    // emitted twice, and emitting it once with both usages is a
                    // document the API server now refuses (D3 §7.3: the usage
                    // separation is ENFORCED for policy-backed namespaces).
                    //
                    // REFUSING HERE RATHER THAN AT `kubectl apply` is the whole
                    // value: the operator learns which key ids overlap and what
                    // to do about them while reading a migration plan, not from
                    // an admission error against a file they had already
                    // reviewed. The roster keeps working unchanged in the
                    // meantime — `weirkeeper::trust::synthesize_legacy` still
                    // merges the two lists in memory, which is what keeps an
                    // unmigrated cluster running and is exactly the
                    // `selfAttestedRisk` LABEL §7.3 preserves for legacy
                    // namespaces.
                    if !usages.contains(&usage) {
                        return Err(TrustError::UsageOverlap(key_id));
                    }
                }
                None => {
                    order.push(key_id.clone());
                    merged.insert(key_id, (entry.clone(), vec![usage]));
                }
            }
        }
    }

    let mut keys = Vec::with_capacity(order.len());
    for key_id in &order {
        let (entry, usages) = &merged[key_id];
        let spki_pem = entry
            .get("spkiPem")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                TrustError::Field(format!(
                    "roster entry keyId {key_id} carries no spkiPem; interface I17 requires key \
                 material, not ids"
                ))
            })?;
        let mut out = serde_json::Map::new();
        out.insert("keyId".into(), Value::String(key_id.clone()));
        out.insert("spkiPem".into(), Value::String(spki_pem.to_string()));
        // THE ALGORITHM IS DISCOVERED FROM THE PEM, because the roster declares
        // none. A wrong guess would be caught by the controller (which checks
        // `algorithm` against the material) and by the reviewer, but guessing
        // is not what happens: the PEM's own SubjectPublicKeyInfo says which.
        out.insert(
            "algorithm".into(),
            Value::String(algorithm_of(spki_pem).to_string()),
        );
        out.insert(
            "usages".into(),
            Value::Array(
                usages
                    .iter()
                    .map(|u| Value::String((*u).to_string()))
                    .collect(),
            ),
        );
        let mut principal = serde_json::Map::new();
        principal.insert("id".into(), Value::String(format!("legacy:{key_id}")));
        if let Some(subject) = entry.get("subject").and_then(Value::as_str) {
            principal.insert("display".into(), Value::String(subject.to_string()));
        }
        out.insert("principal".into(), Value::Object(principal));
        // THE EPOCH, AND NOT A CLOCK READ. The roster has no `notBefore`, so
        // there is no value to carry verbatim, and a `notBefore` of "now" would
        // retroactively invalidate every archive this key signed. It is also
        // what keeps this command idempotent: a clock read would make two runs
        // over the same roster produce two different documents.
        out.insert(
            "notBefore".into(),
            Value::String(LEGACY_NOT_BEFORE.to_string()),
        );
        out.insert(
            "notAfter".into(),
            Value::String(
                entry
                    .get("notAfter")
                    .and_then(Value::as_str)
                    .unwrap_or(LEGACY_NOT_AFTER)
                    .to_string(),
            ),
        );
        // `state: Active` AND NOTHING ELSE. The roster records no retirement
        // and no revocation, so none is written: `retiredAt`, `revokedAt` and
        // `revocationEffectiveFrom` are write-once on the CRD, and a migration
        // that invented one would be asserting a lifecycle event nobody
        // recorded and could never take back.
        out.insert("state".into(), Value::String("Active".to_string()));
        keys.push(Value::Object(out));
    }

    let mut out_spec = serde_json::Map::new();
    out_spec.insert("default".into(), Value::Bool(args.default));
    if !args.namespaces.is_empty() {
        out_spec.insert(
            "namespaces".into(),
            Value::Array(
                args.namespaces
                    .iter()
                    .map(|n| Value::String(n.clone()))
                    .collect(),
            ),
        );
    }
    out_spec.insert(
        "allowedTargetClusterIds".into(),
        spec.get("allowedClusterIds")
            .cloned()
            .unwrap_or_else(|| Value::Array(Vec::new())),
    );
    out_spec.insert("keys".into(), Value::Array(keys));

    let document = document(&args.name, Value::Object(out_spec));
    let header = format!(
        "# Generated by `logweir trust migrate-roster` from TrustRoster/{}.\n\
         # REVIEW THIS FILE BEFORE APPLYING IT. approverKeys became GovernedApproval,\n\
         # signingKeys became EvidenceSigning, allowedClusterIds became\n\
         # allowedTargetClusterIds, and every key is Active with notBefore {} because the\n\
         # roster records no lifecycle. Each key declares EXACTLY ONE usage (CEL rule G8).\n\
         # No ConsoleConfirmation key is ever synthesised.\n\
         # The roster is NOT deleted: a rollback still reads it, unchanged, and nothing\n\
         # consults this policy for a verification or an approval until PLAT-19.1's\n\
         # verification worker lands (docs/keys.md).\n",
        object
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .unwrap_or("default"),
        LEGACY_NOT_BEFORE,
    );
    render(&header, &document)
}

/// The `notBefore` a migrated key carries. See [`migrate_roster`].
pub const LEGACY_NOT_BEFORE: &str = "1970-01-01T00:00:00Z";
/// The `notAfter` a migrated key carries when the roster entry has none.
pub const LEGACY_NOT_AFTER: &str = "9999-12-31T23:59:59Z";

/// `p256` or `ed25519`, discovered from the PEM's own SubjectPublicKeyInfo.
///
/// # Discovered, never guessed and never flagged
///
/// The roster declares no algorithm and the policy requires one, so this is
/// the one place the two shapes need a bridge. It is
/// `VerifyingKey::from_pem_str` — the VERIFYING half of the DSSE machinery,
/// which `scripts/check-one-signer.sh` deliberately does not police
/// (`VerifyingKey` "is NOT a token here and must not be added: verification is
/// permitted everywhere"). Nothing in this module names `SigningKey` or
/// `sign_detached`, takes a key path, or opens a private key.
///
/// A PEM that does not parse is reported as `p256` and is caught downstream:
/// the controller checks `algorithm` against the material and reports
/// `Loaded=False/AlgorithmMismatch`, and a human reviews this file before
/// applying it. A migration output is not the authority on what a key is.
#[must_use]
pub fn algorithm_of(pem: &str) -> &'static str {
    match logweir_evidence::keys::VerifyingKey::from_pem_str(pem) {
        Ok(logweir_evidence::keys::VerifyingKey::Ed25519(_)) => "ed25519",
        _ => "p256",
    }
}

/// The document skeleton both commands emit.
fn document(name: &str, spec: Value) -> Value {
    serde_json::json!({
        "apiVersion": API_VERSION,
        "kind": TRUST_POLICY_KIND,
        "metadata": { "name": name },
        "spec": spec,
    })
}

/// `header` then `document` as YAML.
fn render(header: &str, document: &Value) -> Result<String, TrustError> {
    let mut buf = String::from(header);
    buf.push_str("---\n");
    let mut yaml = Vec::new();
    let mut ser = serde_yaml::Serializer::new(&mut yaml);
    document
        .serialize(&mut ser)
        .map_err(|e| TrustError::Render(e.to_string()))?;
    buf.push_str(&String::from_utf8_lossy(&yaml));
    Ok(buf)
}

/// One of the roster's two key lists.
fn list<'a>(spec: &'a Value, field: &str) -> Result<&'a Vec<Value>, TrustError> {
    spec.get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| TrustError::Field(format!("spec.{field} is absent or not a list")))
}

/// Read the whole input.
fn read(input: &Input) -> Result<String, TrustError> {
    match input {
        Input::Stdin => {
            let mut buf = String::new();
            std::io::stdin()
                .read_to_string(&mut buf)
                .map_err(|e| TrustError::Read(e.to_string()))?;
            Ok(buf)
        }
        Input::File(path) => std::fs::read_to_string(path)
            .map_err(|e| TrustError::Read(format!("{}: {e}", path.display()))),
    }
}

/// JSON first, YAML second — `kubectl get -o json` and `-o yaml` both work.
fn parse(raw: &str) -> Result<Value, TrustError> {
    if let Ok(v) = serde_json::from_str::<Value>(raw) {
        return Ok(v);
    }
    serde_yaml::from_str::<Value>(raw).map_err(|e| TrustError::Parse(e.to_string()))
}

/// The object is the kind this command reads.
fn expect_kind(object: &Value, want: &str) -> Result<(), TrustError> {
    let kind = object.get("kind").and_then(Value::as_str).unwrap_or("");
    if kind == want {
        return Ok(());
    }
    Err(TrustError::WrongObject(format!(
        "the input is a {} and this command reads a {want}",
        if kind.is_empty() {
            "document with no kind"
        } else {
            kind
        }
    )))
}

/// Refuse an input carrying a private key, before anything is written.
///
/// # Errors
///
/// [`TrustError::PrivateMaterial`] naming which marker was found — and NEVER
/// quoting the surrounding bytes. An error message that echoed the key would
/// put it in the terminal scrollback, the shell history file and any CI log
/// capturing stderr, which is the disclosure this check exists to prevent.
pub fn refuse_private_material(raw: &str) -> Result<(), TrustError> {
    if !raw.contains(PRIVATE_PEM_SUBSTRING) {
        return Ok(());
    }
    // THE SUBSTRING DECIDED; the list only names what it found. A spelling
    // this build has never heard of still refuses, and says so in the general
    // form rather than passing.
    let named = PRIVATE_PEM_MARKERS.iter().find(|m| raw.contains(*m));
    Err(TrustError::PrivateMaterial(match named {
        Some(marker) => format!("the input contains a `{marker}` PEM header"),
        None => format!("the input contains the words `{PRIVATE_PEM_SUBSTRING}`"),
    }))
}

/// `logweir trust export`, as the binary runs it.
#[must_use]
pub fn run_export(args: &ExportArgs) -> ExitCode {
    match export(args) {
        Ok(document) => {
            print!("{document}");
            // ON STDERR, so `> trustpolicy.yaml` captures the document and
            // nothing else. A summary line in the file would be a comment the
            // operator has to delete before applying it.
            eprintln!("trust-export-policy={}", args.policy);
            ExitCode::Ok
        }
        Err(e) => {
            eprintln!("trust export failed: {e}");
            ExitCode::Operational
        }
    }
}

/// `logweir trust migrate-roster`, as the binary runs it.
#[must_use]
pub fn run_migrate(args: &MigrateArgs) -> ExitCode {
    match migrate_roster(args) {
        Ok(document) => {
            print!("{document}");
            eprintln!("trust-migrate-name={}", args.name);
            eprintln!("trust-migrate-default={}", args.default);
            if args.namespaces.is_empty() && !args.default {
                eprintln!(
                    "trust migrate-roster: this policy names no namespace and is not the \
                     default, so it governs NOTHING until you add spec.namespaces or \
                     --default. Nothing is applied by this command; review the file first."
                );
            }
            ExitCode::Ok
        }
        Err(e) => {
            eprintln!("trust migrate-roster failed: {e}");
            ExitCode::Operational
        }
    }
}
