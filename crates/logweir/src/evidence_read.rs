//! **A Job that reads a run's evidence reads it with the evidence
//! destination's read grant, and a store that refuses a read is never a
//! verdict on the evidence** (theme A's PoC defect D1).
//!
//! The `Switchover` Job and a TTL clone's cleanup Job read a restore's signed
//! evidence (the scorecard, its sidecar, the positions mapping, a rollback's
//! record) and write beside it (the switchover record, the cleanup
//! attestation). On the PoC the evidence WRITER may only `PutObject` under
//! `logweir/`, the least privilege the docs advise. Both Jobs read with it,
//! were refused 403, and the switchover named the refusal
//! `MappingDigestMismatch`: tampered evidence.
//!
//! # The two handles
//!
//! The controller projects the evidence destination's `evidenceRead` grant
//! into these Jobs' ARCHIVE slot (`AWS_*`, `LOGWEIR_ARCHIVE_CREDENTIALS`,
//! the archive binding pair), which they use for nothing else, and the
//! `evidenceWrite` grant into the EVIDENCE slot (`LOGWEIR_EVIDENCE_*`,
//! `archive` when the two grants are one). It names the read grant with
//! [`READ_GRANT_ARG`]. [`stores`] builds the read handle from the first slot
//! and the write handle from the second, both at the plan's evidence location.
//! Outside the store contract (a hand-run or an inline restore) one ambient
//! credential serves both, as before.
//!
//! # A failed read, in words
//!
//! [`unreadable`] is the one place a failed read becomes a sentence. Absent
//! (`NotFound`) and over the cap (`TooLarge`) are facts about the object, and
//! the evidence code judges them as before. Every other store error is
//! [`EVIDENCE_UNREADABLE`]: an operational failure that names the key, the
//! grant and the store's class (`AccessDenied`, `Timeout`, …), never the
//! backend's text, and never `MappingDigestMismatch` or `NoPositionsMapping`.

use logweir_core::check_contract::CheckCode;
use logweir_engine_oso::storage::{Store, StoreError};
use std::cell::RefCell;

/// The word of a whole run that could not read the evidence it was given.
pub const EVIDENCE_UNREADABLE: &str = "EvidenceUnreadable";

/// The flag the controller names the read grant with.
pub const READ_GRANT_ARG: &str = "--evidence-read-grant";

/// What a run says it read with when no controller named the grant: a
/// hand-run, whose one credential is the operator's own.
pub const AMBIENT_GRANT: &str = "this process's own store credential";

/// A read the store refused: its class and the sentence that names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unreadable {
    /// The store's class (`AccessDenied`, `Timeout`, …).
    pub code: CheckCode,
    /// `EvidenceUnreadable: could not read <key> with grant <grant>: <code>; …`.
    pub sentence: String,
}

/// The failure of a read the STORE refused, or `None` for an object that is
/// absent or over its cap (a fact the evidence code judges).
#[must_use]
pub fn unreadable(key: &str, e: &StoreError, grant: &str) -> Option<Unreadable> {
    if matches!(e, StoreError::NotFound(_) | StoreError::TooLarge { .. }) {
        return None;
    }
    let code = crate::check::store::classify(e);
    Some(Unreadable {
        code,
        sentence: format!(
            "{EVIDENCE_UNREADABLE}: could not read {key} with grant {grant}: {code}; the store \
             refused the read, so the evidence was not judged"
        ),
    })
}

/// The first read a store refused, kept for the caller that judges the run.
#[derive(Debug, Default)]
pub struct Note(RefCell<Option<Unreadable>>);

impl Note {
    /// Keep `key`'s failure when it is a store's refusal ([`unreadable`]) and
    /// nothing was kept before.
    pub fn observe(&self, key: &str, e: &StoreError, grant: &str) {
        if self.0.borrow().is_some() {
            return;
        }
        if let Some(why) = unreadable(key, e, grant) {
            *self.0.borrow_mut() = Some(why);
        }
    }

    /// The kept failure, once.
    pub fn take(&self) -> Option<Unreadable> {
        self.0.borrow_mut().take()
    }

    /// Whether a failure is kept.
    #[must_use]
    pub fn is_set(&self) -> bool {
        self.0.borrow().is_some()
    }
}

/// What one read's error becomes for the closure that reads it: for `String`
/// the words the switchover has always used (absent: `… is not in the
/// evidence store`; otherwise `… could not be read: …`), and for a closure
/// that judges a [`StoreError`] itself, the error unchanged.
pub trait ReadError: Sized {
    /// `e`, the error of reading `key`.
    fn from_store(key: &str, e: StoreError) -> Self;
}

impl ReadError for String {
    fn from_store(key: &str, e: StoreError) -> Self {
        match e {
            StoreError::NotFound(_) => format!("{key} is not in the evidence store"),
            other => format!("{key} could not be read: {other}"),
        }
    }
}

impl ReadError for StoreError {
    fn from_store(_: &str, e: StoreError) -> Self {
        e
    }
}

/// The run's read handle: the store, the grant it reads with, and the first
/// read the store refused. It dereferences to the store, so the evidence code
/// reads through it unchanged and [`EvidenceReader::noted`] keeps the refusal
/// beside it.
pub struct EvidenceReader {
    store: Store,
    grant: String,
    note: Note,
}

impl EvidenceReader {
    /// `store`, read with `grant` ([`AMBIENT_GRANT`] when none was named).
    #[must_use]
    pub fn new(store: Store, grant: Option<&str>) -> Self {
        Self {
            store,
            grant: grant.unwrap_or(AMBIENT_GRANT).to_string(),
            note: Note::default(),
        }
    }

    /// The grant it reads with, as messages name it.
    #[must_use]
    pub fn grant(&self) -> &str {
        &self.grant
    }

    /// One read's result, with a store's refusal kept ([`Note::observe`]).
    ///
    /// # Errors
    ///
    /// `read`'s error, as the closure reading it takes it ([`ReadError`]).
    pub fn noted<E: ReadError>(
        &self,
        key: &str,
        read: Result<Vec<u8>, StoreError>,
    ) -> Result<Vec<u8>, E> {
        read.map_err(|e| {
            self.note.observe(key, &e, &self.grant);
            E::from_store(key, e)
        })
    }

    /// The first read the store refused, once.
    pub fn unreadable(&self) -> Option<Unreadable> {
        self.note.take()
    }
}

/// A check's read-only evidence handle that keeps the first read the store
/// refused, for the `switchoverReadiness` row that reads through it.
pub struct NotingHandle<'g> {
    handle: Box<dyn crate::check::store::ObjectAccess>,
    grant: &'g str,
    note: Note,
}

impl<'g> NotingHandle<'g> {
    /// `handle`, read with `grant`.
    #[must_use]
    pub fn new(handle: Box<dyn crate::check::store::ObjectAccess>, grant: &'g str) -> Self {
        Self {
            handle,
            grant,
            note: Note::default(),
        }
    }

    /// [`crate::check::store::ObjectAccess::get`], with a store's refusal
    /// kept.
    ///
    /// # Errors
    ///
    /// The store's.
    pub fn get(&self, key: &str, max_bytes: u64) -> Result<Vec<u8>, StoreError> {
        self.handle.get(key, max_bytes).inspect_err(|e| {
            self.note.observe(key, e, self.grant);
        })
    }

    /// The first read the store refused, once.
    pub fn unreadable(&self) -> Option<Unreadable> {
        self.note.take()
    }
}

impl std::ops::Deref for EvidenceReader {
    type Target = Store;
    fn deref(&self) -> &Store {
        &self.store
    }
}

/// How a run that stopped before its work ends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ending {
    /// A read the store refused: an operational failure (exit 1), named.
    Unreadable(String),
    /// The evidence code's own refusal (exit 3), unchanged.
    Refused(String),
}

/// **A store's refusal outranks the verdict it caused.** `refusal` is what
/// the evidence code concluded from the bytes it was given; `unreadable` the
/// first read the store refused on the way. When the store refused a read,
/// the conclusion was drawn from bytes that never arrived (on the PoC:
/// `MappingDigestMismatch`), so the run ends [`Ending::Unreadable`]. `None`
/// for a run that did not stop.
#[must_use]
pub fn ending(refusal: Option<String>, unreadable: Option<Unreadable>) -> Option<Ending> {
    let refusal = refusal?;
    Some(match unreadable {
        Some(u) => Ending::Unreadable(u.sentence),
        None => Ending::Refused(refusal),
    })
}

/// **The read handle** at the plan's evidence location `evidence`: under the
/// store contract it is built from the archive slot (the read grant) and can
/// never put; outside it, from the ambient credential, as before. The write
/// handle is built where it always was, from the evidence slot.
///
/// # Errors
///
/// The store contract's refusal, or a store that cannot be built, in words.
pub fn read_store(
    evidence: &logweir_core::engine::StorageUrl,
    contract: bool,
) -> Result<Store, String> {
    if contract {
        let options = crate::backup::store_contract::archive_options().map_err(|e| e.0)?;
        Store::read_only_with(evidence, &options).map_err(|e| e.to_string())
    } else {
        Store::read_only_from_url(evidence).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The flattened text `object_store` gives a 403 behind this crate's key
    /// (the shape the PoC's switchover printed).
    fn denied(key: &str) -> StoreError {
        StoreError::Io(format!(
            "{key}: Error performing GET http://minio:9000/b/{key} in 3ms - Server returned \
             non-2xx status code: 403 Forbidden: <?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <Error><Code>AccessDenied</Code><Message>Access Denied.</Message></Error>"
        ))
    }

    /// **A store's refusal is named, never judged.** A 403 is
    /// `EvidenceUnreadable` naming the key, the grant and `AccessDenied`,
    /// without the backend's text; absent and over the cap are not store
    /// failures (the evidence code judges them).
    ///
    /// KILLS: `unreadable` answering for `NotFound` or `TooLarge` (an absent
    /// mapping would become an outage); the class or the grant left out.
    #[test]
    fn a_denied_read_is_unreadable_and_absent_or_oversized_are_not() {
        let key = "logweir/drills/R1.json";
        let failure = unreadable(key, &denied(key), "evidenceRead of BackupDestination t/d")
            .expect("a store refusal");
        assert_eq!(failure.code, CheckCode::AccessDenied);
        let why = failure.sentence;
        assert!(why.starts_with("EvidenceUnreadable: could not read logweir/drills/R1.json"));
        assert!(
            why.contains("with grant evidenceRead of BackupDestination t/d: AccessDenied"),
            "{why}"
        );
        assert!(!why.contains("minio:9000"), "no backend text: {why}");
        assert!(unreadable(key, &StoreError::NotFound(key.into()), "g").is_none());
        assert!(unreadable(
            key,
            &StoreError::TooLarge {
                key: key.into(),
                cap: 1,
                observed: logweir_engine_oso::storage::OverCap::Reported(2),
            },
            "g"
        )
        .is_none());
    }

    /// **A store's refusal outranks the verdict it caused**, and only a run
    /// that stopped ends at all. CONTROL: with no store refusal the evidence
    /// code's refusal stands, word for word.
    ///
    /// KILLS: the verdict kept over the store's refusal (the PoC's
    /// `MappingDigestMismatch`); a completed run turned into a failure by a
    /// kept note.
    #[test]
    fn a_store_refusal_outranks_the_verdict_it_caused() {
        let key = "logweir/drills/R1.json";
        let u = unreadable(key, &denied(key), "g").expect("a refusal");
        let verdict = "MappingDigestMismatch: logweir/drills/R1.json could not be read".to_string();
        assert_eq!(
            ending(Some(verdict.clone()), Some(u.clone())),
            Some(Ending::Unreadable(u.sentence.clone()))
        );
        assert_eq!(
            ending(Some(verdict.clone()), None),
            Some(Ending::Refused(verdict))
        );
        assert_eq!(ending(None, Some(u)), None);
        assert_eq!(ending(None, None), None);
    }

    /// The note keeps the FIRST refusal and nothing for an absence, and a
    /// read's error reaches the closure in its own type, the words unchanged
    /// for `String`.
    #[test]
    fn the_note_keeps_the_first_refusal_and_the_closure_its_error() {
        let store = Store::in_memory("logweir");
        let reader = EvidenceReader::new(store, Some("evidenceRead of BackupDestination t/d"));
        let absent: Result<Vec<u8>, String> =
            reader.noted("logweir/a", Err(StoreError::NotFound("logweir/a".into())));
        assert_eq!(
            absent.unwrap_err(),
            "logweir/a is not in the evidence store"
        );
        assert!(!reader.note.is_set(), "an absence is not a store failure");
        let first: Result<Vec<u8>, StoreError> = reader.noted("logweir/b", Err(denied("b")));
        assert!(matches!(first, Err(StoreError::Io(_))));
        let _: Result<Vec<u8>, String> = reader.noted("logweir/c", Err(denied("c")));
        let kept = reader.unreadable().expect("kept").sentence;
        assert!(kept.contains("logweir/b"), "the first: {kept}");
        assert!(reader.unreadable().is_none(), "taken once");
    }
}
