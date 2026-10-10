//! **FX-7 — what a receipt's manifest-version pin proves IN THE BUCKET BEING
//! READ**, decided once for every reader that acts on it: the drill's point
//! binding ([`crate::drill::binding::verify_point_binding`]), the
//! `catalogSync` deep check ([`crate::check::kinds::catalog_sync`]) and, since
//! FX-14, the restore preflight of a plan bound to a point
//! ([`crate::check::kinds::restore`]), which answers `ManifestSuperseded`,
//! the store's code with [`UNREADABLE_REMEDY`], or `ready` with
//! [`UNCHECKED_NOTE`] for the three rows of the table below.
//!
//! # A version id belongs to one object in ONE bucket
//!
//! A receipt taken on a versioned bucket pins the version id its manifest
//! read-back was answered with. The catalog makes an archive copied to a second
//! bucket ONE point in TWO places (`docs/formats/catalog-point.md`, the point
//! id and `location_id`; `docs/kubernetes.md`, the best-of availability
//! merge), and a copy made by anything but version-preserving replication —
//! `aws s3 sync`, `mc mirror`, rclone, a migration to another store, any
//! unversioned destination — carries the receipt's pin and NOT the pinned
//! version. So "this bucket's current version is not the pin" is two
//! different facts, and a reader that meets it asks the bucket for the pinned
//! version BY ID before it calls the point written again ([`judge`]):
//!
//! | the read of the pinned version | verdict | binding | deep check |
//! |---|---|---|---|
//! | its bytes | [`PinVerdict::Superseded`]: written again HERE after the point was signed | exit 3 | `Conflict` |
//! | `NotFound` — S3's `404 NoSuchVersion`, and `400 InvalidArgument` for an id the store could never have issued (`Store::get_version_capped`) | [`PinVerdict::Unchecked`] | the digest, and [`UNCHECKED_NOTE`] in the log | the digest, and [`UNCHECKED_NOTE`] in `remedy` |
//! | a store that does not read by version (`StoreError::Backend`) | [`PinVerdict::Unchecked`] | the same | the same |
//! | any other failure | [`PinVerdict::Unreadable`]: could not tell | exit 1 | `Unreadable` |
//!
//! The extra read is made only on that mismatch, so a point read in the bucket
//! that signed it, unchanged, costs what it cost before.
//!
//! # The cost, stated
//!
//! The pin is checked only where the bucket being read still HOLDS the pinned
//! version and SERVES it by id. "Not this bucket's history" and "this bucket's
//! history, gone" are ONE answer to a reader, so a REAL rewrite reaches
//! [`PinVerdict::Unchecked`] by three routes (FX-7 re-check, R-L1):
//!
//! 1. the pinned version is gone from the bucket that signed the point — a
//!    lifecycle rule expired it, or a principal holding `s3:DeleteObjectVersion`
//!    deleted it (measured: a delete of the pin turned the signing bucket's
//!    `Conflict` into `Available` + the note);
//! 2. the rewrite sits in a bucket that never held the pinned version — a copy
//!    synced AFTER the set was written again, or a copy rewritten after it was
//!    made (measured);
//! 3. a store that cannot serve a version it holds (a 404, a `400
//!    InvalidArgument`, another version or none for a retained id) — a
//!    misbehaving S3-compatible store or a proxy that drops `?versionId=`; seen
//!    on neither MinIO nor SeaweedFS.
//!
//! On all three the point degrades to the unversioned case: the digest alone,
//! which an identical manifest over rewritten segments passes by construction.
//! Only a rewrite that CHANGES the manifest bytes is still caught there. Each
//! is reported with [`UNCHECKED_NOTE`], never as a rewrite and never as a pass
//! of the pin. Object Lock retention covering a point's lifetime keeps its
//! pinned version against routes 1's deletion and expiry; a bucket whose
//! noncurrent versions must outlive its points is the operator's rule to keep.
//! The alternative the review named — recording the SIGNING bucket in the
//! receipt and enforcing the pin only there — is a format change, and it would
//! not help route 1 either. Only a segment-digest reader closes all three
//! (`SegmentSample`, declared and not implemented by this build).
//!
//! # The texts live here, once
//!
//! "The pinned version is superseded" widens `availability: Conflict` and
//! exit 3 inside major 1. The owner ruled that MINOR on 2026-10-07 (FX-7
//! review, V3): it can only move a point toward not restorable
//! (`docs/stability.md`). [`judge`]'s `Superseded` arm and
//! [`SUPERSEDED_CAUSE`] are the whole of that cause in both readers.

use logweir_core::backup_receipt::pinnable_version_id;
use logweir_engine_oso::storage::StoreError;

/// The cause, without its subject, so each reader puts its own in front:
/// "this point's manifest …" in the catalog, "recovery point X's manifest K
/// …" in the binding's refusal. A macro and not only a `const` so that
/// [`SUPERSEDED_REMEDY`] can be built from it at compile time.
macro_rules! superseded_cause {
    () => {
        "was written again in this bucket after the point was signed: the bucket still \
         holds the version the signed receipt pins, and that version is no longer the \
         current one"
    };
}

/// **The one cause of a superseded pin**, for both readers.
pub const SUPERSEDED_CAUSE: &str = superseded_cause!();

/// The deep check's remedy for [`PinVerdict::Superseded`]. A fixed sentence,
/// like every remedy `catalog_sync::remedy_for` returns, so no adopter bytes
/// reach an entry.
pub const SUPERSEDED_REMEDY: &str = concat!(
    "This point's manifest ",
    superseded_cause!(),
    ". A restore reads only the current version, so it would read a manifest this point \
     does not describe, and the set's segments may have been rewritten under it. Restore \
     from another point."
);

/// **The note for [`PinVerdict::Unchecked`]: never a refusal, never the
/// superseded remedy.** The deep check appends it to the entry's `remedy`; the
/// binding logs it beside the point it proved.
pub const UNCHECKED_NOTE: &str = "The pin could not be checked in this bucket: the signed \
     receipt pins a manifest version this bucket does not hold (a copy of the archive, a \
     bucket without versioning, or a version that was expired or deleted), so the manifest was \
     checked by its digest alone, which cannot see segments rewritten under an identical \
     manifest.";

/// **The deep check's remedy when the pinned version could not be READ** (FX-7
/// re-check, nit 1): "could not tell", and the one grant the generic
/// `Unreadable` remedy does not name. A fixed sentence, like every remedy.
pub const UNREADABLE_REMEDY: &str = "The manifest's pinned version could not be read — this \
     is \"could not tell\", not \"is not there\", and never a pass over a rewrite nobody \
     could rule out. Reading a version by id needs s3:GetObjectVersion on the archive prefix, \
     beside s3:GetObject (a 403 here is that grant missing); check it, the endpoint and the \
     network path, then sync again.";

/// What the receipt's pin proves in the bucket being read.
#[derive(Debug)]
pub enum PinVerdict {
    /// The receipt pins nothing [`pinnable_version_id`] accepts: a receipt
    /// without the field (every `1.0.0` and `1.1.0` one — an unversioned
    /// bucket's, or one written before FX-7), or a blank or `"null"` pin no
    /// writer produces (a pin is what that function says it is, on the writing
    /// side and on the reading side). The digest is the whole check, as it
    /// always was.
    Unpinned,
    /// The key's current version IS the pinned one. No extra read was made.
    Current,
    /// The bucket HOLDS the pinned version and it is not the current one: the
    /// set was written again here after the point was signed.
    Superseded {
        /// The version the signed receipt pins.
        pinned: String,
        /// The version the bucket answers with now (`None`: no version id, e.g.
        /// versioning suspended and the key written again).
        current: Option<String>,
        /// Whether the retained pinned version still hashes to the attested
        /// digest.
        retained_matches: bool,
    },
    /// The bucket cannot answer for the pin: it holds no such version, or it
    /// does not read by version at all. The digest decides, with
    /// [`UNCHECKED_NOTE`].
    Unchecked {
        /// The version the signed receipt pins.
        pinned: String,
    },
    /// The read of the pinned version failed for any other reason: could not
    /// tell.
    Unreadable {
        /// The version the signed receipt pins.
        pinned: String,
        /// What the store answered.
        error: StoreError,
    },
}

/// Decide what the pin proves here.
///
/// * `pin` — the receipt's `archive.manifest_version_id`, from the RECEIPT,
///   the verification root, never from a catalog record;
/// * `answered` — the version id the current read of the manifest was
///   answered with;
/// * `attested_sha256` — the manifest digest the receipt attests;
/// * `read_pinned` — the read of one version by id. Called only when the
///   current version is not the pin, so the common case costs no read.
pub fn judge(
    pin: Option<&str>,
    answered: Option<&str>,
    attested_sha256: &str,
    read_pinned: impl FnOnce(&str) -> Result<Vec<u8>, StoreError>,
) -> PinVerdict {
    let Some(pinned) = pinnable_version_id(pin) else {
        return PinVerdict::Unpinned;
    };
    let current = pinnable_version_id(answered);
    if current.as_deref() == Some(pinned.as_str()) {
        return PinVerdict::Current;
    }
    match read_pinned(&pinned) {
        Ok(bytes) => PinVerdict::Superseded {
            retained_matches: logweir_core::ids::sha256_prefixed(&bytes) == attested_sha256,
            pinned,
            current,
        },
        // NOT THIS BUCKET'S HISTORY. `NotFound` covers S3's `400
        // InvalidArgument` for an id the store could never have issued
        // (`Store::get_version_capped`); `Backend` is a store that does not read by
        // version at all, which answered with another version or none.
        Err(StoreError::NotFound(_) | StoreError::Backend(_)) => PinVerdict::Unchecked { pinned },
        Err(error) => PinVerdict::Unreadable { pinned, error },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ATTESTED: &str = "sha256:attested";

    fn bytes_hashing_to_nothing_attested() -> Vec<u8> {
        b"the pinned version".to_vec()
    }

    /// The four answers a by-id read can give, each its own verdict.
    #[test]
    fn the_read_of_the_pinned_version_decides_and_nothing_else() {
        assert!(matches!(
            judge(Some("v1"), Some("v2"), ATTESTED, |_| Ok(
                bytes_hashing_to_nothing_attested()
            )),
            PinVerdict::Superseded { ref pinned, ref current, retained_matches: false }
                if pinned == "v1" && current.as_deref() == Some("v2")
        ));
        assert!(matches!(
            judge(Some("v1"), Some("copy-1"), ATTESTED, |v| Err(StoreError::NotFound(
                v.to_string()
            ))),
            PinVerdict::Unchecked { ref pinned } if pinned == "v1"
        ));
        assert!(matches!(
            judge(Some("v1"), None, ATTESTED, |_| Err(StoreError::Backend(
                "this store does not read objects by version".into()
            ))),
            PinVerdict::Unchecked { .. }
        ));
        assert!(matches!(
            judge(Some("v1"), Some("v2"), ATTESTED, |_| Err(StoreError::Io(
                "403 Forbidden".into()
            ))),
            PinVerdict::Unreadable { .. }
        ));
    }

    /// The retained version is hashed, so the binding can say whether the
    /// attested manifest is still in the bucket at its pinned version.
    #[test]
    fn a_retained_version_that_hashes_to_the_attestation_says_so() {
        let bytes = b"{\"topics\":[]}".to_vec();
        let attested = logweir_core::ids::sha256_prefixed(&bytes);
        assert!(matches!(
            judge(Some("v1"), Some("v2"), &attested, |_| Ok(bytes.clone())),
            PinVerdict::Superseded {
                retained_matches: true,
                ..
            }
        ));
    }

    /// The common case costs nothing: the pinned version IS current, and the
    /// by-id read is never made.
    #[test]
    fn a_current_pin_makes_no_extra_read() {
        assert!(matches!(
            judge(Some("v1"), Some("v1"), ATTESTED, |_| panic!(
                "a current pin must not be read by id"
            )),
            PinVerdict::Current
        ));
    }

    /// Versioning suspended, then the key written again: S3 answers the
    /// current read with `"null"`, which is no version — and the pinned
    /// version is still in the bucket's history, so the set WAS written again
    /// here. The by-id read is what tells this from a copy.
    #[test]
    fn a_null_current_version_over_a_retained_pin_is_superseded() {
        assert!(matches!(
            judge(Some("v1"), Some("null"), ATTESTED, |_| Ok(b"x".to_vec())),
            PinVerdict::Superseded { current: None, .. }
        ));
    }

    /// A pin is what `pinnable_version_id` says it is, on both sides: a
    /// receipt carrying a blank or `"null"` pin pins nothing, so no by-id read
    /// is made and the digest decides (review nit: it used to be enforced as a
    /// pin that could never match).
    #[test]
    fn a_pin_no_writer_produces_is_no_pin() {
        for pin in [None, Some(""), Some("  "), Some("null")] {
            assert!(
                matches!(
                    judge(pin, Some("v1"), ATTESTED, |_| panic!(
                        "an unpinned receipt is never read by version"
                    )),
                    PinVerdict::Unpinned
                ),
                "{pin:?}"
            );
        }
    }

    /// The remedy is built FROM the cause, so the two readers cannot drift.
    #[test]
    fn the_remedy_carries_the_one_cause() {
        assert!(SUPERSEDED_REMEDY.contains(SUPERSEDED_CAUSE));
        assert!(!UNCHECKED_NOTE.contains(SUPERSEDED_CAUSE));
        assert!(UNCHECKED_NOTE.contains("could not be checked in this bucket"));
    }

    /// FX-7 re-check R-L1: the note names every way a bucket can stop holding
    /// the pinned version — DELETED included, which an operator may well have
    /// done on purpose — and says what the digest then cannot see.
    #[test]
    fn the_note_names_a_deleted_version_and_what_the_digest_misses() {
        assert!(
            UNCHECKED_NOTE.contains("expired or deleted"),
            "{UNCHECKED_NOTE}"
        );
        assert!(
            UNCHECKED_NOTE.contains("a copy of the archive"),
            "{UNCHECKED_NOTE}"
        );
        assert!(
            UNCHECKED_NOTE.contains("cannot see segments rewritten under an identical manifest"),
            "{UNCHECKED_NOTE}"
        );
    }

    /// FX-7 re-check, nit 1: the deep check's remedy for an unreadable pinned
    /// version names the one grant the generic `Unreadable` remedy does not,
    /// and is neither the superseded cause nor the "could not be checked" note.
    #[test]
    fn the_unreadable_remedy_names_get_object_version() {
        assert!(UNREADABLE_REMEDY.contains("s3:GetObjectVersion"));
        assert!(!UNREADABLE_REMEDY.contains(SUPERSEDED_CAUSE));
        assert!(!UNREADABLE_REMEDY.contains("could not be checked in this bucket"));
    }
}
