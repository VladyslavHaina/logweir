//! The stdout frame writer — D2 §4.1's frame format, from the writing side.
//!
//! # Why a type and not three `println!`s
//!
//! The end frame declares, for every stream, how many parts were printed and
//! what their digest is, and for the topic lines both a COUNT and a DIGEST.
//! `weirkeeper::check::relay` verifies all of that and reports any
//! disagreement as [`CheckCode::ResultUnreadable`] — with the exit code and
//! **without** the log content, so a runner whose declaration drifts from its
//! output produces a check nobody can read and nobody can debug.
//!
//! So the declaration is DERIVED from what this writer actually wrote. The
//! caller hands it entries and payloads; it records the bytes it emitted and
//! builds the end frame from that record. There is no path on which a count
//! is computed twice.
//!
//! # The relay budget is enforced HERE, and that is a second bound
//!
//! `logweir_kafka::inventory::assemble` already truncates at
//! `relayBudgetBytes` — it is the bound D2 §5.5's arithmetic is about, and it
//! is tested in that crate. This writer enforces the same budget again over
//! the entries it is handed, because the two bounds fail differently: the
//! first stops the runner ASSEMBLING more than the budget, the second stops it
//! PRINTING more than the budget whatever it was handed. A check kind that
//! writes topic lines from somewhere other than `assemble` — or an `assemble`
//! whose accounting drifts — still cannot overrun the controller's 8 MiB log
//! read.
//!
//! When the second bound bites, the caller is told how many entries were
//! written so it can correct the result document rather than declare a digest
//! over lines it did not print. [`crate::check::inventory`] does exactly that.

use std::collections::BTreeMap;
use std::io::Write;

use logweir_core::check_contract::{
    frames, CheckCode, EndFrame, FrameError, Stream, TopicEntry, TOPIC_FRAME_PREFIX,
};

/// Why emitting failed.
///
/// BOTH ARMS MEAN "no end line was printed", which is D2 §4.2's exit 1: an
/// operational failure before a result existed. They are kept apart because
/// they are fixed in different places — a refused frame is a runner bug, a
/// broken pipe is the pod's.
#[derive(Debug)]
pub enum EmitError {
    /// A frame the contract refuses to write (too long, unencodable).
    Frame(FrameError),
    /// stdout would not take it.
    Io(std::io::Error),
}

impl std::fmt::Display for EmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Frame(e) => write!(f, "a check frame could not be written: {e}"),
            Self::Io(e) => write!(f, "the check's stdout could not be written: {e}"),
        }
    }
}

impl EmitError {
    /// Always [`CheckCode::ResultUnreadable`] — the same code the controller
    /// will independently reach, because the end line it needs is missing.
    #[must_use]
    pub fn code(&self) -> CheckCode {
        CheckCode::ResultUnreadable
    }
}

impl From<FrameError> for EmitError {
    fn from(e: FrameError) -> Self {
        Self::Frame(e)
    }
}

impl From<std::io::Error> for EmitError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

/// The bytes one entry costs against the relay budget: the frame prefix plus
/// the canonical TSV line.
///
/// The SAME arithmetic as `logweir_kafka::inventory::relay_cost`, and this
/// module does not call that function only because `logweir-kafka` is not a
/// dependency of every consumer of this one. `the_two_relay_costs_agree` in
/// `crates/logweir/tests/check_cli.rs` asserts they return the same number for
/// every shape, so the two bounds cannot drift into disagreeing about what a
/// line costs.
#[must_use]
pub fn relay_cost(entry: &TopicEntry) -> u64 {
    (TOPIC_FRAME_PREFIX.len() + entry.tsv_line().len()) as u64
}

/// A writer that emits frames and remembers exactly what it emitted.
pub struct FrameWriter<W: Write> {
    out: W,
    budget: u64,
    used: u64,
    /// `None` until [`FrameWriter::write_topics`] is called even once.
    ///
    /// The distinction is load-bearing: `None` produces an end frame with NO
    /// `topicLines` block, which says "this check produced no inventory at
    /// all", while `Some(vec![])` produces `{"count":0,…}`, which says "the
    /// cluster showed no topic". A discovery against an unreachable broker and
    /// a discovery against an empty cluster are different findings.
    topics: Option<Vec<TopicEntry>>,
    streams: BTreeMap<Stream, (Vec<u8>, usize)>,
    ended: bool,
}

impl<W: Write> FrameWriter<W> {
    /// `budget` is the plan's `relayBudgetBytes`, counted over topic lines
    /// only — the bound D2 §5.5 states.
    pub fn new(out: W, budget: u64) -> Self {
        Self {
            out,
            budget,
            used: 0,
            topics: None,
            streams: BTreeMap::new(),
            ended: false,
        }
    }

    /// Bytes of topic lines already written.
    #[must_use]
    pub fn used(&self) -> u64 {
        self.used
    }

    /// Write topic lines until the budget is spent; returns how many were
    /// written.
    ///
    /// A short return is NOT an error: it is the relay bound biting, and D2
    /// §5.5 calls the result `truncated: RelayLimit`. The caller must reflect
    /// it in the result document, because the end frame's `topicLines` block
    /// is built from the lines this wrote and a document claiming more would
    /// make the whole relay unreadable.
    ///
    /// # Errors
    /// [`EmitError`] when a line cannot be rendered or stdout refuses it.
    pub fn write_topics(&mut self, entries: &[TopicEntry]) -> Result<usize, EmitError> {
        let written = self.topics.get_or_insert_with(Vec::new);
        let mut count = 0usize;
        for entry in entries {
            let cost = relay_cost(entry);
            // THE SECOND BOUND. Removing this check is mutant M6, and
            // `the_writer_stops_at_the_relay_budget` is what kills it.
            if self.used.saturating_add(cost) > self.budget {
                break;
            }
            let line = frames::write_topic_line(entry)?;
            writeln!(self.out, "{line}")?;
            self.used += cost;
            written.push(entry.clone());
            count += 1;
        }
        Ok(count)
    }

    /// Write one stream's payload as part frames and record its summary.
    ///
    /// An EMPTY payload still writes one part, which is
    /// `frames::write_parts`'s own rule: "the stream was present and empty"
    /// and "the stream was absent" stay distinguishable in the end frame.
    ///
    /// # Errors
    /// [`EmitError`].
    pub fn write_stream(&mut self, stream: Stream, payload: &[u8]) -> Result<(), EmitError> {
        let parts = frames::write_parts(stream, payload)?;
        for line in &parts {
            writeln!(self.out, "{line}")?;
        }
        self.streams.insert(stream, (payload.to_vec(), parts.len()));
        Ok(())
    }

    /// The end frame, built from the record of what was written, and the LAST
    /// line this writer emits.
    ///
    /// # Errors
    /// [`EmitError`].
    pub fn finish(&mut self, plan_sha256: &str, subject_uid: &str) -> Result<EndFrame, EmitError> {
        let end = frames::end_frame(
            plan_sha256,
            subject_uid,
            &self.streams,
            self.topics.as_deref(),
        );
        let line = frames::write_end(&end)?;
        writeln!(self.out, "{line}")?;
        self.out.flush()?;
        self.ended = true;
        Ok(end)
    }

    /// Whether an end line was printed — D2 §4.2's exit-0 condition, read off
    /// the writer rather than tracked by the caller.
    #[must_use]
    pub fn ended(&self) -> bool {
        self.ended
    }
}
