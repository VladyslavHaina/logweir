//! **The Kafka API versions an endpoint serves, as the endpoint told this
//! client** (PROD-01.2) — pure: what the lines mean, with no socket.
//!
//! # Why a client needs to know
//!
//! Every Kafka-compatible endpoint answers ApiVersions with the range of
//! versions it serves for each API. Logweir's own client (librdkafka)
//! negotiates inside that range and needs nothing from this module. The
//! engine does not: it sends each request at one fixed version and never asks
//! (`logweir_engine_oso::vendored::request_versions`), so an endpoint whose
//! range excludes one of those versions cannot be backed up from, or restored
//! into, at all. The same answer says whether the endpoint can name a consumer
//! group's type (ListGroups v5).
//!
//! # Where the answer comes from
//!
//! librdkafka keeps a broker's ApiVersions answer private. It prints it under
//! the `feature` debug context, one line per API key, and
//! `logweir_rdkafka_ffi::logs::drain_logs` reads those lines off a private
//! queue. The two line shapes are librdkafka's
//! (`rd_kafka_handle_ApiVersion`, `rdkafka_request.c:3194-3209` in rdkafka-sys
//! 4.10.0+2.12.1), each behind its own thread and broker prefix:
//!
//! ```text
//! [thrd:localhost:9092/bootstrap]: localhost:9092/bootstrap: Broker API support:
//! [thrd:localhost:9092/bootstrap]: localhost:9092/bootstrap:   ApiKey Produce (0) Versions 0..13
//! ```
//!
//! **A log line is not an API**, and this module treats it as one would treat
//! any text it does not control: a line it cannot read contributes nothing,
//! an answer it cannot read whole is not an answer, and "nothing observed" is
//! reported as exactly that ([`fold`] returns `None`), never as an empty
//! range. `the_shapes_are_the_ones_the_locked_librdkafka_prints` (this crate's
//! `client` tests) holds the two shapes against a real librdkafka connection,
//! so a release that rewords them fails a test instead of silently turning
//! every answer into "not observed".

use std::collections::BTreeMap;

/// The facility librdkafka logs a broker's ApiVersions answer under.
pub const FACILITY: &str = "APIVERSION";

/// The text that opens one broker's answer.
const HEADER: &str = "Broker API support:";

/// Kafka's protocol API key of ListGroups.
pub const LIST_GROUPS: i16 = 16;

/// The first ListGroups version whose answer names each group's type
/// (KIP-848): below it, every group reads as type Unknown.
pub const LIST_GROUPS_WITH_TYPES: i16 = 5;

/// One log line this module can read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Line {
    /// A broker's answer starts: everything this connection said before is
    /// superseded.
    Header,
    /// One API of the answer: its key and the versions served, inclusive.
    Api {
        /// Kafka's protocol API key.
        key: i16,
        /// The lowest version served.
        min: i16,
        /// The highest version served.
        max: i16,
    },
}

/// The connection a line belongs to: librdkafka's `[thrd:…]` prefix, or the
/// empty string for a line without one. Lines of two connections interleave
/// in the log, so an answer is assembled per connection.
#[must_use]
pub fn connection_of(message: &str) -> &str {
    message
        .strip_prefix("[thrd:")
        .and_then(|rest| rest.split_once("]: "))
        .map_or("", |(thread, _)| thread)
}

/// Reads one log line, or `None` for a line that is neither shape.
#[must_use]
pub fn parse_line(message: &str) -> Option<Line> {
    if message.trim_end().ends_with(HEADER) {
        return Some(Line::Header);
    }
    // "…  ApiKey <name> (<key>) Versions <min>..<max>". The name is free text
    // (`Unknown-15000?` for a key librdkafka has no name for), so the key is
    // read from the LAST parenthesis before " Versions ".
    let (head, versions) = message.rsplit_once(") Versions ")?;
    head.rfind("ApiKey ")?;
    let key: i16 = head.rsplit_once('(')?.1.trim().parse().ok()?;
    let (min, max) = versions.trim().split_once("..")?;
    let (min, max): (i16, i16) = (min.trim().parse().ok()?, max.trim().parse().ok()?);
    (min >= 0 && max >= min).then_some(Line::Api { key, min, max })
}

/// The versions every observed broker connection serves, per API key.
///
/// The INTERSECTION over the connections folded in: a version is served only
/// if every one of them serves it, because the engine may be sent to any of
/// them. An API key some connection did not list is not served.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersions {
    ranges: BTreeMap<i16, (i16, i16)>,
    connections: usize,
}

impl ApiVersions {
    /// One connection's answer, from `(key, min, max)` triples. For tests and
    /// doubles; [`fold`] builds the real ones.
    #[must_use]
    pub fn of(ranges: &[(i16, i16, i16)]) -> Self {
        Self {
            ranges: ranges.iter().map(|(k, a, b)| (*k, (*a, *b))).collect(),
            connections: 1,
        }
    }

    /// The inclusive range served for `key`, or `None` when it is not served.
    #[must_use]
    pub fn range(&self, key: i16) -> Option<(i16, i16)> {
        self.ranges.get(&key).copied()
    }

    /// Whether `version` of `key` is served.
    #[must_use]
    pub fn serves(&self, key: i16, version: i16) -> bool {
        self.range(key)
            .is_some_and(|(min, max)| (min..=max).contains(&version))
    }

    /// How many broker connections' answers this is the intersection of.
    #[must_use]
    pub fn connections(&self) -> usize {
        self.connections
    }

    /// `v<min>-v<max>` for `key`, or `not served`: the spelling a check's
    /// message uses.
    #[must_use]
    pub fn describe(&self, key: i16) -> String {
        match self.range(key) {
            Some((min, max)) if min == max => format!("v{min}"),
            Some((min, max)) => format!("v{min}-v{max}"),
            None => "not served".to_string(),
        }
    }
}

/// Assembles the answers out of `(facility, message)` log lines, in the order
/// they were logged.
///
/// `None` means NOT OBSERVED: no connection's answer was read. That is the
/// case for a client that never connected, for a log that carries no
/// `APIVERSION` line at all, and for one whose every answer was empty.
///
/// `complete` says the lines are everything the client had logged when it
/// went quiet (`logweir_rdkafka_ffi::logs::Drained::quiet`). When it is false
/// the last answer of some connection may have been cut short, and a cut
/// answer would read as "this API is not served", so the result is `None`:
/// an answer that cannot be read whole is not an answer.
#[must_use]
pub fn fold<'a>(
    lines: impl IntoIterator<Item = (&'a str, &'a str)>,
    complete: bool,
) -> Option<ApiVersions> {
    if !complete {
        return None;
    }
    // Per connection, its LATEST answer: a header starts a new one.
    let mut answers: BTreeMap<&str, BTreeMap<i16, (i16, i16)>> = BTreeMap::new();
    for (facility, message) in lines {
        if facility != FACILITY {
            continue;
        }
        let connection = connection_of(message);
        match parse_line(message) {
            Some(Line::Header) => {
                answers.insert(connection, BTreeMap::new());
            }
            Some(Line::Api { key, min, max }) => {
                // An entry with no header before it (a log read from its
                // middle) still belongs to its connection's answer.
                answers
                    .entry(connection)
                    .or_default()
                    .insert(key, (min, max));
            }
            None => {}
        }
    }
    let mut answers = answers.into_values().filter(|a| !a.is_empty());
    let mut ranges = answers.next()?;
    let mut connections = 1;
    for other in answers {
        connections += 1;
        ranges = ranges
            .into_iter()
            .filter_map(|(key, (min, max))| {
                let (omin, omax) = *other.get(&key)?;
                let (min, max) = (min.max(omin), max.min(omax));
                (min <= max).then_some((key, (min, max)))
            })
            .collect();
    }
    Some(ApiVersions {
        ranges,
        connections,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const BOOT: &str = "[thrd:localhost:9092/bootstrap]: localhost:9092/bootstrap: ";
    const NODE: &str = "[thrd:localhost:9092/1001]: localhost:9092/1001: ";

    fn line(prefix: &str, rest: &str) -> (&'static str, String) {
        (FACILITY, format!("{prefix}{rest}"))
    }

    fn folded(lines: &[(&'static str, String)], complete: bool) -> Option<ApiVersions> {
        fold(lines.iter().map(|(f, m)| (*f, m.as_str())), complete)
    }

    #[test]
    fn the_two_shapes_are_read_and_nothing_else_is() {
        assert_eq!(
            parse_line(&format!("{BOOT}Broker API support:")),
            Some(Line::Header)
        );
        assert_eq!(
            parse_line(&format!("{BOOT}  ApiKey Produce (0) Versions 0..13")),
            Some(Line::Api {
                key: 0,
                min: 0,
                max: 13
            })
        );
        // A key librdkafka has no name for, and a name with a parenthesis of
        // its own: the key is the last parenthesis before " Versions ".
        assert_eq!(
            parse_line("  ApiKey Unknown-15000? (15000) Versions 0..0"),
            Some(Line::Api {
                key: 15000,
                min: 0,
                max: 0
            })
        );
        assert_eq!(
            parse_line("  ApiKey Odd (name) (7) Versions 2..3"),
            Some(Line::Api {
                key: 7,
                min: 2,
                max: 3
            })
        );
        for unreadable in [
            "",
            "Updated enabled protocol features +ApiVersion to MsgVer1,ApiVersion",
            "  ApiKey Produce (zero) Versions 0..13",
            "  ApiKey Produce (0) Versions 0-13",
            "  ApiKey Produce (0) Versions 13..0",
            "  ApiKey Produce (0) Versions -1..3",
            "  Produce (0) Versions 0..13",
        ] {
            assert_eq!(parse_line(unreadable), None, "{unreadable:?}");
        }
        assert_eq!(
            connection_of(&format!("{BOOT}x")),
            "localhost:9092/bootstrap"
        );
        assert_eq!(connection_of("no prefix"), "");
    }

    #[test]
    fn one_answer_is_its_ranges() {
        let v = folded(
            &[
                line(BOOT, "Broker API support:"),
                line(BOOT, "  ApiKey Produce (0) Versions 0..7"),
                line(BOOT, "  ApiKey Fetch (1) Versions 4..13"),
                line(BOOT, "  ApiKey ListGroups (16) Versions 0..4"),
            ],
            true,
        )
        .expect("one answer");
        assert_eq!(v.connections(), 1);
        assert_eq!(v.range(0), Some((0, 7)));
        assert!(v.serves(0, 7) && !v.serves(0, 8));
        assert!(v.serves(1, 11) && !v.serves(1, 3));
        assert!(!v.serves(LIST_GROUPS, LIST_GROUPS_WITH_TYPES));
        // An API the answer does not list is not served at any version.
        assert_eq!(v.range(32), None);
        assert!(!v.serves(32, 0));
        assert_eq!(v.describe(0), "v0-v7");
        assert_eq!(v.describe(32), "not served");
    }

    /// Two brokers that serve different ranges: only what BOTH serve counts,
    /// and an API one of them lacks is not served.
    #[test]
    fn several_connections_are_intersected() {
        let v = folded(
            &[
                line(BOOT, "Broker API support:"),
                line(NODE, "Broker API support:"),
                line(BOOT, "  ApiKey Produce (0) Versions 0..13"),
                line(NODE, "  ApiKey Produce (0) Versions 3..7"),
                line(BOOT, "  ApiKey Fetch (1) Versions 4..18"),
                line(NODE, "  ApiKey ListGroups (16) Versions 0..5"),
                line(BOOT, "  ApiKey ListGroups (16) Versions 0..4"),
            ],
            true,
        )
        .expect("two answers");
        assert_eq!(v.connections(), 2);
        assert_eq!(v.range(0), Some((3, 7)));
        assert_eq!(v.range(1), None, "Fetch is missing from one answer");
        assert_eq!(v.range(16), Some((0, 4)));
    }

    /// A reconnect logs a new answer for the same connection: the new one
    /// replaces the old one, it is not merged with it.
    #[test]
    fn a_connections_later_answer_replaces_its_earlier_one() {
        let v = folded(
            &[
                line(BOOT, "Broker API support:"),
                line(BOOT, "  ApiKey Produce (0) Versions 0..7"),
                line(BOOT, "  ApiKey Fetch (1) Versions 4..13"),
                line(BOOT, "Broker API support:"),
                line(BOOT, "  ApiKey Produce (0) Versions 0..13"),
            ],
            true,
        )
        .expect("an answer");
        assert_eq!(v.connections(), 1);
        assert_eq!(v.range(0), Some((0, 13)));
        assert_eq!(v.range(1), None, "the later answer does not list Fetch");
    }

    /// NOT OBSERVED is `None`, in each of its shapes; never an empty range
    /// set that would read as "nothing is served".
    #[test]
    fn nothing_observed_is_none() {
        assert_eq!(folded(&[], true), None);
        assert_eq!(
            folded(
                &[("FEATURE", "Updated enabled protocol features".to_string())],
                true
            ),
            None
        );
        // A header with no entry after it.
        assert_eq!(folded(&[line(BOOT, "Broker API support:")], true), None);
        // The same lines under another facility are not an answer.
        assert_eq!(
            folded(
                &[(
                    "BROKER",
                    format!("{BOOT}  ApiKey Produce (0) Versions 0..7")
                )],
                true
            ),
            None
        );
        // A read that was cut short: the lines are there, the answer is not.
        let cut = [
            line(BOOT, "Broker API support:"),
            line(BOOT, "  ApiKey Produce (0) Versions 0..13"),
        ];
        assert!(folded(&cut, true).is_some());
        assert_eq!(folded(&cut, false), None);
    }
}
