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
//! [thrd:broker.example:9092/bootstrap]: broker.example:9092/bootstrap: Broker API support:
//! [thrd:broker.example:9092/bootstrap]: broker.example:9092/bootstrap:   ApiKey Produce (0) Versions 0..13
//! ```
//!
//! **A log line is not an API**, and this module treats it as one would treat
//! any text it does not control: a line it cannot read contributes nothing,
//! an answer it cannot read whole is not an answer, and "nothing observed" is
//! reported as exactly that ([`full_view`] returns the reason), never as an
//! empty range. And an answer is about the CLUSTER only when every broker the
//! cluster lists gave one: a broker nobody asked is unknown. `the_shapes_are_the_ones_the_locked_librdkafka_prints` (this crate's
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

/// The name librdkafka logs a line under: the text between the `[thrd:…]: `
/// prefix and the next `: `. For a broker connection it is
/// `<host>:<port>/<node id>`, or `<host>:<port>/bootstrap` for a connection
/// to a bootstrap address whose broker is not yet known
/// (`rd_kafka_broker_t::rkb_name`).
#[must_use]
pub fn log_name_of(message: &str) -> &str {
    message
        .strip_prefix("[thrd:")
        .and_then(|rest| rest.split_once("]: "))
        .and_then(|(_, after)| after.split_once(": "))
        .map_or("", |(name, _)| name)
}

/// Who a connection reached, read off the name its lines were logged under.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Peer {
    /// A broker of the cluster, by the node id the cluster gave it.
    Node(i32),
    /// A bootstrap address, as `host:port`: the connection answered, and
    /// which broker stands behind the address is not known from the log.
    Bootstrap(String),
    /// A line this module cannot attribute.
    Unknown,
}

impl Peer {
    fn of(log_name: &str) -> Self {
        match log_name.rsplit_once('/') {
            Some((address, "bootstrap")) => Self::Bootstrap(address.to_string()),
            Some((_, id)) => id.parse().map_or(Self::Unknown, Self::Node),
            None => Self::Unknown,
        }
    }
}

/// One broker the cluster's own metadata lists.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Broker {
    /// The node id.
    pub id: i32,
    /// `host:port`, as the cluster advertises it to this client.
    pub address: String,
}

impl Broker {
    /// A broker from the three fields metadata carries.
    #[must_use]
    pub fn new(id: i32, host: &str, port: i32) -> Self {
        Self {
            id,
            address: format!("{host}:{port}"),
        }
    }
}

/// One connection's latest ApiVersions answer, as the log carried it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    /// librdkafka's `[thrd:…]` name: one per connection.
    pub connection: String,
    /// Who the connection reached.
    pub peer: Peer,
    /// The ranges of the answer. EMPTY when the answer's opening line was
    /// read and no entry of it was: an answer that could not be read, kept
    /// so it is counted as one.
    pub ranges: BTreeMap<i16, (i16, i16)>,
}

/// Assembles each connection's LATEST answer out of `(facility, message)` log
/// lines, in the order they were logged. A header starts a new answer for its
/// connection: a reconnect replaces, it does not merge.
///
/// An answer whose opening line was read and whose entries were not is
/// RETURNED, with no range. Dropping it would let the other connections'
/// answers stand for the whole cluster ([`full_view`] refuses that).
#[must_use]
pub fn answers<'a>(lines: impl IntoIterator<Item = (&'a str, &'a str)>) -> Vec<Answer> {
    let mut by_connection: BTreeMap<&str, Answer> = BTreeMap::new();
    for (facility, message) in lines {
        if facility != FACILITY {
            continue;
        }
        let connection = connection_of(message);
        let fresh = || Answer {
            connection: connection.to_string(),
            peer: Peer::of(log_name_of(message)),
            ranges: BTreeMap::new(),
        };
        match parse_line(message) {
            Some(Line::Header) => {
                by_connection.insert(connection, fresh());
            }
            Some(Line::Api { key, min, max }) => {
                // An entry with no header before it (a log read from its
                // middle) still belongs to its connection's answer.
                by_connection
                    .entry(connection)
                    .or_insert_with(fresh)
                    .ranges
                    .insert(key, (min, max));
            }
            None => {}
        }
    }
    by_connection.into_values().collect()
}

/// The versions EVERY broker of a cluster serves, per API key, with how many
/// brokers that is.
///
/// A value of this type exists only for a WHOLE view ([`full_view`]): every
/// broker the cluster lists answered, and so did every bootstrap address.
/// The ranges are the INTERSECTION of the answers: a version is served only
/// if every broker serves it, because the engine may be sent to any of them.
/// An API key some broker did not list is not served.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersions {
    ranges: BTreeMap<i16, (i16, i16)>,
    brokers: usize,
    differ: bool,
}

impl ApiVersions {
    /// One broker's answer, from `(key, min, max)` triples. For tests and
    /// doubles; [`full_view`] builds the real ones.
    #[must_use]
    pub fn of(ranges: &[(i16, i16, i16)]) -> Self {
        Self {
            ranges: ranges.iter().map(|(k, a, b)| (*k, (*a, *b))).collect(),
            brokers: 1,
            differ: false,
        }
    }

    /// The whole view of a cluster of `brokers` brokers whose weakest answer
    /// is `ranges`, for tests and doubles: `differ` says the answers were not
    /// all the same.
    #[must_use]
    pub fn of_cluster(ranges: &[(i16, i16, i16)], brokers: usize, differ: bool) -> Self {
        Self {
            brokers,
            differ,
            ..Self::of(ranges)
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

    /// How many DISTINCT BROKERS answered: every broker the cluster's
    /// metadata listed. Never a count of connections: one broker reached over
    /// two connections is one broker.
    #[must_use]
    pub fn brokers(&self) -> usize {
        self.brokers
    }

    /// Whether two of the answers differed (a rolling upgrade, a mixed
    /// cluster). The ranges are then the weakest answer, and a row says so.
    #[must_use]
    pub fn differ(&self) -> bool {
        self.differ
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

/// Why an observation is not a whole view of the cluster. Each is NOT
/// OBSERVED: `unknown`, never `ready`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Partial {
    /// The log was still being written when the read reached its bound, so
    /// the last answer of some connection may be cut short.
    StillLogging,
    /// The cluster's broker list was not read, so nobody knows whom to ask.
    NoBrokerList,
    /// No connection logged an ApiVersions answer at all.
    NothingAnswered,
    /// A connection's answer was opened and no entry of it could be read:
    /// the client library's lines no longer have the shape this module reads.
    Unreadable {
        /// The connections, by librdkafka's name.
        connections: Vec<String>,
    },
    /// Some broker the cluster lists did not answer, or some bootstrap
    /// address did not.
    NotEveryone {
        /// How many of the listed brokers answered.
        answered: usize,
        /// The brokers the cluster lists.
        listed: Vec<Broker>,
        /// The listed brokers that did not answer.
        silent: Vec<Broker>,
        /// The bootstrap addresses that did not answer.
        silent_bootstrap: Vec<String>,
    },
}

impl std::fmt::Display for Partial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StillLogging => write!(
                f,
                "the observing client was still logging when the read reached its bound, so no \
                 ApiVersions answer was read whole"
            ),
            Self::NoBrokerList => write!(
                f,
                "the cluster's broker list could not be read within the check's budget, so \
                 which brokers to ask is not known"
            ),
            Self::NothingAnswered => write!(
                f,
                "no broker answered ApiVersions on the observing connection within the check's \
                 budget"
            ),
            Self::Unreadable { connections } => write!(
                f,
                "{} connection(s) began an ApiVersions answer and no entry of it could be read \
                 ({}): the client library's log lines are not in the shape this build reads. \
                 That is a defect in Logweir, not in the endpoint",
                connections.len(),
                connections.join(", ")
            ),
            Self::NotEveryone {
                answered,
                listed,
                silent,
                silent_bootstrap,
            } => {
                write!(
                    f,
                    "{answered} of {} broker(s) the cluster lists answered ApiVersions within \
                     the check's budget",
                    listed.len()
                )?;
                if !silent.is_empty() {
                    let named: Vec<String> = silent
                        .iter()
                        .map(|b| format!("broker {} ({})", b.id, b.address))
                        .collect();
                    write!(f, "; no answer from {}", named.join(", "))?;
                }
                if !silent_bootstrap.is_empty() {
                    write!(
                        f,
                        "; no answer from the bootstrap address(es) {}",
                        silent_bootstrap.join(", ")
                    )?;
                }
                write!(
                    f,
                    ". A broker nobody asked is not known to serve anything, so this is not an \
                     answer about the cluster"
                )
            }
        }
    }
}

/// A `host:port` as two spellings of one address compare: lower case, no
/// brackets (an IPv6 literal is written with them in a broker list and
/// without them in a log name).
fn address_key(address: &str) -> String {
    address
        .chars()
        .filter(|c| !matches!(c, '[' | ']'))
        .collect::<String>()
        .to_ascii_lowercase()
}

/// The addresses of a `bootstrap.servers` value, each as `host:port`: the
/// `PROTOCOL://` prefix librdkafka accepts is dropped, and an entry with no
/// port has Kafka's default, as librdkafka gives it.
#[must_use]
pub fn bootstrap_addresses(bootstrap_servers: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for entry in bootstrap_servers.split(',') {
        let entry = entry.trim();
        let entry = entry.split_once("://").map_or(entry, |(_, rest)| rest);
        if entry.is_empty() {
            continue;
        }
        // The port is what follows the last colon OUTSIDE a bracketed literal.
        let has_port = entry
            .rsplit_once(':')
            .is_some_and(|(_, port)| !port.contains(']') && port.parse::<u16>().is_ok());
        let address = if has_port {
            entry.to_string()
        } else {
            format!("{entry}:9092")
        };
        if !out
            .iter()
            .any(|seen| address_key(seen) == address_key(&address))
        {
            out.push(address);
        }
    }
    out
}

/// **A whole view of the cluster, or the reason there is none** (PROD-01.2
/// review, M3).
///
/// The first version of this module intersected whichever connections the
/// client happened to have opened. On a three-broker cluster that was one,
/// two or three of them from run to run, so a row said `ready` for a cluster
/// one of whose brokers nobody had asked: wrong for a cluster whose brokers
/// differ (a rolling upgrade), and different on the next run.
///
/// **Unknown is never ready, and a broker nobody asked is unknown.** So this
/// answers only when:
///
/// * `complete`: the client had gone quiet, so no answer is cut short;
/// * `listed`, the cluster's own broker list, was read;
/// * no connection opened an answer it could not read ([`Answer::ranges`]
///   empty): such a connection is counted as NOT answered, never dropped;
/// * EVERY listed broker answered, on a connection logged under its node id;
/// * every address of `bootstrap` answered: on its own bootstrap connection,
///   or because it is the address of a listed broker that answered. An
///   address the operator named and nobody answered at is a broker the
///   operator believes is there.
///
/// The ranges are then the intersection of EVERY answer read, the bootstrap
/// connections' included, and [`ApiVersions::differ`] says whether two of
/// them differed.
///
/// A broker the cluster does not list (stopped long enough to be fenced) is
/// not asked and not counted: [`ApiVersions::brokers`] is how many the
/// cluster listed, and a row prints it.
///
/// # Errors
///
/// [`Partial`], naming how many of how many answered and which did not.
pub fn full_view(
    answers: &[Answer],
    complete: bool,
    listed: &[Broker],
    bootstrap: &[String],
) -> Result<ApiVersions, Partial> {
    if !complete {
        return Err(Partial::StillLogging);
    }
    if answers.is_empty() {
        return Err(Partial::NothingAnswered);
    }
    let unreadable: Vec<String> = answers
        .iter()
        .filter(|a| a.ranges.is_empty())
        .map(|a| a.connection.clone())
        .collect();
    if !unreadable.is_empty() {
        return Err(Partial::Unreadable {
            connections: unreadable,
        });
    }
    if listed.is_empty() {
        return Err(Partial::NoBrokerList);
    }
    let answered_node = |id: i32| answers.iter().any(|a| a.peer == Peer::Node(id));
    let silent: Vec<Broker> = listed
        .iter()
        .filter(|b| !answered_node(b.id))
        .cloned()
        .collect();
    let answered_at = |address: &str| {
        let key = address_key(address);
        answers
            .iter()
            .any(|a| matches!(&a.peer, Peer::Bootstrap(at) if address_key(at) == key))
            || listed
                .iter()
                .any(|b| address_key(&b.address) == key && answered_node(b.id))
    };
    // A bootstrap address that IS a silent listed broker's is that broker,
    // named once, as the broker.
    let is_a_silent_broker = |address: &str| {
        silent
            .iter()
            .any(|b| address_key(&b.address) == address_key(address))
    };
    let silent_bootstrap: Vec<String> = bootstrap
        .iter()
        .filter(|address| !answered_at(address) && !is_a_silent_broker(address))
        .cloned()
        .collect();
    if !silent.is_empty() || !silent_bootstrap.is_empty() {
        return Err(Partial::NotEveryone {
            answered: listed.len() - silent.len(),
            listed: listed.to_vec(),
            silent,
            silent_bootstrap,
        });
    }
    let mut all = answers.iter().map(|a| &a.ranges);
    let Some(first) = all.next() else {
        return Err(Partial::NothingAnswered);
    };
    let mut ranges = first.clone();
    let mut differ = false;
    for other in all {
        differ |= other != first;
        ranges = ranges
            .into_iter()
            .filter_map(|(key, (min, max))| {
                let (omin, omax) = *other.get(&key)?;
                let (min, max) = (min.max(omin), max.min(omax));
                (min <= max).then_some((key, (min, max)))
            })
            .collect();
    }
    Ok(ApiVersions {
        ranges,
        brokers: listed.len(),
        differ,
    })
}

/// One read of the observing client's log: the `(facility, message)` lines
/// that arrived, and whether the log had GONE QUIET when the read returned
/// (`logweir_rdkafka_ffi::logs::Drained::quiet`). A read that hit its bound
/// while lines were still arriving is not quiet.
pub type LogRead = (Vec<(String, String)>, bool);

/// **The wait for a whole view**: read the log until [`full_view`] holds, or
/// `deadline` has passed. `read` is given how long it may take and returns
/// what arrived; `quiet` is its quiet period, the least a read takes.
///
/// The LAST READ ALWAYS RUNS, so what did arrive is counted, and its own
/// `quiet` answer is what [`full_view`] is given: a log still being written
/// when the budget ran out is "an answer may be cut short", which is no
/// answer (review L4: nothing showed that a read that never went quiet is
/// refused through this function).
///
/// # Errors
///
/// The reason there is no whole view, as a sentence: [`Partial`]'s, or that
/// the log could not be read.
pub fn whole_view_within(
    mut read: impl FnMut(std::time::Duration) -> Result<LogRead, String>,
    listed: &[Broker],
    bootstrap: &[String],
    deadline: std::time::Instant,
    quiet: std::time::Duration,
) -> Result<ApiVersions, String> {
    let left = || deadline.saturating_duration_since(std::time::Instant::now());
    let mut lines: Vec<(String, String)> = Vec::new();
    loop {
        let (arrived, went_quiet) = read(left().max(quiet * 2))
            .map_err(|e| format!("the observing client's log could not be read: {e}"))?;
        lines.extend(arrived);
        let view = full_view(
            &answers(lines.iter().map(|(f, m)| (f.as_str(), m.as_str()))),
            went_quiet,
            listed,
            bootstrap,
        );
        match view {
            Ok(view) => return Ok(view),
            Err(partial) if left() < quiet => return Err(partial.to_string()),
            Err(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BOOT: &str = "[thrd:broker.example:9092/bootstrap]: broker.example:9092/bootstrap: ";
    const NODE: &str = "[thrd:broker.example:9092/1001]: broker.example:9092/1001: ";

    fn line(prefix: &str, rest: &str) -> (&'static str, String) {
        (FACILITY, format!("{prefix}{rest}"))
    }

    /// The prefix of a line from a connection to node `id` at `host`.
    fn node(host: &str, id: i32) -> String {
        format!("[thrd:{host}:9092/{id}]: {host}:9092/{id}: ")
    }

    /// One whole answer on the connection `prefix`: the header and the entries.
    fn answer(prefix: &str, entries: &[&str]) -> Vec<(&'static str, String)> {
        let mut out = vec![line(prefix, "Broker API support:")];
        out.extend(
            entries
                .iter()
                .map(|e| line(prefix, &format!("  ApiKey {e}"))),
        );
        out
    }

    fn read(lines: &[(&'static str, String)]) -> Vec<Answer> {
        answers(lines.iter().map(|(f, m)| (*f, m.as_str())))
    }

    /// A cluster of the brokers `ids`, at `<letter>.example:9092`.
    fn cluster(ids: &[i32]) -> Vec<Broker> {
        ids.iter()
            .map(|id| Broker::new(*id, &format!("b{id}.example"), 9092))
            .collect()
    }

    const KAFKA: [&str; 3] = [
        "Produce (0) Versions 0..13",
        "Fetch (1) Versions 4..18",
        "ListGroups (16) Versions 0..5",
    ];

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
            "broker.example:9092/bootstrap"
        );
        assert_eq!(connection_of("no prefix"), "");
    }

    /// Who a connection reached is read off the name its lines are logged
    /// under, in each shape librdkafka 2.12.1 uses (captured on the compose
    /// stack's three-broker cluster by PROD-01.2's review).
    #[test]
    fn a_connections_peer_is_read_off_its_log_name() {
        for (message, peer) in [
            (
                "[thrd:localhost:29112/1]: localhost:29112/1: Broker API support:",
                Peer::Node(1),
            ),
            (
                "[thrd:localhost:29112/bootstrap]: localhost:29112/bootstrap: Broker API support:",
                Peer::Bootstrap("localhost:29112".to_string()),
            ),
            // The coordinator's connection is a second connection to a broker
            // the cluster already listed.
            (
                "[thrd:GroupCoordinator]: GroupCoordinator/1: Broker API support:",
                Peer::Node(1),
            ),
            // A thread started as a bootstrap connection and since given its
            // node id: the log name is the current one.
            (
                "[thrd:localhost:29113/bootstrap]: localhost:29113/2: Broker API support:",
                Peer::Node(2),
            ),
            (
                "[thrd:[::1]:9092/bootstrap]: [::1]:9092/bootstrap: Broker API support:",
                Peer::Bootstrap("[::1]:9092".to_string()),
            ),
            ("Broker API support:", Peer::Unknown),
            (
                "[thrd:main]: something else: Broker API support:",
                Peer::Unknown,
            ),
        ] {
            assert_eq!(Peer::of(log_name_of(message)), peer, "{message}");
        }
    }

    #[test]
    fn one_broker_that_answered_is_its_ranges() {
        let lines = answer(
            BOOT,
            &[
                "Produce (0) Versions 0..7",
                "Fetch (1) Versions 4..13",
                "ListGroups (16) Versions 0..4",
            ],
        );
        // One broker, whose advertised address is the bootstrap address. The
        // bootstrap connection alone is not the broker's answer.
        let listed = [Broker::new(1001, "broker.example", 9092)];
        assert!(
            matches!(
                full_view(&read(&lines), true, &listed, &[]),
                Err(Partial::NotEveryone { answered: 0, .. })
            ),
            "a bootstrap connection is not the broker: nobody asked node 1001"
        );
        let mut lines = lines;
        lines.extend(answer(
            NODE,
            &[
                "Produce (0) Versions 0..7",
                "Fetch (1) Versions 4..13",
                "ListGroups (16) Versions 0..4",
            ],
        ));
        let v = full_view(
            &read(&lines),
            true,
            &listed,
            &["broker.example:9092".to_string()],
        )
        .expect("the one broker answered");
        assert_eq!(v.brokers(), 1, "two connections, one broker");
        assert!(!v.differ());
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

    /// **PROD-01.2 review, M3: a view of some of the brokers is not an
    /// answer.** Three brokers listed; all three answer, then two, then one
    /// reached over three connections.
    ///
    /// Mutants: the partial view accepted (the `silent` check dropped), and
    /// connections counted as brokers (three answers standing for three
    /// brokers), each fail an arm here.
    #[test]
    fn every_listed_broker_must_answer_or_there_is_no_answer() {
        let listed = cluster(&[1, 2, 3]);
        let from = |ids: &[i32]| -> Vec<(&'static str, String)> {
            ids.iter()
                .flat_map(|id| answer(&node(&format!("b{id}.example"), *id), &KAFKA))
                .collect()
        };

        let v = full_view(&read(&from(&[1, 2, 3])), true, &listed, &[]).expect("all three");
        assert_eq!(v.brokers(), 3);
        assert!(v.serves(0, 8) && !v.differ());

        // Two of three: not an answer, and the reason names how many of how
        // many, and which one.
        let partial = full_view(&read(&from(&[1, 3])), true, &listed, &[])
            .expect_err("broker 2 was not asked");
        assert_eq!(
            partial,
            Partial::NotEveryone {
                answered: 2,
                listed: listed.clone(),
                silent: vec![Broker::new(2, "b2.example", 9092)],
                silent_bootstrap: vec![],
            }
        );
        let said = partial.to_string();
        assert!(
            said.contains("2 of 3 broker(s)") && said.contains("broker 2 (b2.example:9092)"),
            "{said}"
        );

        // ONE broker reached over THREE connections (its own, the bootstrap
        // address it stands behind, and the group coordinator's) is one
        // broker of three. This is what the review measured on the compose
        // cluster from one bootstrap address.
        let mut one_broker = answer(&node("b1.example", 1), &KAFKA);
        one_broker.extend(answer(
            "[thrd:b1.example:9092/bootstrap]: b1.example:9092/bootstrap: ",
            &KAFKA,
        ));
        one_broker.extend(answer(
            "[thrd:GroupCoordinator]: GroupCoordinator/1: ",
            &KAFKA,
        ));
        assert_eq!(read(&one_broker).len(), 3, "three connections");
        match full_view(&read(&one_broker), true, &listed, &[]) {
            Err(Partial::NotEveryone {
                answered, silent, ..
            }) => {
                assert_eq!(answered, 1, "three connections are one broker");
                assert_eq!(silent.iter().map(|b| b.id).collect::<Vec<_>>(), [2, 3]);
            }
            other => panic!("three connections to one broker are not three brokers: {other:?}"),
        }

        // A broker the cluster does not list is not asked for: the same two
        // answers are the whole of a two-broker cluster.
        let v = full_view(&read(&from(&[1, 3])), true, &cluster(&[1, 3]), &[])
            .expect("both listed brokers answered");
        assert_eq!(v.brokers(), 2);
    }

    /// Every bootstrap address must answer too: the operator named it. It
    /// answers on its own connection, or as the listed broker at that
    /// address.
    #[test]
    fn a_bootstrap_address_that_did_not_answer_is_a_broker_nobody_asked() {
        let listed = cluster(&[1, 2]);
        let both: Vec<(&'static str, String)> = [1, 2]
            .iter()
            .flat_map(|id| answer(&node(&format!("b{id}.example"), *id), &KAFKA))
            .collect();
        let bootstrap =
            |list: &[&str]| -> Vec<String> { list.iter().map(|s| s.to_string()).collect() };

        // The bootstrap addresses ARE the two brokers' (differently cased).
        let v = full_view(
            &read(&both),
            true,
            &listed,
            &bootstrap(&["B1.example:9092", "b2.example:9092"]),
        )
        .expect("each bootstrap address is a listed broker that answered");
        assert_eq!(v.brokers(), 2);

        // A third address: the broker behind it was stopped and the cluster
        // no longer lists it. Both listed brokers answered, and it is still
        // not a whole view.
        let partial = full_view(
            &read(&both),
            true,
            &listed,
            &bootstrap(&["b1.example:9092", "b2.example:9092", "b3.example:9092"]),
        )
        .expect_err("b3 was named and did not answer");
        assert_eq!(
            partial,
            Partial::NotEveryone {
                answered: 2,
                listed: listed.clone(),
                silent: vec![],
                silent_bootstrap: vec!["b3.example:9092".to_string()],
            }
        );
        assert!(
            partial
                .to_string()
                .contains("no answer from the bootstrap address(es) b3.example:9092"),
            "{partial}"
        );

        // A silent bootstrap address that is a silent LISTED broker's is named
        // once, as the broker.
        let only_one: Vec<(&'static str, String)> = answer(&node("b1.example", 1), &KAFKA);
        assert_eq!(
            full_view(
                &read(&only_one),
                true,
                &listed,
                &bootstrap(&["b1.example:9092", "b2.example:9092"]),
            ),
            Err(Partial::NotEveryone {
                answered: 1,
                listed: listed.clone(),
                silent: vec![Broker::new(2, "b2.example", 9092)],
                silent_bootstrap: vec![],
            })
        );

        // An address that is no listed broker's (a load balancer) answers on
        // its own bootstrap connection.
        let mut with_lb = both.clone();
        with_lb.extend(answer(
            "[thrd:lb.example:9092/bootstrap]: lb.example:9092/bootstrap: ",
            &KAFKA,
        ));
        let v = full_view(
            &read(&with_lb),
            true,
            &listed,
            &bootstrap(&["lb.example:9092"]),
        )
        .expect("the load balancer answered, and so did both brokers");
        assert_eq!(v.brokers(), 2, "an address is not a broker");
    }

    /// Brokers that serve different ranges: the row is judged on the WEAKEST
    /// answer (only what every one of them serves), and the view says they
    /// differ.
    #[test]
    fn brokers_that_differ_are_judged_on_the_weakest_answer() {
        let listed = cluster(&[1, 2]);
        let mut lines = answer(
            &node("b1.example", 1),
            &[
                "Produce (0) Versions 0..13",
                "Fetch (1) Versions 4..18",
                "ListGroups (16) Versions 0..4",
            ],
        );
        // Broker 2 is mid-upgrade: Produce stops at v7 and it lists no Fetch.
        lines.extend(answer(
            &node("b2.example", 2),
            &["Produce (0) Versions 3..7", "ListGroups (16) Versions 0..5"],
        ));
        let v = full_view(&read(&lines), true, &listed, &[]).expect("both answered");
        assert_eq!(v.brokers(), 2);
        assert!(v.differ(), "the two answers are not the same");
        assert_eq!(v.range(0), Some((3, 7)));
        assert!(
            !v.serves(0, 8),
            "one broker serves Produce v8 and one does not"
        );
        assert_eq!(v.range(1), None, "Fetch is missing from one answer");
        assert_eq!(v.range(16), Some((0, 4)));

        // CONTROL: the same two brokers with the same answer do not differ.
        let same: Vec<(&'static str, String)> = [1, 2]
            .iter()
            .flat_map(|id| answer(&node(&format!("b{id}.example"), *id), &KAFKA))
            .collect();
        assert!(!full_view(&read(&same), true, &listed, &[])
            .unwrap()
            .differ());
    }

    /// A reconnect logs a new answer for the same connection: the new one
    /// replaces the old one, it is not merged with it.
    #[test]
    fn a_connections_later_answer_replaces_its_earlier_one() {
        let mut lines = answer(
            NODE,
            &["Produce (0) Versions 0..7", "Fetch (1) Versions 4..13"],
        );
        lines.extend(answer(NODE, &["Produce (0) Versions 0..13"]));
        let v = full_view(
            &read(&lines),
            true,
            &[Broker::new(1001, "broker.example", 9092)],
            &[],
        )
        .expect("an answer");
        assert_eq!(v.brokers(), 1);
        assert_eq!(v.range(0), Some((0, 13)));
        assert_eq!(v.range(1), None, "the later answer does not list Fetch");
    }

    /// NOT OBSERVED has a reason, in each of its shapes; it is never an empty
    /// range set that would read as "nothing is served", and never the other
    /// connections' answer.
    #[test]
    fn nothing_observed_is_a_reason_never_an_answer() {
        let listed = [Broker::new(1001, "broker.example", 9092)];
        let view = |lines: &[(&'static str, String)], complete: bool| {
            full_view(&read(lines), complete, &listed, &[])
        };
        assert_eq!(view(&[], true), Err(Partial::NothingAnswered));
        assert_eq!(
            view(
                &[("FEATURE", "Updated enabled protocol features".to_string())],
                true
            ),
            Err(Partial::NothingAnswered)
        );
        // The same lines under another facility are not an answer.
        assert_eq!(
            view(
                &[(
                    "BROKER",
                    format!("{NODE}  ApiKey Produce (0) Versions 0..7")
                )],
                true
            ),
            Err(Partial::NothingAnswered)
        );

        // A header with no entry after it: an answer was opened and could not
        // be read. It is COUNTED, not dropped.
        let unread = Partial::Unreadable {
            connections: vec!["broker.example:9092/1001".to_string()],
        };
        assert_eq!(
            view(&[line(NODE, "Broker API support:")], true),
            Err(unread)
        );

        // The review's probe: one connection answered whole and a second one
        // opened an answer with no readable entry. The first is not the
        // cluster's answer.
        let mut one_whole_one_not = answer(NODE, &KAFKA);
        one_whole_one_not.push(line(BOOT, "Broker API support:"));
        let got = view(&one_whole_one_not, true).expect_err("the second connection's answer");
        assert_eq!(
            got,
            Partial::Unreadable {
                connections: vec!["broker.example:9092/bootstrap".to_string()],
            }
        );
        assert!(
            got.to_string().contains("a defect in Logweir"),
            "the reader, not the endpoint, is what to look at: {got}"
        );

        // A read that was cut short: the lines are there, the answer is not.
        let whole = answer(NODE, &KAFKA);
        assert!(view(&whole, true).is_ok());
        assert_eq!(view(&whole, false), Err(Partial::StillLogging));

        // The answers are there and the broker list is not: nobody knows
        // whether these are all the brokers.
        assert_eq!(
            full_view(&read(&whole), true, &[], &[]),
            Err(Partial::NoBrokerList)
        );
    }

    /// **The wait** ([`whole_view_within`]), driven by a reader the test
    /// holds.
    ///
    /// * A log that NEVER GOES QUIET is not an answer, although every broker's
    ///   lines are in it: some answer may be cut short (review L4; the mutant
    ///   is passing `true` where the read's own `quiet` goes).
    /// * CONTROL: the same lines from a log that went quiet are the answer.
    /// * Answers that arrive over several reads are waited for.
    /// * With the budget already spent, exactly one read still runs, and a
    ///   partial view is the reason, never the answer.
    /// * A log that cannot be read is a reason too.
    #[test]
    fn the_wait_takes_a_whole_quiet_view_or_gives_the_reason() {
        use std::time::{Duration, Instant};
        let listed = cluster(&[1, 2, 3]);
        let owned = |ids: &[i32]| -> Vec<(String, String)> {
            ids.iter()
                .flat_map(|id| answer(&node(&format!("b{id}.example"), *id), &KAFKA))
                .map(|(f, m)| (f.to_string(), m))
                .collect()
        };
        let quiet = Duration::from_millis(20);
        let soon = || Instant::now() + Duration::from_millis(200);

        // Never quiet.
        let mut reads = 0;
        let never_quiet = whole_view_within(
            |_| {
                reads += 1;
                std::thread::sleep(Duration::from_millis(10));
                Ok((
                    if reads == 1 {
                        owned(&[1, 2, 3])
                    } else {
                        vec![]
                    },
                    false,
                ))
            },
            &listed,
            &[],
            soon(),
            quiet,
        );
        assert_eq!(
            never_quiet,
            Err(Partial::StillLogging.to_string()),
            "all three answers are in the log, and it was still being written"
        );
        assert!(reads > 1, "it kept reading until its budget was spent");

        // CONTROL: quiet.
        let v = whole_view_within(
            |_| Ok((owned(&[1, 2, 3]), true)),
            &listed,
            &[],
            soon(),
            quiet,
        )
        .expect("three answers in a quiet log");
        assert_eq!(v.brokers(), 3);

        // Arriving over several reads.
        let mut reads = 0;
        let v = whole_view_within(
            |_| {
                reads += 1;
                Ok(match reads {
                    1 => (owned(&[1]), true),
                    2 => (vec![], true),
                    _ => (owned(&[2, 3]), true),
                })
            },
            &listed,
            &[],
            Instant::now() + Duration::from_secs(5),
            quiet,
        )
        .expect("brokers 2 and 3 answered on the third read");
        assert_eq!((v.brokers(), reads), (3, 3));

        // The budget already spent: one read, and the partial view's reason.
        let mut reads = 0;
        let spent = whole_view_within(
            |_| {
                reads += 1;
                Ok((owned(&[1, 3]), true))
            },
            &listed,
            &[],
            Instant::now(),
            quiet,
        )
        .expect_err("broker 2 never answered");
        assert_eq!(reads, 1, "the last read always runs, and only it");
        assert!(
            spent.contains("2 of 3 broker(s)") && spent.contains("broker 2 (b2.example:9092)"),
            "{spent}"
        );

        // A log that cannot be read.
        let unread = whole_view_within(
            |_| Err("the queue refused".to_string()),
            &listed,
            &[],
            soon(),
            quiet,
        )
        .expect_err("no log, no view");
        assert!(
            unread.contains("could not be read: the queue refused"),
            "{unread}"
        );
    }

    #[test]
    fn a_bootstrap_list_is_read_as_librdkafka_reads_it() {
        assert_eq!(
            bootstrap_addresses(" a.example:9093 ,SASL_SSL://b.example:9094,c.example,[::1]:9092,[::1],A.example:9093,"),
            [
                "a.example:9093",
                "b.example:9094",
                "c.example:9092",
                "[::1]:9092",
            ]
        );
        assert!(bootstrap_addresses("").is_empty());
        // A bracketed literal in the list and librdkafka's log name for it
        // are one address.
        assert_eq!(address_key("[::1]:9092"), address_key("::1:9092"));
    }
}
