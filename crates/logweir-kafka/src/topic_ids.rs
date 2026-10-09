//! **PROD-01.4a: what a DescribeTopics answer MEANS.**
//!
//! `logweir-rdkafka-ffi::topics::describe_topics` hands back owned values and
//! interprets nothing (OD-6 (a2)). This module is where they become a
//! decision, pure and unit-tested with no broker, and available under
//! `--no-default-features`:
//!
//! | the answer for one topic | [`TopicIdRead`] |
//! |---|---|
//! | no error, a non-zero ID | `Id`, Kafka's text (`logweir_core::topic_identity::topic_id_text`) |
//! | no error, the all-zero ID | `NoId`: the broker has none (inter-broker protocol below 2.8) |
//! | `UNKNOWN_TOPIC_OR_PARTITION` (3) | `NotFound` |
//! | `TOPIC_AUTHORIZATION_FAILED` (29) | `NotAuthorized`: named, never read as "absent" |
//! | any other code | `Failed`, naming the code |
//! | no entry for a requested name | `Failed`: the broker did not answer for it |
//!
//! A WHOLE-CALL failure is a [`KafkaError`], never a per-topic verdict: no
//! answer within the bound, or librdkafka's transport codes, is
//! [`KafkaError::Unreachable`] — nothing about the topic is known, so it is
//! never `NotFound` or `NotAuthorized` ([`call_failure`]).
use crate::reader::KafkaError;
use logweir_core::topic_identity::{
    topic_id_text, IdRead, NOT_AUTHORIZED, NOT_READ, NO_TOPIC_ID, READ_FAILED, TOPIC_NOT_FOUND,
};

/// Kafka's `UNKNOWN_TOPIC_OR_PARTITION`.
pub const UNKNOWN_TOPIC_OR_PARTITION: i32 = 3;

/// Kafka's `TOPIC_AUTHORIZATION_FAILED`.
pub const TOPIC_AUTHORIZATION_FAILED: i32 = 29;

/// librdkafka's codes that mean no broker answered: `_TIMED_OUT` (-185),
/// `_TRANSPORT` (-195), `_ALL_BROKERS_DOWN` (-187), `_TIMED_OUT_QUEUE`
/// (-166) and `_RESOLVE` (-193). Each says nothing about the topic.
pub const TRANSPORT_CODES: [i32; 5] = [-185, -195, -187, -166, -193];

/// One topic's ID, as one DescribeTopics read established it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicIdRead {
    /// The topic's ID, in Kafka's canonical text.
    Id(String),
    /// The broker answered with the all-zero ID: it has no IDs to give.
    NoId,
    /// The broker does not hold the topic.
    NotFound,
    /// The principal may not Describe the topic.
    NotAuthorized,
    /// Any other failure for this topic, named.
    Failed(String),
    /// This reader does not read topic IDs at all.
    NotRead,
}

impl TopicIdRead {
    /// The receipt's side: the ID, or the closed-set reason there is none.
    #[must_use]
    pub fn to_id_read(&self) -> IdRead {
        match self {
            TopicIdRead::Id(id) => IdRead::Id(id.clone()),
            TopicIdRead::NoId => IdRead::Unread(NO_TOPIC_ID),
            TopicIdRead::NotFound => IdRead::Unread(TOPIC_NOT_FOUND),
            TopicIdRead::NotAuthorized => IdRead::Unread(NOT_AUTHORIZED),
            TopicIdRead::Failed(_) => IdRead::Unread(READ_FAILED),
            TopicIdRead::NotRead => IdRead::Unread(NOT_READ),
        }
    }
}

/// One topic of a DescribeTopics answer, as owned plain values: the name
/// (`None` when it was not UTF-8, which never names a requested topic), the
/// ID's two halves, and the per-topic error's code and message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicAnswer {
    /// The name the answer carries.
    pub name: Option<String>,
    /// The ID's halves; `None` when librdkafka gave no ID at all.
    pub halves: Option<(i64, i64)>,
    /// The per-topic error, as `(code, message)`.
    pub error: Option<(i32, String)>,
}

/// **One answer's verdict** (the table in the module doc). The error decides
/// first: an ID beside an error is whatever the broker sent and means nothing.
#[must_use]
pub fn classify(answer: &TopicAnswer) -> TopicIdRead {
    match &answer.error {
        Some((UNKNOWN_TOPIC_OR_PARTITION, _)) => TopicIdRead::NotFound,
        Some((TOPIC_AUTHORIZATION_FAILED, _)) => TopicIdRead::NotAuthorized,
        Some((code, message)) => TopicIdRead::Failed(format!(
            "DescribeTopics answered error {code} for the topic: {message}"
        )),
        None => match answer.halves {
            None => TopicIdRead::Failed(
                "DescribeTopics answered without an ID for the topic".to_string(),
            ),
            Some((most, least)) => {
                topic_id_text(most, least).map_or(TopicIdRead::NoId, TopicIdRead::Id)
            }
        },
    }
}

/// **Every requested name gets exactly one verdict**, in request order: the
/// answer for it, or `Failed` when the broker gave none (librdkafka leaves a
/// skipped name out of the result) or gave two. An answer for a name that was
/// not requested is ignored; librdkafka refuses such a response itself.
#[must_use]
pub fn join(requested: &[String], answers: &[TopicAnswer]) -> Vec<(String, TopicIdRead)> {
    requested
        .iter()
        .map(|name| {
            let mut matching = answers
                .iter()
                .filter(|a| a.name.as_deref() == Some(name.as_str()));
            let read = match (matching.next(), matching.next()) {
                (Some(answer), None) => classify(answer),
                (None, _) => TopicIdRead::Failed(
                    "DescribeTopics returned no answer for the topic".to_string(),
                ),
                (Some(_), Some(_)) => TopicIdRead::Failed(
                    "DescribeTopics answered for the topic more than once".to_string(),
                ),
            };
            (name.clone(), read)
        })
        .collect()
}

/// **A whole-call failure, named.** `code` is librdkafka's, `None` where the
/// failure carries none. No answer within the bound (`no_result`) and the
/// transport codes are [`KafkaError::Unreachable`]: a transport error is never
/// "not found" and never "not authorized". Anything else is
/// [`KafkaError::Client`].
#[must_use]
pub fn call_failure(code: Option<i32>, no_result: bool, message: &str) -> KafkaError {
    let what = format!("DescribeTopics: {message}");
    if no_result || code.is_some_and(|c| TRANSPORT_CODES.contains(&c)) {
        KafkaError::Unreachable(what)
    } else {
        KafkaError::Client(what)
    }
}

/// The names to send: each requested name once, in first-seen order (a plan
/// never repeats a topic, but librdkafka refuses a repeated name for the
/// whole call, so the request is made safe here).
#[must_use]
pub fn distinct(requested: &[String]) -> Vec<String> {
    let mut seen = std::collections::BTreeSet::new();
    requested
        .iter()
        .filter(|n| seen.insert(n.as_str()))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn answer(name: &str, halves: Option<(i64, i64)>, error: Option<i32>) -> TopicAnswer {
        TopicAnswer {
            name: Some(name.to_string()),
            halves,
            error: error.map(|c| (c, format!("code {c}"))),
        }
    }

    /// PROD-01.4's measured pair: librdkafka's halves and the broker CLI's text.
    const HALVES: (i64, i64) = (720_210_146_497_416_400, -6_285_085_649_756_852_793);
    const TEXT: &str = "Cf6zT_mcTNCoxuPmv1Ztxw";

    #[test]
    fn each_answer_gets_its_named_verdict() {
        assert_eq!(
            classify(&answer("t", Some(HALVES), None)),
            TopicIdRead::Id(TEXT.into())
        );
        assert_eq!(
            classify(&answer("t", Some((0, 0)), None)),
            TopicIdRead::NoId
        );
        assert_eq!(
            classify(&answer("t", Some((0, 0)), Some(3))),
            TopicIdRead::NotFound
        );
        assert_eq!(
            classify(&answer("t", Some((0, 0)), Some(29))),
            TopicIdRead::NotAuthorized
        );
        // An ID beside an error means nothing: the error decides.
        assert_eq!(
            classify(&answer("t", Some(HALVES), Some(29))),
            TopicIdRead::NotAuthorized
        );
        assert!(matches!(
            classify(&answer("t", Some(HALVES), Some(17))),
            TopicIdRead::Failed(m) if m.contains("error 17")
        ));
        assert!(matches!(
            classify(&answer("t", None, None)),
            TopicIdRead::Failed(m) if m.contains("without an ID")
        ));
    }

    #[test]
    fn the_receipt_side_is_the_closed_set() {
        assert_eq!(
            TopicIdRead::Id(TEXT.into()).to_id_read(),
            IdRead::Id(TEXT.into())
        );
        assert_eq!(TopicIdRead::NoId.to_id_read(), IdRead::Unread("noTopicId"));
        assert_eq!(
            TopicIdRead::NotFound.to_id_read(),
            IdRead::Unread("topicNotFound")
        );
        assert_eq!(
            TopicIdRead::NotAuthorized.to_id_read(),
            IdRead::Unread("notAuthorized")
        );
        assert_eq!(
            TopicIdRead::Failed("x".into()).to_id_read(),
            IdRead::Unread("readFailed")
        );
        assert_eq!(TopicIdRead::NotRead.to_id_read(), IdRead::Unread("notRead"));
    }

    #[test]
    fn every_requested_name_gets_one_verdict_and_a_skipped_name_is_never_guessed() {
        let requested = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let answers = vec![
            answer("c", Some((0, 0)), Some(3)),
            answer("a", Some(HALVES), None),
            // An answer for a name nobody asked for is ignored.
            answer("z", Some(HALVES), None),
            TopicAnswer {
                name: None,
                halves: Some(HALVES),
                error: None,
            },
        ];
        let got = join(&requested, &answers);
        assert_eq!(got.len(), 3);
        assert_eq!(got[0], ("a".into(), TopicIdRead::Id(TEXT.into())));
        assert!(
            matches!(&got[1], (n, TopicIdRead::Failed(m)) if n == "b" && m.contains("no answer"))
        );
        assert_eq!(got[2], ("c".into(), TopicIdRead::NotFound));
        let twice = vec![
            answer("a", Some(HALVES), None),
            answer("a", Some(HALVES), None),
        ];
        assert!(matches!(
            &join(&["a".to_string()], &twice)[0].1,
            TopicIdRead::Failed(m) if m.contains("more than once")
        ));
    }

    #[test]
    fn a_transport_failure_is_unreachable_never_not_found() {
        for code in TRANSPORT_CODES {
            assert!(
                matches!(
                    call_failure(Some(code), false, "x"),
                    KafkaError::Unreachable(_)
                ),
                "{code}"
            );
        }
        assert!(matches!(
            call_failure(None, true, "no result within 20s"),
            KafkaError::Unreachable(_)
        ));
        // `_INVALID_ARG` is this process's request, not the network.
        assert!(matches!(
            call_failure(Some(-186), false, "x"),
            KafkaError::Client(_)
        ));
        assert!(matches!(
            call_failure(None, false, "refused before sending"),
            KafkaError::Client(_)
        ));
    }

    #[test]
    fn a_repeated_name_is_sent_once() {
        let r = ["a", "b", "a"].map(String::from);
        assert_eq!(distinct(&r), vec!["a".to_string(), "b".to_string()]);
    }
}
