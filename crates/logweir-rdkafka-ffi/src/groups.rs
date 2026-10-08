//! Consumer-group calls: the typed listing (`rd_kafka_ListConsumerGroups`),
//! the name listing of every group type (`rd_kafka_list_groups`), and the
//! description (`rd_kafka_DescribeConsumerGroups`). Values only: what a state,
//! a type or a code MEANS is `logweir_kafka::groups`' decision (PROD-04.0 §5,
//! §6 T2–T4, T6, T14).
use crate::raw::{self, array, raw_error, run, text, Event};
use crate::{code, sys, CText, CallError, RawError};
use rdkafka::bindings as rd;
use rdkafka::client::{Client, ClientContext};
use std::collections::BTreeSet;
use std::ffi::CString;
use std::os::raw::{c_char, c_int};
use std::ptr::{self, addr_of, NonNull};
use std::time::Duration;

/// The most group ids one description call accepts. librdkafka sends one
/// request per group (`rdkafka_admin.c:8801-8813`); a caller with more ids
/// splits them.
pub const MAX_DESCRIBE_GROUPS: usize = 1000;

/// One group of the typed listing, as librdkafka reported it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedGroup {
    /// The group id.
    pub group_id: CText,
    /// Whether the group's protocol type is empty (a "simple" group, made by
    /// commits alone).
    pub is_simple: bool,
    /// `rd_kafka_consumer_group_state_t`, as the integer librdkafka returned.
    pub state: u32,
    /// `rd_kafka_consumer_group_type_t`, as the integer librdkafka returned.
    /// `0` (Unknown) on a broker that serves ListGroups below v5.
    pub group_type: u32,
}

/// The typed listing: every group librdkafka kept, and one error per broker
/// that did not answer. librdkafka keeps only groups whose protocol type is
/// empty or `consumer` (`rdkafka_admin.c:7520-7527`, C7): share, streams and
/// other-protocol groups are NOT here, and nothing says so. A broker listing a
/// group more than once is possible before librdkafka 2.14.2 (T6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedListing {
    /// The groups, in librdkafka's order, duplicates included.
    pub groups: Vec<ListedGroup>,
    /// One error per broker whose answer is missing from `groups`.
    pub errors: Vec<RawError>,
}

/// **The typed group listing.** One ListGroups request (up to v5) per broker,
/// merged by librdkafka; every group type it can name, with no type filter.
///
/// # Errors
///
/// [`CallError`] when the call as a whole failed (no broker known within the
/// timeout, for one). A broker that failed while others answered is an entry
/// of [`TypedListing::errors`], not an `Err`.
pub fn list_consumer_groups<C: ClientContext>(
    client: &Client<C>,
    timeout: Duration,
) -> Result<TypedListing, CallError> {
    run(
        client,
        rd::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPS,
        rd::RD_KAFKA_EVENT_LISTCONSUMERGROUPS_RESULT,
        timeout,
        |_| Ok(()),
        |rk, options, queue| {
            // SAFETY: `rk` is the handle `client` borrows for the whole of
            // `run`; `options` and `queue` are live guards owned by `run`.
            // librdkafka copies the options into the request
            // (`rdkafka_admin.c:637-640`) and takes its own reference to the
            // queue (`rdkafka_queue.h:733`), so neither is referenced after
            // `run` destroys it.
            unsafe { rd::rd_kafka_ListConsumerGroups(rk, options.as_ptr(), queue.as_ptr()) }
        },
        read_typed_listing,
    )
}

fn read_typed_listing(event: &Event) -> Result<TypedListing, CallError> {
    // SAFETY: `run` checked that the event is a LISTCONSUMERGROUPS_RESULT
    // without a call error; this accessor then returns the event itself, cast
    // (`rdkafka_event.c:374-380`), or NULL on a type mismatch.
    let result = unsafe { rd::rd_kafka_event_ListConsumerGroups_result(event.as_ptr()) };
    if result.is_null() {
        return Err(CallError::UnexpectedResult(
            "not a ListConsumerGroups result".to_string(),
        ));
    }
    let (mut n_valid, mut n_errors) = (0usize, 0usize);
    // SAFETY: `result` is a live ListConsumerGroups result with no call error,
    // whose result list holds exactly the one element these accessors assert
    // (`rdkafka_admin.c:7648-7684`). Each writes its count and returns an
    // array the event owns; both are read below while `event` is borrowed.
    let (valid, errors) = unsafe {
        (
            rd::rd_kafka_ListConsumerGroups_result_valid(result, &mut n_valid),
            rd::rd_kafka_ListConsumerGroups_result_errors(result, &mut n_errors),
        )
    };
    // SAFETY: `valid` is NULL or an array of `n_valid` listing pointers owned
    // by `event`, borrowed for the slice's lifetime.
    let valid = unsafe { array(event, valid.cast_const(), n_valid) };
    // SAFETY: as above, for `n_errors` error pointers.
    let errors = unsafe { array(event, errors.cast_const(), n_errors) };
    let mut groups = Vec::with_capacity(valid.len());
    for &listing in valid {
        if listing.is_null() {
            continue;
        }
        // SAFETY: `listing` is a non-NULL listing owned by `event`. The id is a
        // string it owns, copied by `text`; state and type are read through the
        // integer-typed declarations (T12).
        let group = unsafe {
            ListedGroup {
                group_id: text(event, rd::rd_kafka_ConsumerGroupListing_group_id(listing)),
                is_simple: rd::rd_kafka_ConsumerGroupListing_is_simple_consumer_group(listing) != 0,
                state: sys::listing_state(listing),
                group_type: sys::listing_type(listing),
            }
        };
        groups.push(group);
    }
    let errors = errors
        .iter()
        // SAFETY: each pointer is NULL or an error object owned by `event`;
        // `raw_error` copies it without destroying it.
        .filter_map(|&e| unsafe { raw_error(event, e) })
        .collect();
    Ok(TypedListing { groups, errors })
}

/// One group of the name listing, as the legacy ListGroups v0 and
/// DescribeGroups v0 calls reported it. Only the fields below are read: the
/// member array is never touched (T1 is rust-rdkafka 0.36.2 building a slice
/// from it when it is NULL).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedGroup {
    /// The group id.
    pub group_id: CText,
    /// The per-group DescribeGroups error code (`0` for none), read as an
    /// integer (T12). rust-rdkafka's `GroupInfo` does not expose it (C2).
    pub error: i32,
    /// The classic describe's state text. A non-classic group, and an absent
    /// one, read `Dead` here (K2, T2): never read it for an unclassified group.
    pub state: CText,
    /// The classic describe's protocol type text (same caveat).
    pub protocol_type: CText,
    /// The broker that answered for the group.
    pub broker_id: i32,
}

/// The name listing: every group of every type that the brokers answering
/// listed (ListGroups lists all types, K1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NameListing {
    /// The groups, in librdkafka's order.
    pub groups: Vec<NamedGroup>,
    /// librdkafka answered `_PARTIAL`: some broker did not answer in time.
    pub partial: bool,
}

/// The legacy group list, destroyed exactly once on drop.
struct GroupList(NonNull<rd::rd_kafka_group_list>);

impl Drop for GroupList {
    fn drop(&mut self) {
        // SAFETY: `rd_kafka_list_groups` handed this list to the caller (it
        // sets `*grplistp` only on success or `_PARTIAL`, `rdkafka.c:5214-5226`)
        // to free with `rd_kafka_group_list_destroy`; this guard is its only
        // owner, so it is destroyed exactly once, after every read (the reads
        // borrow `self`).
        unsafe { rd::rd_kafka_group_list_destroy(self.0.as_ptr()) }
    }
}

/// **The name listing of every group type** (`rd_kafka_list_groups`, the call
/// rust-rdkafka's `fetch_group_list` wraps, made here so no `GroupInfo` is
/// ever built: T1, C2, and its `expect` on non-UTF-8 text).
///
/// librdkafka's own merging of broker answers keeps the LAST broker's error
/// (`state->err = err` at the end of both callbacks, `rdkafka.c:5028` and
/// `:5119`): on a cluster of several brokers, a broker that failed can be
/// followed by one that answered, and the listing then reads as complete
/// without that broker's groups (trap T19, PROD-04.0b). The caller must not
/// treat this listing alone as complete; `logweir_kafka::groups` cross-checks
/// it against the typed listing.
///
/// # Errors
///
/// [`CallError::Call`] with librdkafka's code when no listing came back (for
/// example `_TIMED_OUT` with no broker known). `_PARTIAL` is not an error: it
/// is [`NameListing::partial`].
pub fn list_group_names<C: ClientContext>(
    client: &Client<C>,
    timeout: Duration,
) -> Result<NameListing, CallError> {
    let ms = raw::timeout_ms(timeout)?;
    let mut list: *const rd::rd_kafka_group_list = ptr::null();
    // SAFETY: `client.native_ptr()` is the live handle `client` borrows for this
    // whole call; a NULL `group` asks for every group; `&mut list` is a valid
    // out-pointer that librdkafka writes only on success or `_PARTIAL`; the
    // call blocks at most `ms` (bounded, positive). The return value is read as
    // an integer (T12).
    let rc = unsafe { sys::list_groups(client.native_ptr(), ptr::null(), &mut list, ms) };
    if rc != 0 && rc != code::PARTIAL {
        // On any other code librdkafka destroyed the list itself and did not
        // set `list` (`rdkafka.c:5222-5223`). It is NOT freed here: freeing a
        // list librdkafka already freed would be a double free.
        return Err(CallError::Call {
            code: rc,
            message: CText::Utf8(format!("rd_kafka_list_groups answered {rc}")),
        });
    }
    let owner = GroupList(NonNull::new(list.cast_mut()).ok_or_else(|| {
        CallError::UnexpectedResult(format!("rd_kafka_list_groups answered {rc} without a list"))
    })?);
    // SAFETY: the list is live (owned by `owner`). Its two fields are read
    // through raw places; the struct holds no enum field.
    let (infos, count) = unsafe {
        let l = owner.0.as_ptr();
        (
            addr_of!((*l).groups).read(),
            addr_of!((*l).group_cnt).read(),
        )
    };
    let count = usize::try_from(count).unwrap_or(0);
    let mut groups = Vec::with_capacity(count);
    for i in 0..count {
        if infos.is_null() {
            break;
        }
        // SAFETY: `infos` points to `count` initialised group infos owned by
        // `owner` (librdkafka's `group_cnt` invariant), and `i < count`. Every
        // field is read through a raw place: `rd_kafka_group_info` holds `err`,
        // a Rust enum in the binding (T12), so no reference to the struct is
        // ever formed, and `err` is read as the C int it is. `members` is never
        // read (T1). The strings are owned by the list and copied by `text`.
        let group = unsafe {
            let gi = infos.add(i);
            NamedGroup {
                group_id: text(&owner, addr_of!((*gi).group).read()),
                error: addr_of!((*gi).err).cast::<c_int>().read(),
                state: text(&owner, addr_of!((*gi).state).read()),
                protocol_type: text(&owner, addr_of!((*gi).protocol_type).read()),
                broker_id: addr_of!((*gi).broker.id).read(),
            }
        };
        groups.push(group);
    }
    drop(owner);
    Ok(NameListing {
        groups,
        partial: rc == code::PARTIAL,
    })
}

/// One partition of a member's assignment.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AssignedPartition {
    /// The topic.
    pub topic: CText,
    /// The partition.
    pub partition: i32,
}

/// One member of a described group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMember {
    /// The member's `client.id`.
    pub client_id: CText,
    /// The member id the coordinator gave it.
    pub consumer_id: CText,
    /// Its `group.instance.id`, for a static member.
    pub group_instance_id: CText,
    /// The host it connected from.
    pub host: CText,
    /// Its current assignment.
    pub assignment: Vec<AssignedPartition>,
    /// Its target assignment: only a consumer (KIP-848) group has one.
    pub target_assignment: Option<Vec<AssignedPartition>>,
}

/// One described group, as librdkafka reported it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedGroup {
    /// The group id.
    pub group_id: CText,
    /// The per-group error, if the coordinator lookup or the describe failed
    /// for this group (30 for a principal that may not Describe it).
    pub error: Option<RawError>,
    /// Whether the protocol type is empty.
    pub is_simple: bool,
    /// The partition assignor.
    pub partition_assignor: CText,
    /// `rd_kafka_consumer_group_state_t`, as the integer librdkafka returned.
    pub state: u32,
    /// `rd_kafka_consumer_group_type_t`, as the integer librdkafka returned.
    /// The classic describe librdkafka falls back to answers every
    /// non-classic group, and every absent id, "simple, Classic, Dead" with no
    /// error (K2, T2).
    pub group_type: u32,
    /// The coordinator's broker id, when librdkafka names one.
    pub coordinator: Option<i32>,
    /// The members, in librdkafka's order.
    pub members: Vec<GroupMember>,
}

/// The ids a description call sends, refused before anything is sent when
/// empty, too many, blank, carrying a NUL, or repeated (librdkafka refuses a
/// repeated id with an error for the whole call, `rdkafka_admin.c:8773-8791`).
pub fn describe_request(groups: &[&str]) -> Result<Vec<CString>, CallError> {
    if groups.is_empty() {
        return Err(CallError::InvalidInput(
            "no group ids to describe".to_string(),
        ));
    }
    if groups.len() > MAX_DESCRIBE_GROUPS {
        return Err(CallError::InvalidInput(format!(
            "{} group ids in one description; at most {MAX_DESCRIBE_GROUPS}",
            groups.len()
        )));
    }
    let mut seen = BTreeSet::new();
    let mut out = Vec::with_capacity(groups.len());
    for g in groups {
        if g.trim().is_empty() {
            return Err(CallError::InvalidInput(
                "a group id is never blank".to_string(),
            ));
        }
        if !seen.insert(*g) {
            return Err(CallError::InvalidInput(format!("{g:?} appears twice")));
        }
        out.push(
            CString::new(*g)
                .map_err(|_| CallError::InvalidInput(format!("{g:?} carries a NUL byte")))?,
        );
    }
    Ok(out)
}

/// **Describes the named groups** (`rd_kafka_DescribeConsumerGroups`):
/// ConsumerGroupDescribe for each id, falling back to the classic
/// DescribeGroups (v4 at most) where the broker answers GROUP_ID_NOT_FOUND,
/// UNSUPPORTED_VERSION or unsupported feature (`rdkafka_admin.c:8630-8636`).
/// One entry per id, in request order (`:8720-8734`).
///
/// # Errors
///
/// [`CallError::InvalidInput`] per [`describe_request`], before anything is
/// sent; otherwise when the call as a whole failed. A group librdkafka could
/// not describe is an entry whose [`DescribedGroup::error`] is set.
pub fn describe_consumer_groups<C: ClientContext>(
    client: &Client<C>,
    groups: &[&str],
    timeout: Duration,
) -> Result<Vec<DescribedGroup>, CallError> {
    let ids = describe_request(groups)?;
    let mut pointers: Vec<*const c_char> = ids.iter().map(|c| c.as_ptr()).collect();
    run(
        client,
        rd::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DESCRIBECONSUMERGROUPS,
        rd::RD_KAFKA_EVENT_DESCRIBECONSUMERGROUPS_RESULT,
        timeout,
        |_| Ok(()),
        |rk, options, queue| {
            // SAFETY: `rk` is the handle `client` borrows for the whole of
            // `run`; `options` and `queue` are live guards (copied and
            // referenced by librdkafka as in `list_consumer_groups`).
            // `pointers` holds `pointers.len()` pointers into `ids`, which are
            // NUL-terminated and outlive this call; librdkafka copies every id
            // (`rd_strdup`, `rdkafka_admin.c:8765-8771`) and never writes
            // through the array.
            unsafe {
                rd::rd_kafka_DescribeConsumerGroups(
                    rk,
                    pointers.as_mut_ptr(),
                    pointers.len(),
                    options.as_ptr(),
                    queue.as_ptr(),
                )
            }
        },
        read_descriptions,
    )
}

fn read_descriptions(event: &Event) -> Result<Vec<DescribedGroup>, CallError> {
    // SAFETY: `run` checked the event's type and that it carries no call
    // error; this accessor returns the event cast, or NULL on a mismatch.
    let result = unsafe { rd::rd_kafka_event_DescribeConsumerGroups_result(event.as_ptr()) };
    if result.is_null() {
        return Err(CallError::UnexpectedResult(
            "not a DescribeConsumerGroups result".to_string(),
        ));
    }
    let mut n = 0usize;
    // SAFETY: `result` is a live DescribeConsumerGroups result; the accessor
    // writes the count and returns the description array the event owns.
    let descriptions = unsafe { rd::rd_kafka_DescribeConsumerGroups_result_groups(result, &mut n) };
    // SAFETY: NULL or `n` description pointers owned by `event`.
    let descriptions = unsafe { array(event, descriptions.cast_const(), n) };
    let mut out = Vec::with_capacity(descriptions.len());
    for &d in descriptions {
        if d.is_null() {
            continue;
        }
        // SAFETY: `d` is a non-NULL description owned by `event`; every
        // accessor below only reads it. Strings and the error are copied (the
        // error is owned by the description and not destroyed here); state and
        // type are read as integers (T12); the coordinator node is read only
        // when non-NULL.
        let mut group = unsafe {
            let node = rd::rd_kafka_ConsumerGroupDescription_coordinator(d);
            DescribedGroup {
                group_id: text(event, rd::rd_kafka_ConsumerGroupDescription_group_id(d)),
                error: raw_error(event, rd::rd_kafka_ConsumerGroupDescription_error(d)),
                is_simple: rd::rd_kafka_ConsumerGroupDescription_is_simple_consumer_group(d) != 0,
                partition_assignor: text(
                    event,
                    rd::rd_kafka_ConsumerGroupDescription_partition_assignor(d),
                ),
                state: sys::description_state(d),
                group_type: sys::description_type(d),
                coordinator: (!node.is_null()).then(|| rd::rd_kafka_Node_id(node)),
                members: Vec::new(),
            }
        };
        // SAFETY: `d` is live (owned by `event`); the accessor reads a count.
        let members = unsafe { rd::rd_kafka_ConsumerGroupDescription_member_count(d) };
        for m in 0..members {
            // SAFETY: `d` is live; `m` is below its member count, and the
            // accessor returns NULL rather than read past its list.
            let member = unsafe { rd::rd_kafka_ConsumerGroupDescription_member(d, m) };
            if member.is_null() {
                continue;
            }
            // SAFETY: `member` is a non-NULL member owned by `event`. Its
            // strings are copied; its assignments are read by `partitions`,
            // which reads only the topic and partition of each element.
            let member = unsafe {
                let target = rd::rd_kafka_MemberDescription_target_assignment(member);
                GroupMember {
                    client_id: text(event, rd::rd_kafka_MemberDescription_client_id(member)),
                    consumer_id: text(event, rd::rd_kafka_MemberDescription_consumer_id(member)),
                    group_instance_id: text(
                        event,
                        rd::rd_kafka_MemberDescription_group_instance_id(member),
                    ),
                    host: text(event, rd::rd_kafka_MemberDescription_host(member)),
                    assignment: assignment(
                        event,
                        rd::rd_kafka_MemberDescription_assignment(member),
                    ),
                    target_assignment: (!target.is_null()).then(|| assignment(event, target)),
                }
            };
            group.members.push(member);
        }
        out.push(group);
    }
    Ok(out)
}

/// The partitions of a member assignment.
///
/// # Safety
///
/// `a` is NULL or a member assignment owned by `owner`, live while `owner` is
/// borrowed.
unsafe fn assignment(
    owner: &Event,
    a: *const rd::rd_kafka_MemberAssignment_t,
) -> Vec<AssignedPartition> {
    if a.is_null() {
        return Vec::new();
    }
    // SAFETY: `a` is a live assignment owned by `owner` (this function's
    // contract); the accessor returns its partition list, owned by it.
    let list = unsafe { rd::rd_kafka_MemberAssignment_partitions(a) };
    if list.is_null() {
        return Vec::new();
    }
    // SAFETY: `list` is a live `rd_kafka_topic_partition_list_t` owned by
    // `owner`; its `cnt` and `elems` fields are read through raw places (the
    // list struct holds no enum field).
    let (cnt, elems) = unsafe { (addr_of!((*list).cnt).read(), addr_of!((*list).elems).read()) };
    let cnt = usize::try_from(cnt).unwrap_or(0);
    if elems.is_null() {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(cnt);
    for i in 0..cnt {
        // SAFETY: `elems` points to `cnt` initialised elements owned by
        // `owner` (librdkafka's list invariant) and `i < cnt`. Only `topic` and
        // `partition` are read, through raw places: an element holds `err`, a
        // Rust enum in the binding (T12), so no reference to an element is
        // formed and `err` is never read. The topic string is copied.
        let p = unsafe {
            let e = elems.add(i);
            AssignedPartition {
                topic: text(owner, addr_of!((*e).topic).read()),
                partition: addr_of!((*e).partition).read(),
            }
        };
        out.push(p);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::offline_client;
    use rdkafka::producer::Producer;
    use std::time::Instant;

    #[test]
    fn a_description_request_that_cannot_mean_what_it_says_is_refused_before_sending() {
        assert!(matches!(
            describe_request(&[]),
            Err(CallError::InvalidInput(_))
        ));
        assert!(matches!(
            describe_request(&[" "]),
            Err(CallError::InvalidInput(_))
        ));
        assert!(matches!(
            describe_request(&["a", "b", "a"]),
            Err(CallError::InvalidInput(m)) if m.contains("twice")
        ));
        assert!(matches!(
            describe_request(&["a\0b"]),
            Err(CallError::InvalidInput(m)) if m.contains("NUL")
        ));
        let many: Vec<String> = (0..=MAX_DESCRIBE_GROUPS).map(|i| format!("g{i}")).collect();
        let many: Vec<&str> = many.iter().map(String::as_str).collect();
        assert!(matches!(
            describe_request(&many),
            Err(CallError::InvalidInput(_))
        ));
        let ok = describe_request(&["pa-orders", "pa-absent"]).expect("valid");
        assert_eq!(ok.len(), 2);
        assert_eq!(ok[1].as_bytes(), b"pa-absent");
    }

    /// Every call returns, within its timeout plus the margin, with
    /// librdkafka's `_TIMED_OUT` as an integer, when no broker is known.
    #[test]
    fn every_group_call_is_bounded_without_a_broker() {
        let p = offline_client();
        let client = p.client();
        let t = Duration::from_secs(1);
        let started = Instant::now();
        // MEASURED: with no broker the typed listing does NOT fail as a
        // whole. It answers with no group and ONE per-broker error, the
        // timeout: an `Ok` that is not a complete listing. So the safe side
        // must never read `errors` as optional (`logweir_kafka::groups`).
        let listing = list_consumer_groups(client, t).expect("an answer, with an error inside");
        assert!(listing.groups.is_empty(), "{listing:?}");
        assert_eq!(
            listing.errors.iter().map(|e| e.code).collect::<Vec<_>>(),
            vec![code::TIMED_OUT],
            "{listing:?}"
        );
        let how = describe_consumer_groups(client, &["pa-absent"], t).expect_ok_or_timeout();
        let e = list_group_names(client, t).expect_err("no broker");
        assert!(
            matches!(
                e,
                CallError::Call {
                    code: code::TIMED_OUT,
                    ..
                }
            ),
            "{e}"
        );
        let took = started.elapsed();
        assert!(
            took < 3 * (t + crate::POLL_MARGIN),
            "three bounded calls took {took:?} ({how})"
        );
    }

    trait OkOrTimeout {
        fn expect_ok_or_timeout(self) -> String;
    }

    impl OkOrTimeout for Result<Vec<DescribedGroup>, CallError> {
        /// A description with no broker answers either with a whole-call
        /// timeout or with one entry whose own error is a timeout: both are
        /// librdkafka's, and neither is "no error".
        fn expect_ok_or_timeout(self) -> String {
            match self {
                Err(CallError::Call { code, .. }) => {
                    assert_eq!(code, code::TIMED_OUT);
                    "call timed out".to_string()
                }
                Ok(groups) => {
                    assert_eq!(groups.len(), 1, "{groups:?}");
                    let err = groups[0].error.as_ref().expect("an error on the entry");
                    assert_eq!(err.code, code::TIMED_OUT, "{err:?}");
                    assert_eq!(groups[0].group_id, CText::Utf8("pa-absent".to_string()));
                    "entry timed out".to_string()
                }
                Err(other) => panic!("unexpected {other}"),
            }
        }
    }
}
