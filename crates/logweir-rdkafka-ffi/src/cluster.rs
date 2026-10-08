//! `rd_kafka_DescribeCluster` with the caller's authorized operations
//! (KIP-430): the probe that says whether a group listing is complete (T14)
//! and whether a principal may describe ACLs (T9's second probe).
use crate::raw::{array, run, text};
use crate::{sys, CText, CallError};
use rdkafka::bindings as rd;
use rdkafka::client::{Client, ClientContext};
use std::time::Duration;

/// The cluster as one DescribeCluster answered it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterDescription {
    /// The cluster id.
    pub cluster_id: CText,
    /// The controller's broker id, when the answer names one.
    pub controller: Option<i32>,
    /// Every broker id the answer lists.
    pub nodes: Vec<i32>,
    /// The CALLER's authorized operations on the cluster resource, as
    /// `rd_kafka_AclOperation_t` integers (Describe is 8).
    ///
    /// `None` means NOT REPORTED: the broker returned no set (KIP-430's
    /// `INT32_MIN`), which librdkafka turns into a NULL array
    /// (`rd_kafka_AuthorizedOperations_parse`, `rdkafka_admin.c:7719-7723`).
    /// `Some(vec![])` means the broker reported that the caller may do
    /// NOTHING on the cluster (librdkafka's non-NULL one-byte array, `:7732-7736`).
    /// The two must never be confused: the first is "unknown", the second a
    /// refusal.
    pub authorized_operations: Option<Vec<i32>>,
}

/// **Describes the cluster, asking for the caller's authorized operations.**
///
/// # Errors
///
/// [`CallError`] when the call as a whole failed.
pub fn describe_cluster<C: ClientContext>(
    client: &Client<C>,
    timeout: Duration,
) -> Result<ClusterDescription, CallError> {
    run(
        client,
        rd::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DESCRIBECLUSTER,
        rd::RD_KAFKA_EVENT_DESCRIBECLUSTER_RESULT,
        timeout,
        |options| options.include_authorized_operations(),
        |rk, options, queue| {
            // SAFETY: `rk` is the handle `client` borrows for the whole of
            // `run`; `options` and `queue` are live guards owned by `run`.
            // librdkafka copies the options into the request
            // (`rdkafka_admin.c:637-640`) and takes its own reference to the
            // queue (`rdkafka_queue.h:733`).
            unsafe { rd::rd_kafka_DescribeCluster(rk, options.as_ptr(), queue.as_ptr()) }
        },
        |event| {
            // SAFETY: `run` checked the event's type and that it carries no
            // call error; this accessor returns the event cast, or NULL on a
            // mismatch (`rdkafka_event.c:400-405`).
            let result = unsafe { rd::rd_kafka_event_DescribeCluster_result(event.as_ptr()) };
            if result.is_null() {
                return Err(CallError::UnexpectedResult(
                    "not a DescribeCluster result".to_string(),
                ));
            }
            let (mut n_nodes, mut n_ops) = (0usize, 0usize);
            // SAFETY: `result` is a live DescribeCluster result with no call
            // error, so its result list holds the one cluster description the
            // accessors assert (`rdkafka_admin.c:9315-9330`). Each accessor
            // reads the description; the arrays, the id string and the
            // controller node are owned by `event` and read below while it is
            // borrowed. The operations array is read as C ints (T12).
            let (cluster_id, controller, nodes, ops) = unsafe {
                let controller = rd::rd_kafka_DescribeCluster_result_controller(result);
                (
                    text(
                        event,
                        rd::rd_kafka_DescribeCluster_result_cluster_id(result),
                    ),
                    (!controller.is_null()).then(|| rd::rd_kafka_Node_id(controller)),
                    rd::rd_kafka_DescribeCluster_result_nodes(result, &mut n_nodes),
                    sys::cluster_authorized_operations(result, &mut n_ops),
                )
            };
            // SAFETY: NULL or `n_nodes` node pointers owned by `event`.
            let node_ptrs = unsafe { array(event, nodes.cast_const(), n_nodes) };
            let nodes = node_ptrs
                .iter()
                .filter(|n| !n.is_null())
                // SAFETY: each `n` is a non-NULL node owned by `event`; the
                // accessor reads its id.
                .map(|&n| unsafe { rd::rd_kafka_Node_id(n) })
                .collect();
            let authorized_operations = if ops.is_null() {
                None
            } else {
                // SAFETY: `ops` is non-NULL: an array of `n_ops` C enums
                // (4-byte, read as `c_int`) owned by `event`. With `n_ops` 0
                // it is librdkafka's one-byte placeholder, which `array` never
                // reads (an empty slice is returned for a zero length).
                Some(unsafe { array(event, ops, n_ops) }.to_vec())
            };
            Ok(ClusterDescription {
                cluster_id,
                controller,
                nodes,
                authorized_operations,
            })
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::code;
    use rdkafka::producer::Producer;

    #[test]
    fn describe_cluster_is_bounded_without_a_broker() {
        let p = crate::test_support::offline_client();
        let t = Duration::from_secs(1);
        let started = std::time::Instant::now();
        let e = describe_cluster(p.client(), t).expect_err("no broker");
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
        assert!(started.elapsed() < t + crate::POLL_MARGIN);
    }
}
