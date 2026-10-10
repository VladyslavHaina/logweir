//! **T12: the librdkafka functions whose rdkafka-sys bindings return a Rust
//! enum, re-declared here returning the C integer they really return.**
//!
//! rdkafka-sys 4.10.0+2.12.1 binds `rd_kafka_resp_err_t` as a `#[repr(i32)]`
//! Rust enum whose variants stop at 118, then 129 and 130 (`bindings.rs:159-161`),
//! and the group-state, group-type, ACL-operation, resource-type,
//! pattern-type and permission-type enums as `#[repr(u32)]` Rust enums. A C
//! function returning any other value through those declarations produces an
//! invalid discriminant, which is undefined behaviour at the return itself,
//! before any code could check it. librdkafka passes broker error codes
//! through unchanged, and a newer broker or librdkafka can add enum members,
//! so the values are not bounded by the bindings' variants.
//!
//! Each declaration below names the SAME symbol (`link_name`) with a C integer
//! of the same size in place of the enum: a C enum whose values fit `int` is
//! `int`-sized on every target this workspace builds (x86_64 and aarch64,
//! Linux and macOS), and an array of such enums is an array of 4-byte
//! integers. Nothing else differs from the binding, so the call is
//! ABI-identical; only the Rust type of the result changes, from one that has
//! invalid values to one that has none.
//!
//! Functions whose bindings take or return only pointers, `c_int`, `usize` or
//! `i32` are called through `rdkafka::bindings` directly.
use rdkafka::bindings as rd;
use std::os::raw::{c_char, c_int, c_uint};

extern "C" {
    /// `rd_kafka_resp_err_t rd_kafka_error_code(const rd_kafka_error_t *)`.
    #[link_name = "rd_kafka_error_code"]
    pub(crate) fn error_code(error: *const rd::rd_kafka_error_t) -> c_int;

    /// `rd_kafka_resp_err_t rd_kafka_event_error(rd_kafka_event_t *)`.
    #[link_name = "rd_kafka_event_error"]
    pub(crate) fn event_error(event: *mut rd::rd_kafka_event_t) -> c_int;

    /// `rd_kafka_resp_err_t rd_kafka_AdminOptions_set_request_timeout(...)`.
    #[link_name = "rd_kafka_AdminOptions_set_request_timeout"]
    pub(crate) fn set_request_timeout(
        options: *mut rd::rd_kafka_AdminOptions_t,
        timeout_ms: c_int,
        errstr: *mut c_char,
        errstr_size: usize,
    ) -> c_int;

    /// `rd_kafka_resp_err_t rd_kafka_set_log_queue(rd_kafka_t *rk, rd_kafka_queue_t *rkqu)`.
    /// A NULL `rkqu` forwards the logs to the main queue (`rdkafka_queue.c:964-977`).
    #[link_name = "rd_kafka_set_log_queue"]
    pub(crate) fn set_log_queue(rk: *mut rd::rd_kafka_t, rkqu: *mut rd::rd_kafka_queue_t) -> c_int;

    /// `rd_kafka_resp_err_t rd_kafka_list_groups(rk, group, grplistp, timeout_ms)`.
    #[link_name = "rd_kafka_list_groups"]
    pub(crate) fn list_groups(
        rk: *mut rd::rd_kafka_t,
        group: *const c_char,
        grplistp: *mut *const rd::rd_kafka_group_list,
        timeout_ms: c_int,
    ) -> c_int;

    /// `rd_kafka_consumer_group_state_t rd_kafka_ConsumerGroupListing_state(...)`.
    #[link_name = "rd_kafka_ConsumerGroupListing_state"]
    pub(crate) fn listing_state(listing: *const rd::rd_kafka_ConsumerGroupListing_t) -> c_uint;

    /// `rd_kafka_consumer_group_type_t rd_kafka_ConsumerGroupListing_type(...)`.
    #[link_name = "rd_kafka_ConsumerGroupListing_type"]
    pub(crate) fn listing_type(listing: *const rd::rd_kafka_ConsumerGroupListing_t) -> c_uint;

    /// `rd_kafka_consumer_group_state_t rd_kafka_ConsumerGroupDescription_state(...)`.
    #[link_name = "rd_kafka_ConsumerGroupDescription_state"]
    pub(crate) fn description_state(
        description: *const rd::rd_kafka_ConsumerGroupDescription_t,
    ) -> c_uint;

    /// `rd_kafka_consumer_group_type_t rd_kafka_ConsumerGroupDescription_type(...)`.
    #[link_name = "rd_kafka_ConsumerGroupDescription_type"]
    pub(crate) fn description_type(
        description: *const rd::rd_kafka_ConsumerGroupDescription_t,
    ) -> c_uint;

    /// `const rd_kafka_AclOperation_t *rd_kafka_DescribeCluster_result_authorized_operations(result, size_t *cntp)`:
    /// an array of C enums, read as `c_int`. NULL means "not requested or not
    /// reported"; a non-NULL pointer with a count of 0 means "none"
    /// (`rd_kafka_AuthorizedOperations_parse`, `rdkafka_admin.c:7713-7749`).
    #[link_name = "rd_kafka_DescribeCluster_result_authorized_operations"]
    pub(crate) fn cluster_authorized_operations(
        result: *const rd::rd_kafka_DescribeCluster_result_t,
        cntp: *mut usize,
    ) -> *const c_int;

    /// `rd_kafka_ResourceType_t rd_kafka_AclBinding_restype(...)`.
    #[link_name = "rd_kafka_AclBinding_restype"]
    pub(crate) fn acl_resource_type(acl: *const rd::rd_kafka_AclBinding_t) -> c_uint;

    /// `rd_kafka_ResourcePatternType_t rd_kafka_AclBinding_resource_pattern_type(...)`.
    #[link_name = "rd_kafka_AclBinding_resource_pattern_type"]
    pub(crate) fn acl_pattern_type(acl: *const rd::rd_kafka_AclBinding_t) -> c_uint;

    /// `rd_kafka_AclOperation_t rd_kafka_AclBinding_operation(...)`.
    #[link_name = "rd_kafka_AclBinding_operation"]
    pub(crate) fn acl_operation(acl: *const rd::rd_kafka_AclBinding_t) -> c_uint;

    /// `rd_kafka_AclPermissionType_t rd_kafka_AclBinding_permission_type(...)`.
    #[link_name = "rd_kafka_AclBinding_permission_type"]
    pub(crate) fn acl_permission_type(acl: *const rd::rd_kafka_AclBinding_t) -> c_uint;
}
