//! `rd_kafka_DescribeAcls` over every binding (a filter of ANY on every
//! field). Values only: the clamped resource types and operations (T10), the
//! CLUSTER resource librdkafka calls BROKER (T11), and the "0 bindings" that
//! hides a refusal (T9) are `logweir_kafka::acls`' decisions.
use crate::raw::{array, errstr_text, run, text};
use crate::{sys, CText, CallError};
use rdkafka::bindings as rd;
use rdkafka::client::{Client, ClientContext};
use std::os::raw::c_char;
use std::ptr::{self, NonNull};
use std::time::Duration;

/// One ACL binding, as librdkafka reported it. The four enums are the
/// integers librdkafka returned; librdkafka CLAMPS a resource type above
/// TransactionalId and an operation above IdempotentWrite to 0, Unknown
/// (`rdkafka_admin.c:5587-5646`, C9), so a 0 here is not the broker's value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclBinding {
    /// `rd_kafka_ResourceType_t`.
    pub resource_type: u32,
    /// The resource name.
    pub name: CText,
    /// `rd_kafka_ResourcePatternType_t`.
    pub pattern_type: u32,
    /// The principal, `User:…`.
    pub principal: CText,
    /// The host.
    pub host: CText,
    /// `rd_kafka_AclOperation_t`.
    pub operation: u32,
    /// `rd_kafka_AclPermissionType_t`.
    pub permission: u32,
}

/// The ANY filter, destroyed exactly once on drop.
struct Filter(NonNull<rd::rd_kafka_AclBindingFilter_t>);

impl Filter {
    fn any() -> Result<Filter, CallError> {
        use rd::{rd_kafka_AclOperation_t as Op, rd_kafka_AclPermissionType_t as Perm};
        use rd::{rd_kafka_ResourcePatternType_t as Pat, rd_kafka_ResourceType_t as Res};
        let mut errstr: [c_char; 512] = [0; 512];
        // SAFETY: every enum argument is a valid variant; the three string
        // arguments are NULL, which the filter constructor accepts as "any";
        // `errstr` is a writable buffer of exactly `errstr.len()` bytes. The
        // result is a new filter the caller owns, or NULL.
        let raw = unsafe {
            rd::rd_kafka_AclBindingFilter_new(
                Res::RD_KAFKA_RESOURCE_ANY,
                ptr::null(),
                Pat::RD_KAFKA_RESOURCE_PATTERN_ANY,
                ptr::null(),
                ptr::null(),
                Op::RD_KAFKA_ACL_OPERATION_ANY,
                Perm::RD_KAFKA_ACL_PERMISSION_TYPE_ANY,
                errstr.as_mut_ptr(),
                errstr.len(),
            )
        };
        NonNull::new(raw)
            .map(Filter)
            .ok_or_else(|| CallError::Options {
                code: -1,
                message: errstr_text(&errstr),
            })
    }
}

impl Drop for Filter {
    fn drop(&mut self) {
        // SAFETY: created by `rd_kafka_AclBindingFilter_new` and owned by this
        // guard alone. `rd_kafka_DescribeAcls` COPIES the filter into its
        // request (`rd_kafka_AclBindingFilter_copy`, `rdkafka_admin.c:5691-5692`),
        // so no request refers to this object, and this is its only destroy.
        unsafe { rd::rd_kafka_AclBinding_destroy(self.0.as_ptr()) }
    }
}

/// **Every ACL binding the broker returns to this principal.**
///
/// T9: librdkafka drops DescribeAcls' top-level error code, so "no authorizer"
/// and "not authorised" both come back here as `Ok(vec![])`
/// (`rdkafka_admin.c:5553-5560`, `:5660`). An empty answer means nothing until
/// `logweir_kafka::acls` has the two positive probes.
///
/// # Errors
///
/// [`CallError`] when the call as a whole failed.
pub fn describe_acls<C: ClientContext>(
    client: &Client<C>,
    timeout: Duration,
) -> Result<Vec<AclBinding>, CallError> {
    let filter = Filter::any()?;
    run(
        client,
        rd::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DESCRIBEACLS,
        rd::RD_KAFKA_EVENT_DESCRIBEACLS_RESULT,
        timeout,
        |_| Ok(()),
        |rk, options, queue| {
            // SAFETY: `rk` is the handle `client` borrows for the whole of
            // `run`; `options` and `queue` are live guards owned by `run`
            // (copied, and referenced by librdkafka, as for every call here);
            // `filter` is live until after `run` returns, and librdkafka copies
            // it into the request before this call returns.
            unsafe {
                rd::rd_kafka_DescribeAcls(rk, filter.0.as_ptr(), options.as_ptr(), queue.as_ptr())
            }
        },
        |event| {
            // SAFETY: `run` checked the event's type and that it carries no
            // call error; this accessor returns the event cast, or NULL.
            let result = unsafe { rd::rd_kafka_event_DescribeAcls_result(event.as_ptr()) };
            if result.is_null() {
                return Err(CallError::UnexpectedResult(
                    "not a DescribeAcls result".to_string(),
                ));
            }
            let mut n = 0usize;
            // SAFETY: `result` is a live DescribeAcls result; the accessor
            // writes the count and returns the binding array the event owns.
            let acls = unsafe { rd::rd_kafka_DescribeAcls_result_acls(result, &mut n) };
            // SAFETY: NULL or `n` binding pointers owned by `event`.
            let acls = unsafe { array(event, acls.cast_const(), n) };
            let mut out = Vec::with_capacity(acls.len());
            for &a in acls {
                if a.is_null() {
                    continue;
                }
                // SAFETY: `a` is a non-NULL binding owned by `event`. Its four
                // enums are read through the integer-typed declarations (T12);
                // its strings are copied while `event` is borrowed.
                let binding = unsafe {
                    AclBinding {
                        resource_type: sys::acl_resource_type(a),
                        name: text(event, rd::rd_kafka_AclBinding_name(a)),
                        pattern_type: sys::acl_pattern_type(a),
                        principal: text(event, rd::rd_kafka_AclBinding_principal(a)),
                        host: text(event, rd::rd_kafka_AclBinding_host(a)),
                        operation: sys::acl_operation(a),
                        permission: sys::acl_permission_type(a),
                    }
                };
                out.push(binding);
            }
            Ok(out)
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::code;
    use rdkafka::producer::Producer;

    #[test]
    fn describe_acls_is_bounded_without_a_broker() {
        let p = crate::test_support::offline_client();
        let t = Duration::from_secs(1);
        let started = std::time::Instant::now();
        let e = describe_acls(p.client(), t).expect_err("no broker");
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

    #[test]
    fn the_any_filter_is_built_and_destroyed() {
        for _ in 0..1000 {
            let f = Filter::any().expect("the ANY filter is valid");
            drop(f);
        }
    }
}
