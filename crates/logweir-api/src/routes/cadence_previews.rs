//! Cadence previews (PLAT-04.2, D1 §4.4): what a cron expression — or a
//! preset — will actually do, in a time zone, before anything is saved.
//!
//! COMPUTED IN RUST AND NOWHERE ELSE. The browser never evaluates cron: a
//! second implementation is a second answer, and the one that matters is the
//! controller's. This route calls `weirkeeper::cadence`, the SAME module the
//! scheduler calls, so a preview and the slot that later fires cannot
//! disagree. `BackupSchedule.status.nextRuns` is the same shape from the same
//! function, for a schedule that already exists.
//!
//! NO CLUSTER READ, AND NO NAMESPACE. A draft cadence is not an object. This
//! route asks Kubernetes nothing — `tests/cadence_previews.rs` asserts the
//! adapter recorded no request at all — so there is no namespace to scope it
//! to and no cursor, no idempotency key and no audit object. It is D1 §4.4's
//! exact path for that reason.
//!
//! AUTHORIZATION IS STILL REAL. D1 §4.4 says "viewer role and above"; with no
//! namespace in the path, that is "this actor may read schedules SOMEWHERE".
//! An actor with no such grant is `403 forbidden` and gets no evaluator: the
//! answer is cheap, but it is still a product feature and not a public
//! calculator.

use axum::extract::State;
use axum::response::Response;
use http::{StatusCode, Uri};
use weirkeeper::cadence::presets::{self, Preset};
use weirkeeper::cadence::{Cadence, CadenceError, MAX_PREVIEW_COUNT, TZDB_SOURCE};

use super::json;
use crate::app::AppState;
use crate::audit::Decision;
use crate::auth::Actor;
use crate::authz::Action;
use crate::contract::{CadenceAdjustment, CadencePreviewResponse, NextRunView};
use crate::http::RequestId;
use crate::problem::{ApiError, FieldError, ProblemCode};
use crate::projection::preset_view;

/// The route identifier.
pub const ROUTE: &str = "GET /api/v1/cadence-previews";

/// D1 §4.4's default number of firings.
pub const DEFAULT_COUNT: usize = weirkeeper::cadence::DEFAULT_PREVIEW_COUNT;

/// Every query parameter this route accepts. The preset parameters are named
/// individually so an unknown one is `malformed_request` rather than ignored.
const PARAMETERS: [&str; 10] = [
    "schedule",
    "preset",
    "minute",
    "hour",
    "dayOfWeek",
    "dayOfMonth",
    "n",
    "timeZone",
    "count",
    "after",
];

/// Whether `actor` may read schedules in ANY namespace it is bound to.
fn authorize_any(state: &AppState, actor: &Actor) -> Result<(), ApiError> {
    let authorizer = state.authorizer();
    let allowed = authorizer
        .namespaces(actor)
        .iter()
        .any(|ns| authorizer.allows(actor, ns, Action::ReadSchedules));
    if allowed {
        actor
            .audit
            .set_action(Action::ReadSchedules.name(), Decision::Allow);
        return Ok(());
    }
    actor
        .audit
        .set_action(Action::ReadSchedules.name(), Decision::Deny);
    actor.audit.set_failure("forbidden");
    Err(ApiError::new(
        ProblemCode::Forbidden,
        "Previewing a cadence requires permission to read schedules in at least one granted \
         namespace.",
    ))
}

/// One preset parameter, read and range-checked against the CATALOGUE rather
/// than against a second copy of the ranges.
fn preset_parameter(
    kind: &str,
    name: &str,
    params: &std::collections::BTreeMap<String, String>,
    errors: &mut Vec<FieldError>,
) -> u32 {
    let Some(raw) = params.get(name) else {
        errors.push(FieldError::new(
            name,
            "required",
            format!("preset {kind} requires {name}"),
        ));
        return 0;
    };
    match raw.parse::<u32>() {
        Ok(value) => value,
        Err(_) => {
            errors.push(FieldError::new(
                name,
                "not_an_integer",
                format!("{name} must be a non-negative integer"),
            ));
            0
        }
    }
}

/// Build the preset named by `preset=` from the query, refusing a parameter
/// that belongs to a DIFFERENT preset.
fn preset_from_query(
    kind: &str,
    params: &std::collections::BTreeMap<String, String>,
) -> Result<Preset, ApiError> {
    let Some(spec) = presets::catalogue().iter().find(|s| s.kind == kind) else {
        return Err(ApiError::validation(vec![FieldError::new(
            "preset",
            "unknown_preset",
            format!(
                "preset must be one of {}",
                presets::catalogue()
                    .iter()
                    .map(|s| s.kind)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        )]));
    };
    let mut errors = Vec::new();
    // A PARAMETER THAT IS NOT THIS PRESET'S IS AN ERROR, NOT A LEFTOVER.
    // `preset=daily&dayOfWeek=3` is a form that lost track of which shape it
    // is on, and answering it with Monday-to-Sunday-inclusive daily previews
    // would hide that.
    for name in ["minute", "hour", "dayOfWeek", "dayOfMonth", "n"] {
        if params.contains_key(name) && !spec.parameters.iter().any(|p| p.name == name) {
            errors.push(FieldError::new(
                name,
                "not_a_parameter_of_this_preset",
                format!("preset {kind} does not take {name}"),
            ));
        }
    }
    let get =
        |name: &str, errors: &mut Vec<FieldError>| preset_parameter(kind, name, params, errors);
    let preset = match kind {
        "hourly" => Preset::Hourly {
            minute: get("minute", &mut errors),
        },
        "everyNHours" => Preset::EveryNHours {
            n: get("n", &mut errors),
            minute: get("minute", &mut errors),
        },
        "daily" => Preset::Daily {
            hour: get("hour", &mut errors),
            minute: get("minute", &mut errors),
        },
        "weekly" => Preset::Weekly {
            day_of_week: get("dayOfWeek", &mut errors),
            hour: get("hour", &mut errors),
            minute: get("minute", &mut errors),
        },
        "monthly" => Preset::Monthly {
            day_of_month: get("dayOfMonth", &mut errors),
            hour: get("hour", &mut errors),
            minute: get("minute", &mut errors),
        },
        _ => unreachable!("the catalogue lookup above already refused an unknown kind"),
    };
    if !errors.is_empty() {
        return Err(ApiError::validation(errors));
    }
    // THE CATALOGUE VALIDATES ITS OWN RANGES. `compile` is the single place
    // that knows `n ∈ {2,3,4,6,8,12}` and `dayOfMonth ≤ 28`; repeating those
    // bounds here would be the second copy D1 §4.2 exists to avoid.
    presets::compile(&preset).map_err(|e| {
        let presets::PresetError::OutOfRange { parameter, .. } = &e;
        ApiError::validation(vec![FieldError::new(
            *parameter,
            "out_of_range",
            e.to_string(),
        )])
    })?;
    Ok(preset)
}

/// `GET /api/v1/cadence-previews`.
pub async fn preview(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize_any(&state, &actor)?;
    let params = crate::http::parse_query(uri.query(), &PARAMETERS)?;

    // Exactly one source of the expression.
    let (schedule, preset) = match (params.get("schedule"), params.get("preset")) {
        (Some(_), Some(_)) | (None, None) => {
            return Err(ApiError::validation(vec![FieldError::new(
                "schedule",
                "required",
                "send exactly one of schedule=<cron> or preset=<kind> with its parameters",
            )]))
        }
        (Some(raw), None) => {
            if let Err(code) = crate::validate::check_single_line(raw, 128) {
                return Err(ApiError::validation(vec![FieldError::new(
                    "schedule",
                    code,
                    "a cron expression is required",
                )]));
            }
            (raw.clone(), presets::match_preset(raw))
        }
        (None, Some(kind)) => {
            let preset = preset_from_query(kind, &params)?;
            let compiled = presets::compile(&preset).expect("the preset was range-checked");
            (compiled, Some(preset))
        }
    };

    let count = match params.get("count") {
        None => DEFAULT_COUNT,
        Some(raw) => match raw.parse::<usize>() {
            Ok(n) if (1..=MAX_PREVIEW_COUNT).contains(&n) => n,
            _ => {
                return Err(ApiError::validation(vec![FieldError::new(
                    "count",
                    "out_of_range",
                    format!("count must be an integer from 1 to {MAX_PREVIEW_COUNT}"),
                )]))
            }
        },
    };
    let after = match params.get("after") {
        None => state.now(),
        Some(raw) => match chrono::DateTime::parse_from_rfc3339(raw) {
            Ok(t) => t.with_timezone(&chrono::Utc),
            Err(_) => {
                return Err(ApiError::validation(vec![FieldError::new(
                    "after",
                    "invalid_timestamp",
                    "after must be an RFC 3339 instant",
                )]))
            }
        },
    };

    // The zone string is bounded BEFORE it reaches the engine, because the
    // engine's `UnknownTimeZone` names the string it was given and that string
    // ends up in a response.
    if let Some(raw) = params.get("timeZone") {
        if let Err(code) = crate::validate::check_single_line(raw, 64) {
            return Err(ApiError::validation(vec![FieldError::new(
                "timeZone",
                code,
                "timeZone must be an IANA zone name of at most 64 characters",
            )]));
        }
    }

    // THE ENGINE'S OWN ERRORS, NAMED ON THE FIELD THEY BELONG TO. D1 §0.2
    // fixes these two field codes; they are what the console highlights.
    let cadence = Cadence::parse(&schedule, params.get("timeZone").map(String::as_str)).map_err(
        |e| match e {
            CadenceError::UnknownTimeZone { .. } => ApiError::validation(vec![FieldError::new(
                "timeZone",
                "timezone_unknown",
                crate::validate::bounded(&e.to_string(), 256),
            )]),
            CadenceError::Schedule(_) => ApiError::validation(vec![FieldError::new(
                "schedule",
                "schedule_invalid",
                crate::validate::bounded(&e.to_string(), 256),
            )]),
        },
    )?;

    let runs = cadence
        .next_runs(after, count)
        .into_iter()
        .map(|run| NextRunView {
            at: run.at,
            local_time: run.local_time,
            adjustment: run.adjustment.map(|a| match a {
                weirkeeper::cadence::Adjustment::NonexistentLocalTimeShifted => {
                    CadenceAdjustment::NonexistentLocalTimeShifted
                }
                weirkeeper::cadence::Adjustment::RepeatedLocalTimeFirst => {
                    CadenceAdjustment::RepeatedLocalTimeFirst
                }
                weirkeeper::cadence::Adjustment::RepeatedLocalTimeSecond => {
                    CadenceAdjustment::RepeatedLocalTimeSecond
                }
            }),
        })
        .collect();

    Ok(json(
        StatusCode::OK,
        &CadencePreviewResponse {
            request_id,
            schedule,
            preset: preset.map(preset_view),
            // The EFFECTIVE zone: `Zone::name()` answers `UTC` for the absent
            // field, which is what `status.policy.timeZone` records too.
            time_zone: cadence.zone().name().to_string(),
            tzdb: TZDB_SOURCE.to_string(),
            after,
            runs,
        },
    ))
}
