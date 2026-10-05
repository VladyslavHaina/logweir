//! `runnerResources` — what a runner pod asks for and is capped at (FX-2).
//!
//! # The defect this module closes
//!
//! `Restore.spec.runnerResources` and a `RehearsalSchedule`'s
//! `spec.bounds.runnerResources` were accepted by the schema, documented as
//! "what the runner pod asks for and is capped at", and copied from the
//! schedule onto the `Restore` it creates — and then [`crate::job::build`]
//! rendered a container with no `resources` at all. An operator who set
//! `limits.memory: 2Gi` got a `BestEffort` pod with no cap.
//!
//! # The decision: APPLY, and REFUSE rather than clamp
//!
//! The field is applied, not withdrawn. D3 §4.5 designed it, the CRD documents
//! it, and the alternative is worse on every axis an operator can see: a pod
//! that states no requests is `BestEffort`, the first thing the kubelet evicts
//! under node pressure, and a restore evicted mid-write is a half-written
//! target; a namespace whose `ResourceQuota` covers compute refuses every pod
//! that states no limits, so without the field a restore cannot run there at
//! all. PROD-10.1's bounded controls start from this lever.
//!
//! What is applied is validated first, by [`validate`], and a value that fails
//! is REFUSED with a reason naming the field. Nothing is ever clamped: a
//! clamped limit is a Job nobody asked for, and a run OOM-killed at a limit the
//! controller chose would read as a runner defect.
//!
//! # The rules, in the order they are checked
//!
//! 1. **The quantity parses** in the grammar
//!    [`crate::crds::rehearsal_schedule::QUANTITY_PATTERN`] admits. The
//!    schema already enforces the pattern; the controller re-checks because
//!    it never trusts a field it projects into a pod, and because a pattern
//!    alone cannot say anything about the VALUE.
//! 2. **The unit fits the resource.** Memory is a whole number of bytes, so
//!    `memory: 100m` — a tenth of a byte, the classic slip for `100Mi` — is
//!    refused. CPU is a whole number of millicores, the finest amount the
//!    scheduler honours.
//! 3. **Nothing is above its ceiling**, requests and limits alike: CPU
//!    [`CPU_CEILING`], memory [`MEMORY_CEILING`] — D3 §4.1's numbers. They are
//!    compiled in because no chart value configures them (none exists), and
//!    they are the controller's because comparing quantities in CEL needs the
//!    `quantity` library, whose presence cannot be proved on the 1.29 floor.
//! 4. **A limit is a cap.** A zero limit is refused — to a container runtime
//!    zero reads as "no limit", the opposite of what was written — and a
//!    memory limit below [`MEMORY_LIMIT_FLOOR`] is refused: no runner starts
//!    in less, and a bare number there (`512`, which is 512 bytes) is almost
//!    always a missing unit.
//! 5. **A request is at most its limit**, per resource, when both are set —
//!    the API server's own rule, checked here so a Restore is refused on its
//!    own object rather than failing as a rejected Job.
//!
//! Every failing rule is reported, not only the first: the objects are
//! immutable, and an operator who fixes one field per re-created object would
//! need as many attempts as there are mistakes.
//!
//! # What reaches the container
//!
//! The quantities are copied VERBATIM, in the spelling the object carries; the
//! API server canonicalises them on the Job (`0.5` is stored as `500m`). An
//! ABSENT block — or one with no quantity in it — renders no `resources` key
//! at all, which is byte-for-byte the Job shape before FX-2: the namespace's
//! `LimitRange` defaults apply exactly as they did.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;

use crate::crds::rehearsal_schedule::{ResourceQuantities, RunnerResources};
use crate::job::ContainerResources;

/// The resource name `cpu`.
pub const CPU: &str = "cpu";
/// The resource name `memory`.
pub const MEMORY: &str = "memory";

/// The CPU ceiling, requests and limits alike — D3 §4.1's "cpu limit <= 4".
pub const CPU_CEILING: &str = "4";
/// The memory ceiling, requests and limits alike — D3 §4.1's "memory limit
/// <= 8Gi".
pub const MEMORY_CEILING: &str = "8Gi";
/// The smallest memory LIMIT accepted. Not a measured minimum for a working
/// run — PROD-10.1 measures that — but a floor below which no runner process
/// starts, set to catch a missing unit before it becomes a pod that cannot be
/// created.
pub const MEMORY_LIMIT_FLOOR: &str = "32Mi";

/// The field path of `Restore.spec.runnerResources`.
pub const RESTORE_PATH: &str = "spec.runnerResources";
/// The field path of `RehearsalSchedule.spec.bounds.runnerResources`.
pub const REHEARSAL_PATH: &str = "spec.bounds.runnerResources";

/// One refused quantity: the field it is in, and why.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refusal {
    /// The full field path — `spec.runnerResources.limits.memory`.
    pub field: String,
    /// One sentence naming the value and the rule it breaks.
    pub detail: String,
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.field, self.detail)
    }
}

/// Every refused quantity in one block — never empty.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refused(pub Vec<Refusal>);

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parts: Vec<String> = self.0.iter().map(ToString::to_string).collect();
        f.write_str(&parts.join("; "))
    }
}

impl std::error::Error for Refused {}

/// Validate a `runnerResources` block and render what the runner container
/// carries.
///
/// `Ok(None)` for an absent block, and for a block that names no quantity at
/// all: the container then carries no `resources`, exactly as before FX-2.
/// `path` is the block's own field path ([`RESTORE_PATH`] or
/// [`REHEARSAL_PATH`]); every refusal names the full path of the field it is
/// about.
///
/// # Errors
///
/// [`Refused`], carrying every rule every quantity breaks — see the module
/// documentation for the rules and their order.
pub fn validate(
    resources: Option<&RunnerResources>,
    path: &str,
) -> Result<Option<ContainerResources>, Refused> {
    let Some(resources) = resources else {
        return Ok(None);
    };
    let mut refusals = Vec::new();
    let requests = checked_block(
        resources.requests.as_ref(),
        &format!("{path}.requests"),
        false,
        &mut refusals,
    );
    let limits = checked_block(
        resources.limits.as_ref(),
        &format!("{path}.limits"),
        true,
        &mut refusals,
    );

    // RULE 5, over the values that survived rules 1-4 — which are exact whole
    // bytes and whole millicores, so the comparison is exact too.
    for name in [CPU, MEMORY] {
        if let (Some((request_text, request)), Some((limit_text, limit))) =
            (requests.get(name), limits.get(name))
        {
            if request.compare(limit) == Ordering::Greater {
                refusals.push(Refusal {
                    field: format!("{path}.requests.{name}"),
                    detail: format!(
                        "is \"{request_text}\", above {path}.limits.{name} \
                         (\"{limit_text}\"); a request is at most its limit"
                    ),
                });
            }
        }
    }

    if !refusals.is_empty() {
        return Err(Refused(refusals));
    }
    let rendered = ContainerResources {
        requests: requests
            .into_iter()
            .map(|(name, (text, _))| (name.to_string(), text))
            .collect(),
        limits: limits
            .into_iter()
            .map(|(name, (text, _))| (name.to_string(), text))
            .collect(),
    };
    if rendered.requests.is_empty() && rendered.limits.is_empty() {
        return Ok(None);
    }
    Ok(Some(rendered))
}

/// Rules 1-4 over one `requests` or `limits` block. Returns the quantities
/// that passed, by resource name, with their verbatim text.
fn checked_block(
    block: Option<&ResourceQuantities>,
    path: &str,
    is_limit: bool,
    refusals: &mut Vec<Refusal>,
) -> BTreeMap<&'static str, (String, Amount)> {
    let mut passed = BTreeMap::new();
    let Some(block) = block else {
        return passed;
    };
    for (name, value) in [(CPU, block.cpu.as_ref()), (MEMORY, block.memory.as_ref())] {
        let Some(text) = value else {
            continue;
        };
        let field = format!("{path}.{name}");
        match check_one(name, text, is_limit) {
            Ok(amount) => {
                passed.insert(name, (text.clone(), amount));
            }
            Err(detail) => refusals.push(Refusal { field, detail }),
        }
    }
    passed
}

/// Rules 1-4 for one quantity.
fn check_one(name: &str, text: &str, is_limit: bool) -> Result<Amount, String> {
    // ---- 1. the grammar ---------------------------------------------------
    let Some(amount) = parse(text) else {
        return Err(format!(
            "is \"{text}\", which is not a Kubernetes quantity: a decimal number with an \
             optional SI suffix (n u m k M G T P E), binary suffix (Ki Mi Gi Ti Pi Ei) or \
             exponent (e9) — 500m, 2, 512Mi, 1e9"
        ));
    };

    // ---- 2. the unit fits the resource -----------------------------------
    let (ceiling_text, unit_ok) = if name == MEMORY {
        (MEMORY_CEILING, amount.is_multiple_of(NANOS_PER_UNIT))
    } else {
        (CPU_CEILING, amount.is_multiple_of(NANOS_PER_MILLI))
    };
    if !unit_ok {
        return Err(if name == MEMORY {
            let hint = if text.ends_with('m') {
                " — `m` is milli, a thousandth of a byte; `Mi` is mebibytes"
            } else {
                ""
            };
            format!("is \"{text}\", which is not a whole number of bytes{hint}")
        } else {
            format!(
                "is \"{text}\", which is finer than 1m, the smallest amount of CPU the scheduler \
                 honours"
            )
        });
    }

    // ---- 3. the ceiling ---------------------------------------------------
    let ceiling = parse(ceiling_text).expect("the compiled-in ceiling is a quantity");
    if amount.compare(&ceiling) == Ordering::Greater {
        return Err(format!(
            "is \"{text}\", above this build's {name} ceiling of {ceiling_text} (D3 §4.1); the \
             controller refuses rather than clamps, because a clamped value is a Job nobody \
             asked for"
        ));
    }

    // ---- 4. a limit is a cap ----------------------------------------------
    if is_limit {
        if amount.is_zero() {
            return Err(format!(
                "is \"{text}\"; a zero limit reads as \"no limit\" to a container runtime, the \
                 opposite of a cap — leave the limit out instead"
            ));
        }
        if name == MEMORY {
            let floor = parse(MEMORY_LIMIT_FLOOR).expect("the compiled-in floor is a quantity");
            if amount.compare(&floor) == Ordering::Less {
                return Err(format!(
                    "is \"{text}\", below the {MEMORY_LIMIT_FLOOR} floor: no runner starts in \
                     less, and a bare number is bytes (512 is 512 bytes) — did you mean Mi or Gi?"
                ));
            }
        }
    }
    Ok(amount)
}

// ===========================================================================
// An exact Kubernetes quantity
// ===========================================================================

/// Nano-units in one unit.
const NANOS_PER_UNIT: u128 = 1_000_000_000;
/// Nano-units in one milli-unit.
const NANOS_PER_MILLI: u128 = 1_000_000;

/// The most significant digits [`parse`] evaluates. Twenty is the CRD's own
/// `maxLength`, so every stored value fits; the bound is what keeps the
/// arithmetic below inside a `u128` with no approximation anywhere.
const MAX_SIGNIFICANT_DIGITS: usize = 20;

/// A quantity's value, exactly, in nano-units (10⁻⁹ of the resource's unit).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Amount {
    /// The value in nano-units, ROUNDED UP when it has a finer part; `None`
    /// when it does not fit a `u128`, which is larger than any ceiling.
    nanos: Option<u128>,
    /// Whether the value has a part finer than one nano-unit.
    sub_nano: bool,
}

impl Amount {
    /// Whether this is exactly zero.
    #[must_use]
    pub fn is_zero(&self) -> bool {
        self.nanos == Some(0)
    }

    /// Whether the value is an exact multiple of `nanos` nano-units.
    ///
    /// A value too large for a `u128` of nano-units answers `true`: it is
    /// some 10²⁹ units at least, so far above every ceiling that rule 3 is the
    /// refusal worth reporting, not a question about its last digit.
    fn is_multiple_of(&self, nanos: u128) -> bool {
        !self.sub_nano && self.nanos.is_none_or(|n| n % nanos == 0)
    }

    /// The order of two amounts; a value too large for a `u128` is above every
    /// value that fits. Exact for any two values that are whole nano-units.
    #[must_use]
    pub fn compare(&self, other: &Self) -> Ordering {
        match (self.nanos, other.nanos) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (Some(a), Some(b)) => a.cmp(&b),
        }
    }
}

/// Parse a quantity in the grammar `QUANTITY_PATTERN` admits —
/// `^[0-9]+(\.[0-9]+)?(([KMGTPE]i)|[numkMGTPE]|([eE][-+]?[0-9]+))?$` — into
/// its exact value. `None` for anything else, including a value with more
/// than [`MAX_SIGNIFICANT_DIGITS`] significant digits, which no stored object
/// can carry.
#[must_use]
pub fn parse(text: &str) -> Option<Amount> {
    let bytes = text.as_bytes();
    let int_len = bytes.iter().take_while(|b| b.is_ascii_digit()).count();
    if int_len == 0 {
        return None;
    }
    let (int_part, mut rest) = text.split_at(int_len);
    let mut frac_part = "";
    if let Some(after_dot) = rest.strip_prefix('.') {
        let frac_len = after_dot.bytes().take_while(u8::is_ascii_digit).count();
        if frac_len == 0 {
            return None;
        }
        frac_part = &after_dot[..frac_len];
        rest = &after_dot[frac_len..];
    }
    let (exp10, binary) = suffix(rest)?;

    // The value is digits × 10^(exp10 − |frac|) × 2^(10·binary). Leading
    // zeros carry nothing and trailing zeros move into the exponent, so what
    // is left is the significant digits.
    let digits: String = format!("{int_part}{frac_part}");
    let significant = digits.trim_start_matches('0');
    if significant.is_empty() {
        return Some(Amount {
            nanos: Some(0),
            sub_nano: false,
        });
    }
    let trimmed = significant.trim_end_matches('0');
    let trailing_zeros = i64::try_from(significant.len() - trimmed.len()).ok()?;
    if trimmed.len() > MAX_SIGNIFICANT_DIGITS {
        return None;
    }
    let mantissa: u128 = trimmed.parse().ok()?;
    let frac_len = i64::try_from(frac_part.len()).ok()?;
    // The power of ten that turns the mantissa into NANO-units.
    let power = exp10
        .saturating_add(trailing_zeros)
        .saturating_sub(frac_len)
        .saturating_add(9);
    // 2^(10·binary) is at most 2^60 and the mantissa below 10^20 < 2^67, so
    // their product is below 2^127.
    let scaled = mantissa.checked_mul(1u128 << (10 * binary))?;
    if power >= 0 {
        let nanos = u32::try_from(power)
            .ok()
            .and_then(|p| 10u128.checked_pow(p))
            .and_then(|factor| scaled.checked_mul(factor));
        return Some(Amount {
            nanos,
            sub_nano: false,
        });
    }
    // A negative power: divide, rounding up, and remember the remainder.
    let divisor = u32::try_from(-power)
        .ok()
        .and_then(|p| 10u128.checked_pow(p));
    Some(match divisor {
        Some(divisor) => Amount {
            nanos: Some(scaled.div_ceil(divisor)),
            sub_nano: scaled % divisor != 0,
        },
        // Beyond 10^38 the divisor exceeds any mantissa this accepts, so the
        // value is a positive amount below one nano-unit.
        None => Amount {
            nanos: Some(1),
            sub_nano: true,
        },
    })
}

/// A suffix as `(power of ten, power of 1024)`, or `None` when it is not one
/// of the three forms the grammar admits.
fn suffix(text: &str) -> Option<(i64, u32)> {
    let decimal = |p: i64| Some((p, 0));
    match text {
        "" => decimal(0),
        "n" => decimal(-9),
        "u" => decimal(-6),
        "m" => decimal(-3),
        "k" => decimal(3),
        "M" => decimal(6),
        "G" => decimal(9),
        "T" => decimal(12),
        "P" => decimal(15),
        "E" => decimal(18),
        "Ki" => Some((0, 1)),
        "Mi" => Some((0, 2)),
        "Gi" => Some((0, 3)),
        "Ti" => Some((0, 4)),
        "Pi" => Some((0, 5)),
        "Ei" => Some((0, 6)),
        _ => {
            let exponent = text.strip_prefix(['e', 'E'])?;
            let (negative, digits) = match exponent.as_bytes().first() {
                Some(b'-') => (true, &exponent[1..]),
                Some(b'+') => (false, &exponent[1..]),
                _ => (false, exponent),
            };
            if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            // SATURATED, NOT REFUSED: an exponent past ±10000 decides the value
            // as surely as the exact one would — far above every ceiling, or
            // far below every unit.
            let magnitude = digits
                .trim_start_matches('0')
                .parse::<i64>()
                .unwrap_or(if digits.trim_start_matches('0').is_empty() {
                    0
                } else {
                    10_000
                })
                .min(10_000);
            decimal(if negative { -magnitude } else { magnitude })
        }
    }
}
