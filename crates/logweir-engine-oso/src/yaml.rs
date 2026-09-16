//! The workspace's SINGLE YAML scalar escaper.
//!
//! It was `render_restore::yaml_scalar` — one of the two renderers owning the
//! function the other one imported. That is a private detail turned into a
//! dependency: `render_validation.rs` read `use crate::render_restore::{…,
//! yaml_scalar}`, so the escaper's home was decided by which renderer happened
//! to be written first, and a third renderer (`render_backup`) arriving with
//! its own copy would have been the path of least resistance. Both halves of
//! the hazard below are the same hazard in all three documents, so there is
//! one function and one place to fix it.
//!
//! `tests/render_backup.rs::there_is_exactly_one_yaml_escaper` reads the three
//! renderer sources and asserts none of them defines `fn yaml_scalar(`.
//! Task 3 added `yaml_scalar_checked` HERE, beside this one, and swapped every
//! call site over; `tests/expansion.rs::every_renderer_calls_the_checked_
//! escaper` reads the same three sources and asserts NO call site calls the
//! unchecked `yaml_scalar(` any more. The unchecked function is still the one
//! escaper — `yaml_scalar_checked` is a pre-check in front of it, not a second
//! escaper — which is why the "exactly one" count is over `fn yaml_scalar(`
//! and not over the name's every prefix.

use crate::render_backup::RenderError;

/// Escapes an arbitrary string for safe embedding as a YAML double-quoted
/// scalar, and is used at EVERY interpolation site in both renderers
/// (`backup_id`, bootstrap servers, both sides of `topic_mapping`, every
/// storage field, `checkpoint_state`, `run_id` and `triggered_by`).
///
/// Global Constraint 4 says the three forbidden keys are never emitted "at
/// any value" — a value that, written raw, would ITSELF contain a physical
/// line whose pre-colon token is one of those keys is still an emission.
/// Concretely: `render_validation::render(&plan, "r", Some("x\ndry_run:
/// true"))` written without this function would produce a physical line
/// `dry_run: true` inside `restore.yaml`/`validation.yaml`, because a raw `\n`
/// in an interpolated `&str` is a real newline byte once it lands in the
/// rendered `String`. A `"` would truncate a naively hand-quoted scalar early
/// (see the old `format!("backup_id: \"{}\"\n\n", …)`, which quoted but never
/// escaped); a `\` would be reinterpreted as the start of a YAML escape by
/// whatever eventually reads the file. This function closes all three: it
/// ALWAYS emits a double-quoted scalar (never a bare/plain one) and escapes
/// backslash, double-quote, and every C0 control character (`\n`, `\r`, `\t`
/// by their short names; everything else below 0x20 as `\xHH`).
///
/// Deliberately NOT an attempt at full YAML plain-scalar-safety detection
/// (leading indicators, ambiguous ": ", reserved words, …) with quoting only
/// where "needed": a scalar that is unconditionally quoted is unconditionally
/// safe, and the resulting document is easier for the human reviewer this
/// task exists for to audit than one where quoting is conditional on the
/// input's shape.
///
/// What this function does NOT and CANNOT defend against: upstream's
/// `expand_env_vars` [U/kafka-backup/crates/kafka-backup-cli/src/commands/
/// config.rs:6-35] scans the RAW file text for a literal `${VAR}` and
/// substitutes it BEFORE the file is parsed as YAML at all — a textual
/// preprocessing pass with no awareness of quoting. A value containing a
/// literal `${...}` is expanded by the engine's own preprocessing regardless
/// of how this function escapes it, and the substituted text (drawn from
/// whichever environment variable is named, in the engine's OWN process
/// environment) lands in the file un-requoted. That hazard sits a layer
/// below YAML syntax entirely, so no YAML-scalar escaper — this one or any
/// other — can close it; it is a distinct problem from the one this function
/// solves.
pub(crate) fn yaml_scalar(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\x{:02x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// The ONE `${…}` sequence a rendered document may carry for the SOURCE
/// cluster's SASL password, emitted as a RAW LITERAL and never through the
/// escaper (`yaml_scalar_checked` refuses `${` in any input by construction).
///
/// This is the deliberate exception to G-EXP, and the reason it is an
/// exception: the password must NOT be in the bytes Logweir hashes. It reaches
/// the engine through the engine's own textual expansion of its own process
/// environment, so the rendered document names the variable and the projected
/// Secret supplies the value — which is also why the value itself has to be
/// safe to substitute into pre-parse text, and why
/// `logweir_core::guard::credential_is_renderable` exists (interface **I11**).
/// The SASL block that emits it is Task 6's (interface **I1**); this task
/// lands the name and the sweep that permits exactly these two.
pub const PLACEHOLDER_SOURCE_PASSWORD: &str = "${LOGWEIR_SOURCE_PASSWORD}";
/// The target cluster's twin of `PLACEHOLDER_SOURCE_PASSWORD`.
pub const PLACEHOLDER_TARGET_PASSWORD: &str = "${LOGWEIR_TARGET_PASSWORD}";

/// The engine's SASL/SCRAM-SHA-512 wire spelling — **ONE hyphen**.
///
/// `SaslMechanism` is `#[serde(rename_all = "SCREAMING-KEBAB-CASE")]` over
/// `Plain, ScramSha256, ScramSha512, Gssapi`
/// [U:crates/kafka-backup-core/src/config.rs:318-331], so `ScramSha512`
/// serialises as `SCRAM-SHA512`. **librdkafka spells the same mechanism
/// `SCRAM-SHA-512`** (`crates/logweir-kafka/src/rdkafka_reader.rs`), and
/// upstream's own `config/example-backup.yaml` comments the librdkafka form
/// and is wrong.
///
/// VERIFIED BY EXECUTION against the digest-pinned engine
/// (`third_party/kafka-backup-binary.digest`): the two-hyphen form is a serde
/// TYPE error that aborts config load —
/// *"Failed to parse config: source.security.sasl_mechanism: unknown variant
/// `SCRAM-SHA-512`, expected one of `PLAIN`, `SCRAM-SHA256`, `SCRAM-SHA512`,
/// `GSSAPI`"* — and **not** an unknown key, so `subprocess`'s unknown-key
/// readback cannot see it. Exactly what `render_restore.rs` already records
/// about `time_window_start`.
pub const ENGINE_SCRAM_SHA_512: &str = "SCRAM-SHA512";

/// The `security:` block, as all THREE rendered documents carry it — the
/// engine's `KafkaConfig.security` under a `source:` or a `target:` key.
///
/// # The nesting is `security:`, and it was measured, not read
///
/// The four keys are fields of `SecurityConfig`
/// [U:crates/kafka-backup-core/src/config.rs:191-208], reached through
/// `KafkaConfig.security` (`:173-189`), **not** fields of `KafkaConfig`
/// itself. Rendered one level too high — at two-space indent directly under
/// `source:` — the digest-pinned engine answers:
///
/// ```text
/// WARN Ignoring unknown config key `source.security_protocol`
/// WARN Ignoring unknown config key `source.sasl_mechanism`
/// WARN Ignoring unknown config key `source.sasl_username`
/// WARN Ignoring unknown config key `source.sasl_password`
/// ```
///
/// which is the `strip_offset_headers` defect exactly (Task 4 review F-1):
/// `OsoCliEngine::assert_no_dropped_logweir_key` aborts the run at exit 1
/// AFTER the document was written, and short of that abort a plan that named
/// SCRAM would have dialled the cluster unauthenticated. Under `security:`
/// the same engine emits **no** unknown-key warning at all.
///
/// # The password line is the raw placeholder, and never the value
///
/// `sasl_password` is emitted as the RAW LITERAL `${LOGWEIR_*_PASSWORD}`,
/// unquoted, and is the ONE interpolation in this workspace that does not go
/// through `yaml_scalar_checked` — which would refuse it, since it refuses
/// `${` in any input by construction. That is the deliberate G-EXP exception
/// and the reason for it: **the secret must not be in the bytes Logweir
/// hashes.** The engine substitutes it out of its own process environment
/// (`expand_env_vars`), the projected Secret supplies the value, and
/// `assert_no_unnamed_dollar_brace` then passes because these two names are
/// the only `${…}` sequences a rendered document may carry.
///
/// Unquoted rather than `"${…}"`, and that is a choice with a reason: the
/// expanded value lands as a YAML PLAIN scalar. A plain scalar's hazards —
/// a `#` after a space, a `: `, a leading flow indicator — produce a config
/// PARSE ERROR or a truncated password, i.e. a failed authentication; they
/// cannot open a new key, because `\n` and `\r` are refused by
/// `logweir_core::guard::credential_is_renderable` before the value is ever
/// projected. Double-quoting would instead make `\` an escape introducer, so
/// a password containing a backslash would be silently REWRITTEN (`\n` inside
/// a double-quoted scalar is a newline) and a trailing one would unterminate
/// the scalar — a silent-corruption class the five refused characters do not
/// cover. Unquoted trades a parse error for a wrong password, which is the
/// right way round.
pub(crate) fn render_security_block(
    auth: &logweir_core::engine::AuthRender,
    password_placeholder: &str,
) -> Result<String, RenderError> {
    use logweir_core::engine::AuthRender;
    match auth {
        // NOTHING AT ALL — see `AuthRender`'s doc comment. This is also what
        // keeps every golden that predates SCRAM byte-identical.
        AuthRender::Plaintext => Ok(String::new()),
        AuthRender::ScramSha512 {
            username,
            tls,
            tls_ca_file,
        } => {
            // A CA with no TLS transport is refused, never dropped: see
            // `logweir_core::connection::TlsCaWithoutTls`. `with_tls_ca_file`
            // is the constructor that already refuses it; this is the backstop
            // for a plan whose fields were set directly.
            if tls_ca_file.is_some() && !*tls {
                return Err(RenderError::TlsCaWithoutTls);
            }
            let mut s = String::from("  security:\n");
            // SCREAMING_SNAKE_CASE over Plaintext|Ssl|SaslPlaintext|SaslSsl
            // [U:config.rs:261-269]. `tls` is separate from the mechanism
            // because SASL/SCRAM over PLAINTEXT and over SSL are two
            // `security.protocol` values for ONE mechanism.
            s.push_str(&format!(
                "    security_protocol: {}\n",
                yaml_scalar_checked(if *tls { "SASL_SSL" } else { "SASL_PLAINTEXT" })?
            ));
            s.push_str(&format!(
                "    sasl_mechanism: {}\n",
                yaml_scalar_checked(ENGINE_SCRAM_SHA_512)?
            ));
            // **G-ID.** FROM THE PLAN BYTES, never from a cluster object read
            // at run time — `plan.<source|target>_auth` is the only source
            // this line has, and `plan_hash` covers it.
            s.push_str(&format!(
                "    sasl_username: {}\n",
                yaml_scalar_checked(username)?
            ));
            // The raw placeholder literal. See this function's doc comment.
            s.push_str(&format!("    sasl_password: {password_placeholder}\n"));
            // PLAT-07.1, Global Constraint 29's engine half. `SecurityConfig`
            // declares `ssl_ca_location: Option<PathBuf>` and, when it is set,
            // builds the rustls root store from that file ALONE instead of the
            // bundled webpki roots [U:crates/kafka-backup-core/src/config.rs:210-212,
            // U:crates/kafka-backup-core/src/kafka/tls.rs:97-127, tag v0.21.0].
            // A pod-local path the runner took from the projected CA volume,
            // checked like every other interpolation (a `${` in it is refused).
            // Emitted only when set, so every document without a private CA is
            // byte-identical to one rendered before this key existed.
            if let Some(ca) = tls_ca_file {
                s.push_str(&format!(
                    "    ssl_ca_location: {}\n",
                    yaml_scalar_checked(ca)?
                ));
            }
            Ok(s)
        }
    }
}

/// Guard **G-EXP**, first leg: `yaml_scalar` plus a pre-check that REFUSES any
/// input containing `${`.
///
/// The hazard is the one `yaml_scalar`'s own doc comment names and then says
/// it cannot close. Upstream runs `expand_env_vars` over the WHOLE config file
/// as raw text before it is parsed as YAML at all
/// [U:crates/kafka-backup-cli/src/commands/config.rs:1-34], wired into every
/// command Logweir drives (`backup.rs:11`, `restore.rs:12`,
/// `validate_restore.rs:8`), and an UNSET variable is replaced with the EMPTY
/// STRING behind nothing but a `tracing::warn!`. Two consequences, and only
/// the second one needs an attacker:
///
/// 1. A correctness hazard with no attacker at all: a topic legitimately named
///    `orders${X}` becomes `orders`, silently, and the drill restores a
///    different topic than the approved plan named.
/// 2. The document Logweir hashes is a TEMPLATE; the document the engine
///    executes is its expansion. `plan_hash`
///    (`crates/logweir/src/drill/phase1_approval.rs:87`) therefore covers
///    bytes the engine never executes, and anything able to set an environment
///    variable on the runner pod can change topics, prefixes, bootstrap
///    servers or the storage prefix AFTER the hash check passed.
///
/// Escaping cannot help, because the expansion happens a layer below YAML
/// syntax: `"orders${X}"` is expanded inside its own quotes. So the only
/// closure is a REFUSAL, and it lives here — beside the escaper, at every
/// interpolation site — rather than as a post-render scan alone, so the
/// refusal names the input value the operator has to go and fix.
pub fn yaml_scalar_checked(value: &str) -> Result<String, RenderError> {
    if value.contains("${") {
        return Err(RenderError::DollarBrace(value.to_string()));
    }
    Ok(yaml_scalar(value))
}

/// Guard **G-EXP**, second leg: the post-render sweep. The only `${…}`
/// sequences permitted ANYWHERE in a rendered document are
/// `PLACEHOLDER_SOURCE_PASSWORD` and `PLACEHOLDER_TARGET_PASSWORD`.
///
/// Called by all three renderers' `render_and_digest`, BEFORE the digest is
/// taken, so a digest is only ever computed over a document that passed. A
/// sweep after the hash would publish the hash of a template and then refuse —
/// the digest would already be in a caller's hand.
///
/// # Why a second leg at all, when every interpolation site is checked
///
/// `yaml_scalar_checked` guards the VALUES. This guards the DOCUMENT, which is
/// the thing the engine expands: a `${` that arrives from a literal in a
/// renderer's own format string, from a field rendered without the escaper (an
/// integer today, a string after somebody's refactor), or from a future
/// storage arm, is invisible to the per-value check and visible here. Two legs
/// because the two failure modes are independent, not because one is
/// insufficient.
///
/// The scan reads each `${` to its matching `}`, or to the end of the physical
/// line when there is none — an unterminated `${` is reported as what it is
/// rather than swallowing the rest of the file, and it is refused just the
/// same, because upstream's own scanner is the authority on how far it reads
/// and this one may not claim to know.
pub fn assert_no_unnamed_dollar_brace(doc: &str) -> Result<(), RenderError> {
    let bytes = doc.as_bytes();
    let mut i = 0usize;
    // `$` (0x24) and `{` (0x7b) are ASCII, and no UTF-8 continuation byte is
    // below 0x80, so a byte-wise search cannot land inside a multi-byte
    // character and every index below is a char boundary.
    while i + 1 < bytes.len() {
        if bytes[i] != b'$' || bytes[i + 1] != b'{' {
            i += 1;
            continue;
        }
        let mut j = i + 2;
        let mut closed = None;
        while j < bytes.len() {
            match bytes[j] {
                b'}' => {
                    closed = Some(j + 1);
                    break;
                }
                b'\n' => break,
                _ => j += 1,
            }
        }
        let stop = closed.unwrap_or(j);
        let found = &doc[i..stop];
        if found != PLACEHOLDER_SOURCE_PASSWORD && found != PLACEHOLDER_TARGET_PASSWORD {
            return Err(RenderError::UnnamedPlaceholder(found.to_string()));
        }
        i = stop;
    }
    Ok(())
}

/// **G-EXP's ordering half.** Applies `yaml_scalar_checked`'s pre-check to a
/// list of entries EARLY, discarding the escaped output, so a value carrying
/// `${` is refused as an EXPANSION and not as something else.
///
/// # Why this exists rather than "the per-value check will catch it anyway"
///
/// It will, but with the wrong reason. `${` contains `{` and `}`, which are
/// two of `logweir_core::guard::GLOB_METACHARACTERS`, and both renderers run
/// **G-GLOB** over their topic lists FIRST — before a single byte is pushed,
/// deliberately (see `render_backup::render`). So `orders${X}` was reported as
/// a glob metacharacter, which sends an operator to the wrong fix: they did
/// not write a pattern, they wrote an interpolation, and the answer is not to
/// escape a brace but to stop the engine expanding the name out from under the
/// plan hash. G-EXP is therefore checked ahead of G-GLOB wherever both apply.
///
/// The escaped string is thrown away on purpose: this must be the SAME
/// predicate as the interpolation sites', not a second copy of
/// `contains("${")` that can drift from it, so it calls the one function and
/// keeps only its verdict.
pub(crate) fn reject_dollar_brace(entries: &[String]) -> Result<(), RenderError> {
    for entry in entries {
        yaml_scalar_checked(entry)?;
    }
    Ok(())
}
