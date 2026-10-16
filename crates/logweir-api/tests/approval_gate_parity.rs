//! **Gate parity: a `Switchover` is held to every approval gate a `Restore`
//! is** (PROD-04.2), and a second person approves a switchover in the
//! console.
//!
//! ONE TABLE, BOTH KINDS. Each row is one gate: the create's refusals before
//! anything exists, an approver's submission, and the second person's click.
//! Every row runs against the Restore route and the Switchover route over a
//! fresh cluster and must get the SAME status, problem code and field code
//! from each — and leave no object of either kind behind where the gate is a
//! create's. The routes share the functions (`routes::subject`); this table
//! is what would notice a route that stopped calling one.

mod support;

use std::sync::Arc;

use logweir_api::app::Clock as _;
use logweir_api::approval::{ApprovalSettings, ConfirmationKey};
use logweir_api::auth::oidc::Identity;
use logweir_api::auth::session::{self, SessionClaims};
use logweir_api::authz::Role;
use logweir_core::approval_policy::{
    ApprovalPolicySet, ExpectedSubject, RestoreAuthorization, PAYLOAD_TYPE_RESTORE_AUTHORIZATION,
};
use logweir_core::switchover::{Expected, PlanGroup, PlanTopic, RestoreRef, SwitchoverPlan};
use logweir_evidence::keys::SigningKey;
use logweir_evidence::Sidecar;
use serde_json::{json, Value};
use support::{FakeKube, Options, SharedApp, SharedOptions, TestApp, TestResponse, ISSUER, NS_A};

const POLICIES: &str = "allowOrdinaryConfirmation: true
policies:
  - name: prod-pair
    mode: two-person
    maxAgeSeconds: 3600
  - name: prod-strict
    mode: strict
    maxAgeSeconds: 3600
  - name: team-confirm
    mode: confirm
";

const TICKET: &str = "CHG-4711";
const V1: &str = "application/vnd.logweir.drill-approval+json;version=1.0.0";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Restore,
    Switchover,
}

const KINDS: [Kind; 2] = [Kind::Restore, Kind::Switchover];

impl Kind {
    const fn plural(self) -> &'static str {
        match self {
            Kind::Restore => "restores",
            Kind::Switchover => "switchovers",
        }
    }

    fn path(self) -> String {
        format!("/api/v1/workspaces/{NS_A}/{}", self.plural())
    }
}

fn sha(c: char) -> String {
    format!("sha256:{}", c.to_string().repeat(64))
}

/// A switchover plan over `groups` groups of `per` partitions each.
fn switchover_plan(groups: usize, per: u32) -> String {
    serde_yaml::to_string(&SwitchoverPlan {
        format_version: "1.0.0".into(),
        restore: RestoreRef {
            name: "incident".into(),
            uid: "r-uid".into(),
            plan_hash: sha('a'),
            scorecard_sha256: sha('b'),
            mapping_sha256: sha('c'),
        },
        rollback_of: None,
        groups: (0..groups)
            .map(|g| PlanGroup {
                source: format!("app-{g}"),
                target: format!("app-{g}"),
                topics: [(
                    "r-orders".to_string(),
                    PlanTopic {
                        offsets: (0..per).map(|p| (p, 40 + i64::from(p))).collect(),
                        approximate: if per > 1 { vec![1] } else { vec![] },
                        expected: Expected::none(),
                    },
                )]
                .into(),
            })
            .collect(),
    })
    .expect("yaml")
}

/// A restore plan over `topics` topics (the golden plan's, renamed).
fn restore_plan(topics: usize) -> String {
    let list: String = (0..topics)
        .map(|i| format!("    - \"topic-{i:05}\"\n"))
        .collect();
    support::golden_plan().replace("    - \"orders\"\n    - \"payments\"\n", &list)
}

/// What a plan variant is, per kind.
#[derive(Clone, Copy, Debug)]
enum Plan {
    /// A plan a second person can be shown in full.
    Good,
    /// A plan neither kind's scope can read.
    Unreadable,
    /// A plan too large to show: 1025 topics; 8193 positions.
    TooLarge,
}

fn plan(kind: Kind, plan: Plan) -> String {
    match (kind, plan) {
        (Kind::Restore, Plan::Good) => support::golden_plan(),
        (Kind::Switchover, Plan::Good) => switchover_plan(1, 2),
        (_, Plan::Unreadable) => "kind: not-a-plan\n".to_string(),
        (Kind::Restore, Plan::TooLarge) => restore_plan(1025),
        (Kind::Switchover, Plan::TooLarge) => {
            // 8193 positions: 2 groups of 4096, one more partition on the second.
            let mut p: SwitchoverPlan =
                serde_yaml::from_str(&switchover_plan(2, 4096)).expect("plan");
            p.groups[1]
                .topics
                .get_mut("r-orders")
                .expect("topic")
                .offsets
                .insert(5000, 1);
            serde_yaml::to_string(&p).expect("yaml")
        }
    }
}

/// A create body of `kind` over `plan`, under `approval`, with `ticket`.
fn create_body(kind: Kind, plan: &str, approval: &str, ticket: Option<&str>) -> Value {
    let mut body = match kind {
        Kind::Restore => support::restore_body(plan),
        Kind::Switchover => json!({
            "planBytes": plan,
            "planHash": logweir_core::ids::sha256_prefixed(plan.as_bytes()),
            "restoreRef": {"name": "incident", "uid": "r-uid"},
            "deadlineSeconds": 900
        }),
    };
    body["approvalRef"] = json!({ "name": approval });
    if let Some(ticket) = ticket {
        body["ticket"] = json!(ticket);
    }
    body
}

/// The (status, problem code, first field and its code) of a response.
fn outcome(response: &TestResponse) -> (u16, String, Option<(String, String)>) {
    let v = response.json();
    let field = v["errors"][0]["field"].as_str().map(|field| {
        (
            field.to_string(),
            v["errors"][0]["code"].as_str().unwrap_or("").to_string(),
        )
    });
    (
        response.status.as_u16(),
        v["code"].as_str().unwrap_or("").to_string(),
        field,
    )
}

// ---- the apps -------------------------------------------------------------

struct Console {
    settings: Arc<ApprovalSettings>,
    public: logweir_evidence::keys::VerifyingKey,
}

fn console(binding: &str, with_key: bool) -> Console {
    let key = SigningKey::generate_ed25519();
    let public = key.verifying_key();
    let policies =
        ApprovalPolicySet::parse(&format!("{POLICIES}namespaces:\n  {NS_A}: {binding}\n"))
            .expect("valid");
    Console {
        settings: Arc::new(ApprovalSettings {
            policies,
            confirmation: with_key.then(|| ConfirmationKey::from_key(key).expect("key")),
            ..ApprovalSettings::default()
        }),
        public,
    }
}

fn shared(console: &Console) -> SharedApp {
    SharedApp::new(
        FakeKube::new(),
        support::idp::MockIdp::new(ISSUER, &[]),
        SharedOptions {
            bindings: support::RoleBindings {
                revision: "parity-1".into(),
                bindings: vec![
                    support::binding(Role::Operator, NS_A, &["ops"]),
                    support::binding(Role::Approver, NS_A, &["approvers"]),
                    support::binding(Role::Administrator, NS_A, &["admins"]),
                ],
            },
            approval: Arc::clone(&console.settings),
            ..SharedOptions::default()
        },
    )
}

struct Session {
    cookie: String,
    csrf: String,
}

fn session_as(app: &SharedApp, subject: &str, groups: &[&str]) -> Session {
    let now = app.app.clock.now();
    let sid = format!(
        "sid-{}",
        subject.replace(|c: char| !c.is_ascii_alphanumeric(), "-")
    );
    let identity = Identity {
        issuer: ISSUER.to_string(),
        subject: subject.to_string(),
        display_name: subject.to_string(),
        verified_email: None,
        display_name_claimed: false,
        groups: groups.iter().map(|g| (*g).to_string()).collect(),
        auth_time: now,
    };
    let claims = SessionClaims::issue(&identity, sid.clone(), app.keys.version(), now, 900);
    Session {
        cookie: session::set_cookie(&app.keys, &claims, now)
            .split(';')
            .next()
            .expect("a cookie pair")
            .to_string(),
        csrf: app.keys.csrf_token(&sid),
    }
}

/// Where a row runs.
#[derive(Clone, Copy, Debug)]
enum Setup {
    /// The shared console, `team-a` bound to the policy, with or without the
    /// console key.
    Shared(&'static str, bool),
    /// The in-cluster administrator console, `team-a` bound to the policy.
    LocalAdmin(&'static str),
    /// The administrator console, `team-a` unbound (`legacy-governed-v1`).
    Unbound,
}

/// One app a row talks to.
enum App {
    Shared(SharedApp),
    Local(TestApp),
}

impl App {
    fn new(setup: Setup) -> Self {
        match setup {
            Setup::Shared(binding, key) => App::Shared(shared(&console(binding, key))),
            Setup::LocalAdmin(binding) => App::Local(TestApp::with(
                FakeKube::new(),
                Options {
                    approval: Arc::clone(&console(binding, true).settings),
                    ..Options::default()
                },
            )),
            Setup::Unbound => App::Local(TestApp::new()),
        }
    }

    fn fake(&self) -> &FakeKube {
        match self {
            App::Shared(app) => &app.app.fake,
            App::Local(app) => &app.fake,
        }
    }

    /// A POST as `subject` in `groups` (ignored on the administrator
    /// console, whose one identity is the local administrator).
    async fn post(
        &self,
        subject: &str,
        groups: &[&str],
        path: &str,
        key: Option<&str>,
        body: &Value,
    ) -> TestResponse {
        match self {
            App::Shared(app) => {
                let s = session_as(app, subject, groups);
                app.post(path, &s.cookie, Some(&s.csrf), key, &body.to_string())
                    .await
            }
            App::Local(app) => app.post(path, key, &body.to_string()).await,
        }
    }

    async fn get(&self, subject: &str, groups: &[&str], path: &str) -> TestResponse {
        match self {
            App::Shared(app) => {
                let s = session_as(app, subject, groups);
                app.get(path, &s.cookie).await
            }
            App::Local(app) => app.get(path).await,
        }
    }
}

const OPS: &[&str] = &["ops"];
const APPROVERS: &[&str] = &["approvers"];

/// A subject created by `alice` (an operator) under `setup`; its name.
async fn created(app: &App, kind: Kind, ticket: Option<&str>) -> String {
    let body = create_body(kind, &plan(kind, Plan::Good), "cutover", ticket);
    let made = app
        .post("alice", OPS, &kind.path(), Some("parity-create-1"), &body)
        .await;
    assert_eq!(made.status, 201, "{kind:?}: {}", made.text());
    made.json()["item"]["name"]
        .as_str()
        .expect("a name")
        .to_string()
}

fn sidecar(payload_type: &str) -> String {
    json!({"payloadType": payload_type, "signatures": [{"keyid": "approver-1", "sig": "AAAA"}]})
        .to_string()
}

/// **Every approval gate, against both kinds, one refusal.**
///
/// KILLS: a switchover create that skips any of the restore create's gates
/// (the modes `localAdmin` may serve, the console key, a comparable
/// requester, a showable ticket, the ticket rule, the reserved confirmation
/// suffix, the confirmation name's length, a showable two-person scope); a
/// switchover approval submission that stores unbounded or unparsed bytes,
/// takes a personal-key countersignature in a two-person namespace, takes
/// approval bytes under a bound policy, lets the requester approve, or records
/// a v1 document of another kind or plan; a switchover click or request view
/// that skips the restore's mode, role, format or separation checks.
#[tokio::test]
#[allow(clippy::too_many_lines, clippy::type_complexity)]
async fn every_approval_gate_refuses_a_switchover_as_it_refuses_a_restore() {
    // ---- the create's gates: nothing exists afterwards ----------------------
    type Expect = (u16, &'static str, Option<(&'static str, &'static str)>);
    let long_name = format!(
        "{}.{}.{}.{}",
        "a".repeat(60),
        "b".repeat(60),
        "c".repeat(60),
        "d".repeat(60)
    );
    let create_rows: Vec<(&str, Setup, &str, Plan, String, Option<&str>, Expect)> = vec![
        (
            "the administrator console serves no two-person namespace",
            Setup::LocalAdmin("prod-pair"),
            "alice",
            Plan::Good,
            "cutover".into(),
            Some(TICKET),
            (409, "policy_mismatch", None),
        ),
        (
            "no console key under a bound policy",
            Setup::Shared("team-confirm", false),
            "alice",
            Plan::Good,
            "cutover".into(),
            None,
            (409, "policy_mismatch", None),
        ),
        (
            "a requester a two-person approval cannot compare",
            Setup::Shared("prod-pair", true),
            "system:serviceaccount:team-a:deployer",
            Plan::Good,
            "cutover".into(),
            Some(TICKET),
            (409, "policy_mismatch", None),
        ),
        (
            "a ticket a second person cannot be shown",
            Setup::Shared("prod-pair", true),
            "alice",
            Plan::Good,
            "cutover".into(),
            Some("CHG-\u{202e}4711"),
            (422, "validation_failed", Some(("ticket", "not_showable"))),
        ),
        (
            "no ticket under a Governed policy",
            Setup::Shared("prod-strict", true),
            "alice",
            Plan::Good,
            "cutover".into(),
            None,
            (422, "validation_failed", Some(("ticket", "required"))),
        ),
        (
            "a ticket in an unbound namespace",
            Setup::Unbound,
            "alice",
            Plan::Good,
            "cutover".into(),
            Some(TICKET),
            (422, "validation_failed", Some(("ticket", "not_accepted"))),
        ),
        (
            "an approval name ending in the confirmation suffix",
            Setup::Shared("prod-strict", true),
            "alice",
            Plan::Good,
            "cutover-confirmation".into(),
            Some(TICKET),
            (
                422,
                "validation_failed",
                Some(("approvalRef.name", "reserved_suffix")),
            ),
        ),
        (
            "an approval name whose confirmation is no longer a name",
            Setup::Shared("prod-strict", true),
            "alice",
            Plan::Good,
            long_name.clone(),
            Some(TICKET),
            (
                422,
                "validation_failed",
                Some(("approvalRef.name", "too_long")),
            ),
        ),
        (
            "a two-person plan no scope can read",
            Setup::Shared("prod-pair", true),
            "alice",
            Plan::Unreadable,
            "cutover".into(),
            Some(TICKET),
            (
                422,
                "validation_failed",
                Some(("planBytes", "scope_incomplete")),
            ),
        ),
        (
            "a two-person plan too large to show in full",
            Setup::Shared("prod-pair", true),
            "alice",
            Plan::TooLarge,
            "cutover".into(),
            Some(TICKET),
            (
                422,
                "validation_failed",
                Some(("planBytes", "scope_incomplete")),
            ),
        ),
    ];
    for (gate, setup, who, variant, approval, ticket, expect) in &create_rows {
        let mut seen = Vec::new();
        for kind in KINDS {
            let app = App::new(*setup);
            let body = create_body(kind, &plan(kind, *variant), approval, *ticket);
            let response = app
                .post(who, OPS, &kind.path(), Some("parity-gate-0001"), &body)
                .await;
            let got = outcome(&response);
            assert_eq!(
                (
                    got.0,
                    got.1.as_str(),
                    got.2.as_ref().map(|(f, c)| (f.as_str(), c.as_str()))
                ),
                *expect,
                "{gate} / {kind:?}: {}",
                response.text()
            );
            assert_eq!(
                app.fake().count(kind.plural(), NS_A),
                0,
                "{gate} / {kind:?}"
            );
            assert_eq!(app.fake().count("approvals", NS_A), 0, "{gate} / {kind:?}");
            seen.push(got);
        }
        assert_eq!(seen[0], seen[1], "{gate}: the two kinds disagree");
    }

    // ---- an approver's submission ---------------------------------------------
    // (setup, the create's ticket, who submits, the body (kind, its plan hash), expected)
    type Body = fn(Kind, &str) -> Value;
    let submit_rows: Vec<(&str, Setup, Option<&str>, &str, &[&str], Body, Expect)> = vec![
        (
            "private-key text in the sidecar",
            Setup::Shared("prod-strict", true),
            Some(TICKET),
            "bob",
            APPROVERS,
            |_, _| json!({"sidecarBytes": "-----BEGIN PRIVATE KEY-----\nMC4CAQ==\n-----END PRIVATE KEY-----"}),
            (
                422,
                "validation_failed",
                Some(("sidecarBytes", "private_key")),
            ),
        ),
        (
            "a sidecar over 64 KiB",
            Setup::Shared("prod-strict", true),
            Some(TICKET),
            "bob",
            APPROVERS,
            |_, _| json!({"sidecarBytes": "x".repeat(64 * 1024 + 1)}),
            (422, "validation_failed", Some(("sidecarBytes", "too_long"))),
        ),
        (
            "a sidecar that is not one",
            Setup::Shared("prod-strict", true),
            Some(TICKET),
            "bob",
            APPROVERS,
            |_, _| json!({"sidecarBytes": "{}"}),
            (
                422,
                "validation_failed",
                Some(("sidecarBytes", "not_a_sidecar")),
            ),
        ),
        (
            "an approval document over 64 KiB",
            Setup::Unbound,
            None,
            "admin",
            APPROVERS,
            |_, _| json!({"sidecarBytes": sidecar(V1), "approvalBytes": "x".repeat(64 * 1024 + 1)}),
            (
                422,
                "validation_failed",
                Some(("approvalBytes", "too_long")),
            ),
        ),
        (
            "a countersignature in a two-person namespace",
            Setup::Shared("prod-pair", true),
            Some(TICKET),
            "bob",
            APPROVERS,
            |_, _| json!({"sidecarBytes": sidecar(PAYLOAD_TYPE_RESTORE_AUTHORIZATION)}),
            (409, "policy_mismatch", None),
        ),
        (
            "an approval in a confirm namespace",
            Setup::Shared("team-confirm", true),
            None,
            "bob",
            APPROVERS,
            |_, _| json!({"sidecarBytes": sidecar(PAYLOAD_TYPE_RESTORE_AUTHORIZATION)}),
            (409, "policy_mismatch", None),
        ),
        (
            "approval bytes under a bound policy",
            Setup::Shared("prod-strict", true),
            Some(TICKET),
            "bob",
            APPROVERS,
            |_, _| json!({"sidecarBytes": sidecar(PAYLOAD_TYPE_RESTORE_AUTHORIZATION), "approvalBytes": "{}"}),
            (
                422,
                "validation_failed",
                Some(("approvalBytes", "not_accepted")),
            ),
        ),
        (
            "the requester approving their own request",
            Setup::Shared("prod-strict", true),
            Some(TICKET),
            "alice",
            &["ops", "approvers"],
            |_, _| json!({"sidecarBytes": sidecar(PAYLOAD_TYPE_RESTORE_AUTHORIZATION)}),
            (403, "forbidden", None),
        ),
        (
            "a v1 sidecar of another payload type, unbound",
            Setup::Unbound,
            None,
            "admin",
            APPROVERS,
            |_, hash| json!({"sidecarBytes": sidecar(PAYLOAD_TYPE_RESTORE_AUTHORIZATION), "approvalBytes": json!({"plan_hash": hash}).to_string()}),
            (409, "policy_mismatch", None),
        ),
        (
            "an approval document that does not parse, unbound",
            Setup::Unbound,
            None,
            "admin",
            APPROVERS,
            |_, _| json!({"sidecarBytes": sidecar(V1), "approvalBytes": "not json"}),
            (
                422,
                "validation_failed",
                Some(("approvalBytes", "not_an_approval")),
            ),
        ),
        (
            "an approval of another plan, unbound",
            Setup::Unbound,
            None,
            "admin",
            APPROVERS,
            |kind, _| json!({"sidecarBytes": sidecar(V1), "approvalBytes": json!({"plan_hash": sha('f'), "subject_kind": format!("{kind:?}")}).to_string()}),
            (
                422,
                "validation_failed",
                Some(("approvalBytes", "plan_mismatch")),
            ),
        ),
        (
            "an approval of the other kind, unbound",
            Setup::Unbound,
            None,
            "admin",
            APPROVERS,
            |kind, hash| {
                let other = if kind == Kind::Restore {
                    "Switchover"
                } else {
                    "Restore"
                };
                json!({"sidecarBytes": sidecar(V1), "approvalBytes": json!({"plan_hash": hash, "subject_kind": other}).to_string()})
            },
            (
                422,
                "validation_failed",
                Some(("approvalBytes", "subject_kind_mismatch")),
            ),
        ),
    ];
    for (gate, setup, ticket, who, groups, body, expect) in &submit_rows {
        let mut seen = Vec::new();
        for kind in KINDS {
            let app = App::new(*setup);
            let name = created(&app, kind, *ticket).await;
            let hash = logweir_core::ids::sha256_prefixed(plan(kind, Plan::Good).as_bytes());
            let before = app.fake().count("approvals", NS_A);
            let response = app
                .post(
                    who,
                    groups,
                    &format!("{}/{name}/approval", kind.path()),
                    None,
                    &body(kind, &hash),
                )
                .await;
            let got = outcome(&response);
            assert_eq!(
                (
                    got.0,
                    got.1.as_str(),
                    got.2.as_ref().map(|(f, c)| (f.as_str(), c.as_str()))
                ),
                *expect,
                "{gate} / {kind:?}: {}",
                response.text()
            );
            assert_eq!(
                app.fake().count("approvals", NS_A),
                before,
                "{gate} / {kind:?}: nothing stored"
            );
            seen.push(got);
        }
        assert_eq!(seen[0], seen[1], "{gate}: the two kinds disagree");
    }

    // ---- the second person's click and the request view -----------------------
    // (setup, who, groups, read the view (else click), the click's digest, expected)
    let well_formed = sha('0');
    let console_rows: Vec<(&str, Setup, &str, &[&str], bool, String, Expect)> = vec![
        (
            "the request view outside a two-person namespace",
            Setup::Shared("prod-strict", true),
            "bob",
            APPROVERS,
            true,
            String::new(),
            (409, "policy_mismatch", None),
        ),
        (
            "a click outside a two-person namespace",
            Setup::Shared("prod-strict", true),
            "bob",
            APPROVERS,
            false,
            well_formed.clone(),
            (409, "policy_mismatch", None),
        ),
        (
            "a click on the administrator console",
            Setup::LocalAdmin("prod-strict"),
            "admin",
            APPROVERS,
            false,
            well_formed.clone(),
            (409, "policy_mismatch", None),
        ),
        (
            "a click without the approver role",
            Setup::Shared("prod-pair", true),
            "dave",
            &["admins"],
            false,
            well_formed.clone(),
            (403, "forbidden", None),
        ),
        (
            "a click naming no digest",
            Setup::Shared("prod-pair", true),
            "bob",
            APPROVERS,
            false,
            "sha256:XYZ".into(),
            (
                422,
                "validation_failed",
                Some(("confirmationSha256", "invalid_format")),
            ),
        ),
        (
            "a click on a request that is not the one shown",
            Setup::Shared("prod-pair", true),
            "bob",
            APPROVERS,
            false,
            well_formed.clone(),
            (409, "state_conflict", None),
        ),
    ];
    for (gate, setup, who, groups, read, digest, expect) in &console_rows {
        let mut seen = Vec::new();
        for kind in KINDS {
            let app = App::new(*setup);
            let name = created(&app, kind, Some(TICKET)).await;
            let before = app.fake().count("approvals", NS_A);
            let response = if *read {
                app.get(
                    who,
                    groups,
                    &format!("{}/{name}/approval-request", kind.path()),
                )
                .await
            } else {
                app.post(
                    who,
                    groups,
                    &format!("{}/{name}/console-approval", kind.path()),
                    None,
                    &json!({ "confirmationSha256": digest }),
                )
                .await
            };
            let got = outcome(&response);
            assert_eq!(
                (
                    got.0,
                    got.1.as_str(),
                    got.2.as_ref().map(|(f, c)| (f.as_str(), c.as_str()))
                ),
                *expect,
                "{gate} / {kind:?}: {}",
                response.text()
            );
            assert_eq!(
                app.fake().count("approvals", NS_A),
                before,
                "{gate} / {kind:?}"
            );
            seen.push(got);
        }
        assert_eq!(seen[0], seen[1], "{gate}: the two kinds disagree");
    }
}

/// **K3 at the API: a second person approves a switchover in the console.**
/// alice requests; bob is shown every group, topic and position from the
/// verified request and its plan, and clicks; the console signs an approval
/// of subject kind `Switchover` that the readers' own rule accepts for this
/// switchover and for no restore. alice cannot approve her own (403), and a
/// request too large to show is never offered.
///
/// KILLS: a switchover approval signed for the wrong kind; a scope that
/// shows a slice or drops a position's class; a click that takes the view's
/// word for the scope; the requester approving.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn a_second_person_approves_a_switchover_in_the_console() {
    let console = console("prod-pair", true);
    let app = shared(&console);
    let alice = session_as(&app, "alice", &["ops", "approvers"]);
    let p = switchover_plan(2, 3);
    let made = app
        .post(
            &Kind::Switchover.path(),
            &alice.cookie,
            Some(&alice.csrf),
            Some("k3-create-0001"),
            &create_body(Kind::Switchover, &p, "cutover", Some(TICKET)).to_string(),
        )
        .await;
    assert_eq!(made.status, 201, "{}", made.text());
    let made = made.json();
    assert_eq!(made["authorization"]["state"], "awaitingApproval");
    assert_eq!(made["authorization"]["operatorMode"], "two-person");
    assert_eq!(
        made["authorization"]["confirmationName"],
        "cutover-confirmation"
    );
    let name = made["item"]["name"].as_str().expect("a name").to_string();
    let uid = made["item"]["uid"].as_str().expect("a uid").to_string();
    let request = app
        .app
        .fake
        .object("approvals", NS_A, "cutover-confirmation")
        .expect("the request is stored");
    assert_eq!(request["spec"]["subjectRef"]["kind"], "Switchover");

    // What bob is shown.
    let bob = session_as(&app, "bob", APPROVERS);
    let seen = app
        .get(
            &format!("{}/{name}/approval-request", Kind::Switchover.path()),
            &bob.cookie,
        )
        .await;
    assert_eq!(seen.status, 200, "{}", seen.text());
    let item = seen.json()["item"].clone();
    assert_eq!(item["switchover"], name.as_str());
    assert_eq!(item["switchoverUid"], uid.as_str());
    assert_eq!(item["state"], "pending");
    assert_eq!(item["requester"], format!("{ISSUER}#alice"));
    assert_eq!(item["ticket"], TICKET);
    assert_eq!(item["approve"]["offered"], true, "{item}");
    assert_eq!(item["scopeComplete"], true, "{item}");
    let scope = &item["scope"];
    assert_eq!(scope["groupsCount"], 2);
    assert_eq!(scope["positionsCount"], 6);
    assert_eq!(scope["restore"]["name"], "incident");
    assert_eq!(scope["restore"]["mappingSha256"], sha('c'));
    assert_eq!(
        scope["groups"][1],
        json!({
            "source": "app-1",
            "target": "app-1",
            "topics": [{"topic": "r-orders", "positions": [
                {"partition": 0, "offset": 40, "approximate": false}, // engine-token-ok: a JSON field of the switchover scope view, not an engine subcommand
                {"partition": 1, "offset": 41, "approximate": true}, // engine-token-ok: a JSON field of the switchover scope view, not an engine subcommand
                {"partition": 2, "offset": 42, "approximate": false} // engine-token-ok: a JSON field of the switchover scope view, not an engine subcommand
            ]}]
        })
    );
    // alice is not offered her own, and her click is refused.
    let hers = app
        .get(
            &format!("{}/{name}/approval-request", Kind::Switchover.path()),
            &alice.cookie,
        )
        .await;
    assert_eq!(hers.json()["item"]["approve"]["offered"], false);
    assert_eq!(hers.json()["item"]["approve"]["refusal"], "requester");
    assert!(hers.json()["item"]["approve"]["sentence"]
        .as_str()
        .is_some_and(|s| s.contains("this switchover")));
    let digest = item["confirmationSha256"]
        .as_str()
        .expect("sha")
        .to_string();
    let own = app
        .post(
            &format!("{}/{name}/console-approval", Kind::Switchover.path()),
            &alice.cookie,
            Some(&alice.csrf),
            None,
            &json!({ "confirmationSha256": digest }).to_string(),
        )
        .await;
    own.assert_problem(403, "forbidden");
    assert!(app.app.fake.object("approvals", NS_A, "cutover").is_none());

    // bob's click.
    app.app.clock.advance(60);
    let bob = session_as(&app, "bob", APPROVERS);
    let clicked = app
        .post(
            &format!("{}/{name}/console-approval", Kind::Switchover.path()),
            &bob.cookie,
            Some(&bob.csrf),
            None,
            &json!({ "confirmationSha256": digest }).to_string(),
        )
        .await;
    assert_eq!(clicked.status, 201, "{}", clicked.text());
    let stored = app
        .app
        .fake
        .object("approvals", NS_A, "cutover")
        .expect("the approval is stored");
    assert_eq!(stored["spec"]["subjectRef"]["kind"], "Switchover");
    assert_eq!(stored["spec"]["subjectRef"]["name"], name.as_str());
    let bytes = stored["spec"]["approvalBytes"].as_str().expect("bytes");
    let sidecar: Sidecar =
        serde_json::from_str(stored["spec"]["sidecarBytes"].as_str().expect("sidecar"))
            .expect("a sidecar");
    logweir_evidence::verify::verify_detached(
        &console.public,
        PAYLOAD_TYPE_RESTORE_AUTHORIZATION,
        bytes.as_bytes(),
        &sidecar,
    )
    .expect("the console signed it");
    let approved = RestoreAuthorization::from_bytes(bytes.as_bytes()).expect("v2");
    assert_eq!(approved.subject.kind, "Switchover");
    assert_eq!(
        approved.approver.as_ref().map(|a| a.subject.as_str()),
        Some("bob")
    );
    // THE READERS' OWN RULE: this switchover, and never a restore of the
    // same name, UID and plan.
    let policy = console
        .settings
        .policies
        .resolve(NS_A)
        .bound()
        .cloned()
        .expect("bound");
    let expected = |kind: &str| ExpectedSubject {
        kind: kind.to_string(),
        namespace: support::INSTALL_NS.to_string(),
        name: name.clone(),
        uid: uid.clone(),
        plan_hash: logweir_core::ids::sha256_prefixed(p.as_bytes()),
    };
    let now = app.app.clock.now();
    logweir_core::approval_policy::check_restore_authorization(
        &approved,
        &expected("Switchover"),
        &policy,
        now,
    )
    .expect("the readers accept it for the switchover");
    assert!(logweir_core::approval_policy::check_restore_authorization(
        &approved,
        &expected("Restore"),
        &policy,
        now
    )
    .is_err());
}
