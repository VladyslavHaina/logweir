# Release notes

One entry per release candidate, newest first. Each entry says what an operator
must know before running it: the **supported behaviour** that changed, the
**operator actions** an upgrade requires, the **verification scope** behind
each claim, who holds **retention authority**, and **migration and rollback**.
[The release checklist](tag1-checklist.md) row 9 points here, and its *What to
record* list is filled in this file's candidate record, not in a pull request.

A claim with no evidence behind it yet carries an `[UNVERIFIED]` mark and the
sentence that would close it (`scripts/check-unverified-labels.sh` refuses a
mark without one). The supported path these notes assume is
[quickstart.md, *The supported path*](quickstart.md); the measured limits are in
[stability.md, *Measured scale limits*](stability.md#measured-scale-limits-plat-202).

Each item says what changed, what to do (**Do**), what the claim rests on
(**Scope**: the rows that prove it, and what is not yet proven live) and how
to roll it back. The rows' transcripts, review rounds and measurements are in
git history and in each row's report.

---

## Unreleased — `main` after `v0.2.0-rc.1`

The last tag is `v0.2.0-rc.1` (candidate `56a60ebe`, publication `2c277dc1`);
its record is in the next entry, whose twenty-seven items are what that
candidate shipped. This entry collects what lands on `main` after that
publication: items 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42,
43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61,
62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100,
101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114 and 115. Items continue the next entry's numbering. The candidate
cut from this entry is `v0.2.0-rc.2`; its record follows
[the release checklist](tag1-checklist.md) as the next entry's does. Item 90
landed after the `v0.2.0-rc.2` tag and is not in that candidate; the
`v0.2.0` release that carries it has its own record below. Items 91 to 101
landed after the `v0.2.0` tag and are in the `v0.3.0` release, whose record
is below. Items 102 to 115 landed after the `v0.3.0` tag and are in no
release yet.

### Candidate record

Filled as the next entry's is: the publication commit and image digests from
the release dry run, before the tag; the chart and archive rows from the tag
run's `release.json`, after it. A row left as `—` is an unrecorded fact, not a
pass.

| What | Value |
|---|---|
| Candidate commit (the tagged commit) | `f9ed98ff`, the commit that added this record; it differs from the publication commit only under `docs/` |
| Version tag | `v0.2.0-rc.2`, pushed 2026-10-12 with the owner's approval; a GitHub pre-release |
| Publication commit (`release.json` `.images.publication`) | `96e601f04bd8a2993742400689353d74df3e7b8c` |
| CI run (`ci.yml`) for the publication commit, its `publish` job green | [38256093754](https://github.com/VladyslavHaina/logweir/actions/runs/38256093754): `check`, the four `e2e` shards and `publish` (build amd64, build arm64, promote, sign) green |
| Release dry run (`release.yml` dispatched on `main`) | [38267445281](https://github.com/VladyslavHaina/logweir/actions/runs/38267445281) on `0605455a`, every job green, including the full `ci.yml`, three builds and `drill / drill-from-artifact`; `release.sh verify` on its `release-assets`: 14 assets verified |
| Release run (`release.yml` on the tag) and release drill | [38272680382](https://github.com/VladyslavHaina/logweir/actions/runs/38272680382): every job green, including the full `ci.yml`, `drill / drill-from-artifact`, `publish-images` and `github-release`; the version tags name the dry run's three digests |
| Runner image digest (`logweir`; manifest list, amd64 and arm64) | `docker.io/vladyslavhaina/logweir@sha256:f2eb50861baec026286863aadb0d80d780841092a733ab8a03cf07ed7aadb2ba` |
| Controller image digest (`weirkeeper`; manifest list, amd64 and arm64) | `docker.io/vladyslavhaina/weirkeeper@sha256:4f666786af4dbdc9a155ae63908566796675f8a5662b256e373fd03c089c6653` |
| Console image digest (`logweir-console`; manifest list, amd64 and arm64) | `docker.io/vladyslavhaina/logweir-console@sha256:932b1f0f259a3530dd1cb73c8a1b80cb865e58ac6093f9a505ab72b57021507b` |
| Identity bootstrap image (`identity.bootstrapImage`, the chart's pin; `release.json` `.images.bootstrap` from the next candidate on, with the commit it was built from) | `docker.io/vladyslavhaina/logweir@sha256:8c5a76556236b3ff22d8618c1e2f141816eb60f6b1a476519b3b618efde348f6` (manifest list, amd64 and arm64), built from `d4e3295607155cb3b99fbe77a96f166595f5e5a2`, read anonymously on 2026-10-12; rc.2's chart pins it and its `release.json` does not list it |
| Chart (`logweir-chart`, from the tag run's `release.json` `.chart`) | `oci://registry-1.docker.io/vladyslavhaina/logweir-chart` `0.2.0-rc.2`, package sha256 `96608096061b2eac286d11dbaea2bf476495a0f35f3d34c3b03ecb8eb68dbc99`, OCI digest `sha256:9bcfe65776cd5772ff0777f037c6d4ba1cd2fad2d246f7a56d971bbab43232ac`; an anonymous `helm pull` is byte-identical to the release asset |
| CLI archives (three, from the tag run's `release.json` `.archives`) | `logweir-x86_64-unknown-linux-gnu.tar.xz` `11b63939f32060d97fbc4e60662c9c3f1714e041ddcb010f5f694462218fc797` (glibc 2.34+); `logweir-aarch64-unknown-linux-gnu.tar.xz` `7c4489a3e4ada0199052b1609c622fc52f948a4039f1c942fcbc106200f5269a` (glibc 2.34+); `logweir-aarch64-apple-darwin.tar.xz` `f607015421447029c7b0766796621df342f4e539c522c5606170bacd334a5e13` (macOS 11+, Homebrew `openssl@3`); `SHA256SUMS` checks every asset |
| `ui/` bundle, file by file | the output of the next entry's command, which the release asset `ui-files.sha256` also carries |
| Kubernetes exercises run on this candidate | PROD-14.1 on docker-desktop (arm64, Kubernetes 1.34, Helm 4.0.1), shared mode with Traefik, Dex and cert-manager, MinIO storage: the disconnected recovery from the kit (5 min 37 s, at `22dc6147`); a fresh install to the first verified restore in 4 min 22 s (at `22dc6147`); the upgrade to this publication (`96e601f0`, Helm revision 6) with every fix re-checked live, including FX-15's retention refusal and the console's visual-pass fixes |
| Checks deliberately deferred, each with its reason | item 76's bundle-to-chart walk (`v0.2.0-rc.1` ships no `logweir.yaml` to walk from; it stays `[UNVERIFIED]`); cosmetic console findings of the visual pass and its re-check (catalog rows at 1024 px, a large integer's display, four wording slips), recorded for v1.1 |

### `v0.3.0`: the release after `v0.2.0`

Logweir v2's theme A, cutover (OD-15), built together and proven once on the
PoC, with its one fix round. It is `v0.2.0` (the next record) plus items 91 to
101:

- **Consumer positions follow a restore (item 95, PROD-04.2).** A `Switchover`
  applies the reviewed positions of a verified complete restore to stopped
  consumer groups on its target, one person or two by the namespace's approval
  mode, and signs a record; a live group is refused, and the rollback is a
  download.
- **A restore can take the source's topic configuration (item 92, PROD-05.2)**
  and apply the held settings after a passing verdict; **owner detection
  (item 93, PROD-05.1a)** reads Strimzi `KafkaTopic` resources where the chart
  grants it (off by default).
- **Safe clones (item 91, PROD-11.1a):** a scratch restore may keep its topics
  for a declared time and removes exactly the topics it created.
- **Point-in-time selection (item 94, PROD-01.1b)** and **sealed runner lines
  (item 96, FX-43).**
- **The fix round (items 98 to 101):** the Switchover and the clone cleanup
  read evidence with the read grant, and a store refusal is never a verdict;
  a create refused for a taken approval name leaves nothing; both readers read
  a key file alike; an older runner's Preflight rows say the pod ran.
- **Before a rollback below this release:** delete the `Switchover`-kind
  Approvals first (rollback step 7, [kubernetes.md](kubernetes.md)).

The rebuilt console, the v1.1 fixes and the deep UI validation's fixes (items 102 and on) are on `main` after
this release and are not in it.

### Candidate record, `v0.3.0`

Filled as the next record is: before the tag from the release dry run; the
chart and archive rows after the tag.

| What | Value |
|---|---|
| Candidate commit (the tagged commit) | `77da964b`, the commit that added this record, on `claude/rel-v030` (merged into `main` afterwards); it differs from the publication commit only under `docs/` |
| Version tag | `v0.3.0`, pushed 2026-10-14, approved by the owner on 2026-10-13 (OD-18) once theme A's test pass was green; a full GitHub Release |
| Publication commit (`release.json` `.images.publication`) | `cc59922e1ac9764b7c8c171113c304d4613f93ee` |
| CI run (`ci.yml`) for the publication commit, its `publish` job green | [38513884587](https://github.com/VladyslavHaina/logweir/actions/runs/38513884587): `check`, the five `e2e` shards and `publish` (build amd64, build arm64, promote, sign) green |
| Release dry run (`release.yml` dispatched on `claude/rel-v030` at the publication commit) | [38518413169](https://github.com/VladyslavHaina/logweir/actions/runs/38518413169), every job green; `release.sh verify` on its `release-assets` from a checkout of the publication commit: 14 assets verified |
| Release run (`release.yml` on the tag) and release drill | [38523676498](https://github.com/VladyslavHaina/logweir/actions/runs/38523676498): every job green, including the full `ci.yml`, three builds, `drill / drill-from-artifact`, `publish-images` and `github-release`; the version tags name the dry run's three digests (read back with `docker buildx imagetools inspect`) |
| Runner image digest (`logweir`; amd64 and arm64) | `docker.io/vladyslavhaina/logweir@sha256:ed8bdea4c67a3c89e0b0f66e59a9c3b96b0830e40951bca178144069c83a9903` |
| Controller image digest (`weirkeeper`; amd64 and arm64) | `docker.io/vladyslavhaina/weirkeeper@sha256:0bb5bfcd81dc13f99f5aaf99b205999f4eb7581e8080c1ea10b25ae35581f48d` |
| Console image digest (`logweir-console`; amd64 and arm64) | `docker.io/vladyslavhaina/logweir-console@sha256:c0986cda49fd14526976fe12e01829aa9f0c2e44a79599d23e2cbe9eb5c38ac9` |
| Identity bootstrap image (`identity.bootstrapImage`; `release.json` `.images.bootstrap`) | `docker.io/vladyslavhaina/logweir@sha256:8c5a76556236b3ff22d8618c1e2f141816eb60f6b1a476519b3b618efde348f6` (amd64 and arm64), built from `d4e32956` (unchanged since `v0.2.0`) |
| Chart (`logweir-chart`, from the tag run's `release.json` `.chart`) | `oci://registry-1.docker.io/vladyslavhaina/logweir-chart` `0.3.0`, package sha256 `aa3b052976dfb7767d326ef0e4b4ed16b450461fcc6bd2d5cfcd7172049c3164`, OCI digest `sha256:92425d12dad17b77cf364a1913986467685abb840d8f2bffbdce34c923c6aa92`; an anonymous `helm pull` is byte-identical to the release asset |
| CLI archives (three, from the tag run's `release.json` `.archives`) | `logweir-x86_64-unknown-linux-gnu.tar.xz` `26da97fc18b78821d39569422f06b2affcd8bec8cb3cc08d5c192b071b9cb2b8` (glibc 2.34+); `logweir-aarch64-unknown-linux-gnu.tar.xz` `3eab7603bddbeeeecbe18ac377f525881437e4320180e8bf000724051ddc2aea` (glibc 2.34+); `logweir-aarch64-apple-darwin.tar.xz` `ecaf4308ba06e98d14b08b9856bb2fbf92cfee90d649d60b6547771dcf0445a7` (macOS 11+, Homebrew `openssl@3`); `SHA256SUMS` checks every asset after a fresh `gh release download` |
| Kubernetes exercises run on this candidate | the PoC (docker-desktop, arm64, Kubernetes 1.34, Helm 4.0.1, shared mode with Traefik, Dex and cert-manager, MinIO with least-privilege grants): theme A's single pass `poc-v2a` on `sha-4db6e5bb` (Helm revisions 9 to 17; 18 of P1–P23 passed, the rest blocked by D1 alone; the Strimzi `KafkaTopic` CRD installed for P10 and removed; a `helm rollback` to `v0.2.0` and back, P9, which found D5), then `poc-v2a-r2` on this publication (revisions 18 to 21): the render diff was only chart labels, image tags and one CRD description; `cosign verify` of all four images; the identity keys, the schedule and the catalog counts unchanged; P7u-c, P7u, P1, P1c, P2, P5, P7, P8, P3, P23 and P14 passed once each |
| Checks deliberately deferred, each with its reason | as `v0.2.0`'s, plus: a Switchover against a destination whose `evidenceRead` cannot be held by a pod (`ControllerIdentity`) reads with `evidenceWrite`, named (item 98); the clone cleanup's ServiceAccount and the evidence destination's UID check on the cleanup path are follow-ups (L-1, L-5 of the fix round's review); the sampled lane's whole-segment decode is a follow-up (FX-30's review) |

### `v0.2.0`: the release after `v0.2.0-rc.2`

The first release of Logweir v1: the v1 cut (OD-11), simplified (PROD-17.1) and
proven on a clean docker-desktop install (PROD-14.1). It is `v0.2.0-rc.2` (the
next entry) plus what landed on `main` after that tag:

- **Item 90 (FX-51):** a transient connection reset no longer fails a run's
  evidence write as unattested; a re-send that finds the key taken succeeds
  only on identical bytes, and evidence is never overwritten.
- **The Helm chart, validated and simplified:** every refusal in one place
  (`_validate.tpl`), with the same messages and order; the NetworkPolicy reads the API server's
  addresses from EndpointSlice; the identity bootstrap image is recorded in
  `release.json`. The rendered manifests are otherwise unchanged.
- **Operator action:** move a source-level `kafka.markerTopic` to
  `kafka.target.markerTopic` before upgrading; the chart now refuses it by name
  (see item 89's operator actions).

### Candidate record, `v0.2.0`

Filled as the next entry's is: before the tag from the release dry run; the
chart and archive rows after the tag.

| What | Value |
|---|---|
| Candidate commit (the tagged commit) | `c43410dd`, the commit that added this record; it differs from the publication commit only under `docs/` |
| Version tag | `v0.2.0`, pushed 2026-10-12 with the owner's approval; a full GitHub Release |
| Publication commit (`release.json` `.images.publication`) | `3c2f358ccabc960638464ef2ea44fff2e9a5baa0` |
| CI run (`ci.yml`) for the publication commit, its `publish` job green | [38294944730](https://github.com/VladyslavHaina/logweir/actions/runs/38294944730): `check`, the four `e2e` shards and `publish` (build amd64, build arm64, promote, sign) green |
| Release dry run (`release.yml` dispatched on `main`) | [38302537508](https://github.com/VladyslavHaina/logweir/actions/runs/38302537508) on `03a48bf9`, every job green; `release.sh verify` on its `release-assets`: 14 assets verified |
| Release run (`release.yml` on the tag) and release drill | [38305748419](https://github.com/VladyslavHaina/logweir/actions/runs/38305748419): every job green, including the full `ci.yml`, `drill / drill-from-artifact`, `publish-images` and `github-release`; the version tags name the dry run's three digests |
| Runner image digest (`logweir`; amd64 and arm64) | `docker.io/vladyslavhaina/logweir@sha256:5dcd8e37b55d309cf58beb00dcbc293ca70ac1514252c8261677b30881188f34` |
| Controller image digest (`weirkeeper`; amd64 and arm64) | `docker.io/vladyslavhaina/weirkeeper@sha256:c80995d7dd48ff2f5864c698436c5e75f1fbda0109ff48ce6eddfaf57b822991` |
| Console image digest (`logweir-console`; amd64 and arm64) | `docker.io/vladyslavhaina/logweir-console@sha256:4e5718771c7285b82670e040ab12445a9d509315cf16767ed20a93248af7613c` |
| Identity bootstrap image (`identity.bootstrapImage`; `release.json` `.images.bootstrap`) | `docker.io/vladyslavhaina/logweir@sha256:8c5a76556236b3ff22d8618c1e2f141816eb60f6b1a476519b3b618efde348f6` (amd64 and arm64), built from `d4e32956` |
| Chart (`logweir-chart`, from the tag run's `release.json` `.chart`) | `oci://registry-1.docker.io/vladyslavhaina/logweir-chart` `0.2.0`, package sha256 `7c721d7e4531905281101144b33ecdebf40f909f486d8461ec9e62258d1d72b9`, OCI digest `sha256:8b7faae8f5754190481314dfa8ae89e5500891cf465b96d45b249f00b5de5a1a`; an anonymous `helm pull` is byte-identical to the release asset |
| CLI archives (three, from the tag run's `release.json` `.archives`) | `logweir-x86_64-unknown-linux-gnu.tar.xz` `371c3713aab33f81a71777b8c661bff278439db2feb2e75550436d3ffaecbc94` (glibc 2.34+); `logweir-aarch64-unknown-linux-gnu.tar.xz` `1a3f4698fcd8895eaeabcf9bcc44843696cb283910a1140f7f28b7095041d4d4` (glibc 2.34+); `logweir-aarch64-apple-darwin.tar.xz` `89f02aec0f111080cfd27f049eb323fdd3ecbfaa4b75ccd76ef6261bef241575` (macOS 11+, Homebrew `openssl@3`); `SHA256SUMS` checks every asset |
| Kubernetes exercises run on this candidate | the PoC (docker-desktop, arm64, Kubernetes 1.34, Helm 4.0.1, shared mode with Traefik, Dex and cert-manager) upgraded from `v0.2.0-rc.2`'s publication to `3c2f358c` (Helm revisions 7 and 8): the rendered diff was only labels, image tags and comments, with the same NetworkPolicy peers; `cosign verify` of all four images; a backup with its consumer-positions block verified by both readers, and a verified restore. `v0.2.0-rc.2`'s record covers the clean install, the kit recovery and the visual pass |
| Checks deliberately deferred, each with its reason | as `v0.2.0-rc.2`'s, plus: the demo MinIO and Kafka are not PSA-restricted (documented; hardening is v1.1's); the YAML parser replacement (RUSTSEC-2024-0370/0449) is v1.1's |

### The operator-facing changes after `v0.2.0-rc.1`

Every item carries its own proof in its **Scope** line, including what the PoC
upgrade that carries it has not run yet. What each item changes, and what an
upgrade or a rollback needs for it, is collected under *Required operator
actions* and *Migration and rollback* below.

#### 28. The engine is `kafka-backup` 0.23.3; an `http://` archive endpoint needs `allow_http: true` (PROD-00.3f)

**Changed.** The runner image carries `kafka-backup` **0.23.3** (upstream
commit `afb160e7`) in place of 0.21.0. The segment format, the three engine
commands Logweir runs and every key it renders are unchanged, and archives
written by either engine restore with the other. `logweir doctor` accepts
exactly 0.23.3, as a whole token: 0.21.0 and a suffixed `0.23.3+build` are
mismatches. Receipts and scorecards name the engine that ran. A storage
location with a plain `http://` endpoint and `allow_http: false` is refused at
phase 0 with exit 3 (`refusal-reason=GuardRefused`) by `drill run`,
`restore run` and `backup run`, and no engine document is rendered with it: the
engine now derives plaintext from the scheme. `VirtualHosted` addressing with a
custom endpoint stays refused (`AddressingUnsupportedByEngine` /
`addressing_unsupported_by_engine`). The full-drill floor stays 0.21.0.
**Do:** roll the controller **and** runner image together; the controller
stamps the version into every runner Job. A standalone CLI install replaces its
engine binary with 0.23.3 (the digest in
`third_party/kafka-backup-binary.digest`) and its `LOGWEIR_ENGINE_VERSION` /
`LOGWEIR_ENGINE_DIGEST` with the new pair ([quickstart.md](quickstart.md), step
4; `examples/cronjob-drill.yaml`). A spec that names an `http://` endpoint must
say `allow_http: true`; a saved destination already cannot combine the two
(rule R3).
**Scope:** source review of every change from 0.21.0 ([decision record](to-do/decisions/PROD-00-engine-route.md) §12), CI's e2e suite on Kafka 3.7.1 and 4.3.1, archives drilled across both engines, guard rows with mutants, and `engine-matrix` run 37736333362; the controller's `LOGWEIR_ENGINE_VERSION` reaches a live runner Job only at the PoC upgrade.
**Rollback:** an older runner and controller run 0.21.0 again; `doctor` from
that build refuses 0.23.3. Measured: an archive 0.23.3 wrote restores and
verifies `pass` under the 0.21.0 engine. Reasoned from source, not run: an
OLDER Logweir build reading a 0.23.3 archive or receipt (the manifest keys and
the segment container are unchanged).

#### 29. A fresh install needs no approver key; an upgrade changes no namespace's approval (PROD-16.1)

**Changed.** Three approval modes by name: **confirm** (one person clicks
Create in the console, no key; internal `Ordinary`), **two-person** (item 66)
and **strict** (an approver's personal key; `Governed` or
`legacy-governed-v1`). On a FIRST `helm install` (Helm 3.19+ or 4.x) with a
console, the managed identity and `identity.bootstrapFeatures.consoleKey: true`,
the identity hook generates the console's `ConsoleConfirmation` key
(`logweir-console-confirmation`, retained, never regenerated; key loss stops the
hook). When the cluster has no trust of its own (no `TrustPolicy`, no
`TrustRoster/default`) it also creates one `TrustPolicy`
`logweir-installation` for the installation signer and that key, and marks the
install (`logweir.dev/approval-default` on `logweir-signing-trust`, a claim
naming that policy's UID and both key ids, honoured only beside that exact
policy). Every namespace without its own policy is then confirm
(`default-confirm-v1`); an explicit binding always wins. An install without a
console, with `identity.externalSecret`, or whose console key was adopted from a
hand-made Secret gets no policy, no marker and no grant. The `localAdmin`
console confirms too, as `urn:logweir:local-admin#admin`.
`approvalPolicy.default` (`confirm` with `allowOrdinaryConfirmation`, or
`strict`) overrides the marker; `policies[].mode` accepts `confirm`/`strict`.
The policy name `default-confirm-v1` is RESERVED beside `legacy-governed-v1`: a
document that declares it is refused by the chart at render and by the
controller and the console at start, and so are `mode: two-person` and a
`defaultMode` other than `confirm`/`strict`. The console's configuration gains
`confirmationKeyManaged`; the API's policy view gains `operatorMode` and
`basis`, and a create's `authorization` gains `operatorMode` (OpenAPI
`1.0.0-alpha.3`, additive; item 86).
**The install-only grant.** The `ClusterRole`/`ClusterRoleBinding`
`<release>-identity-trust` is a `post-install` hook of the first install alone,
deleted when the install's hooks finish on Helm 3.19+ and 4.x. Helm 3.12–3.18
keeps it after a failed hook, so the chart refuses to render it there
(`identity.installationTrust needs Helm 3.19.0 or newer`).
**An upgrade changes no namespace's approval mode:** no marker, no trust step;
every unbound namespace stays `legacy-governed-v1`. The hook only generates the
console key, or ADOPTS the one a PLAT-19.2 install made by hand under that
name. One thing changes: a `localAdmin` console now confirms namespaces already
bound to an `Ordinary` policy, so whoever can port-forward to it can authorise
a restore there alone.
**Do:** nothing, to keep today's approval, but first rename any policy named
`default-confirm-v1`. To opt an older install into confirm: a trust
administrator adds the key from `logweir-console-trust` to the `TrustPolicy`
governing those namespaces (`ConsoleConfirmation`), then set
`approvalPolicy.allowOrdinaryConfirmation: true` and `approvalPolicy.default:
confirm` and upgrade ([install.md](install.md) §5f). **After a failed first
install:** `kubectl delete clusterrolebinding,clusterrole
<release>-identity-trust --ignore-not-found`, then check it is NotFound. An
operator who pins an `identity.bootstrapImage` older than runner rev
`2fe8d907` sets `identity.bootstrapFeatures.consoleKey: false`.
**Scope:** core, identity-hook, controller, runner, console and chart rows (Helm 3.18.6 for the floor) and a localAdmin host journey; the fresh-install rows wait for PROD-14.1's clean install.
**Rollback:** remove `approvalPolicy.default` first (an older binary refuses a
document carrying `defaultMode`); `confirmationKeyManaged` goes with the chart
(an older console refuses the unknown field). An older controller and console
ignore the marker: every unbound namespace is `legacy-governed-v1` again, and a
pending confirmation under `default-confirm-v1` is refused
`ApprovalPolicyMismatch` (fail closed). The installation `TrustPolicy`, both key
Secrets and their public ConfigMaps stay.

#### 30. A plan can ask phase 7 to verify every record, and the scorecard says what its verdict covered (PROD-08.1)

**Added.** `sample.coverage: complete` in a drill or restore plan makes phase 7
read every archived segment of every restored partition, check its sha256
against the manifest, decode it, and compare every restored record with the
expected output by `x-original-offset`, headers in order. It reports exact
per-partition counts of missing, unexpected, duplicate, out-of-order and
different records, and names every faulty segment. `sample.complete_max_records`
bounds it; a bound that stops it signs `covered: false`, never a pass. Every
1.4.0 scorecard carries the optional `integrity.verification` block. The
scorecard is format **1.4.0**; both readers check arms IV-1 to IV-7 and print
`integrity coverage:` lines; `verify_scorecard.py` is 1.19.0
([drill-spec.md](formats/drill-spec.md#samplecoverage-and-samplecomplete_max_records-prod-081),
[the scorecard format](formats/drill-scorecard.md#integrityverification-format-140)).
On the upgrade: a sampled drill fails a restored head that repeats or reorders
source offsets (only a target the engine wrote wrongly); phase 0 refuses, exit
3, `coverage: complete` beside `max_partitions`, `complete_max_records` without
complete coverage, or a bound of `0`.
**Do:** nothing for existing plans, which stay sampled and byte-identical. To
verify every record, add `coverage: complete` to the plan's `sample` block and
re-approve it; budget about a minute per GiB of one-KiB records, against about
five seconds sampled, and set `complete_max_records` if a run must stop.
**Scope:** the fault matrix over real segments (`crates/logweir/tests/complete_verify.rs`), phase rows, the seven arms in both readers with the corpus and parity gate, mutants, and live compose rows in `e2e/tests/record_semantics.rs`; the PoC upgrade runs its sampled rows unchanged.
**Rollback:** an older runner ignores `sample.coverage`, runs a sampled check
and signs format 1.3.0 again; the 1.4.0 scorecards already written stay valid
under both readers.

#### 31. Every scheduled run's recovery point is offered from the Catalog view (FX-17)

**Changed.** The catalog sync's redactor did not read a scheduled run's set id
(`<schedule uid>-<yyyymmdd>-<hhmmss>`, or `…-r<k>` for a retry) as an
identity, so a catalog synced by a runner up to `v0.2.0-rc.1` published every
SCHEDULED point as `backupId: "[redacted]"`, `receiptKey:
"[redacted].receipt.json"` and `manifestKey: "[redacted].json"`, and the Catalog
view offered none of them. The redactor now reads exactly the shape the
controller mints (a lower-case UID, a real instant, at most a one-digit retry
suffix) as an identity; a near miss is still withheld. When a set id or receipt
key does come back redacted, the console names the field and its cause.
What changes on the upgrade:
- **A catalog's view does not change until its next sync** with the new runner.
- **The schedule page shows the catalog's verdict for scheduled runs**, and a
  set the catalog marks not selectable reads "not restorable" instead of a link.
- **A set id chosen for `logweir backup run`** (`--backup-id-override`, or a
  plan's `backup_id`) still has its receipt key withheld unless it is a public
  form (a UUID, or lower-case letters, digits, `.`, `-`, `_` and `=` under 40
  characters); retention keeps such a point `protected: Unknown`.
- **A `RetentionPolicy` starts weighing scheduled sets one by one.** All
  scheduled points used to share the set id `[redacted]`, so none was ever
  expired. After the re-sync those outside `keepLast` / `keepDays` become
  candidates, and an `Enforce` policy with `requireApprovedPlan: false` deletes
  the due ones at its next run. A set and its retry (`…-r1`) are separate sets.
- **A catalog-point readiness check** compares the whole values, not the
  redacted row.
**Do:** before the runner image rolls, read the plan preview of every
`Enforce` `RetentionPolicy` with `requireApprovedPlan: false`, or set it to
`true` until you have read the first plan after the re-sync. After the runner
image rolls, sync each `RecoveryCatalog` once: set `spec.syncRequest` to a new
value.
**Scope:** contract, check, controller, reaper and console rows over one shared set-id fixture, mutants, and live compose rows (scheduled points offered; retention deleting `X` and `Y` and leaving `X-r1`); the PoC re-sync is the upgrade's.
**Rollback:** an older runner withholds the scheduled set ids again at the next
sync, and retention treats every scheduled set as one shared set again. Sets an
`Enforce` policy deleted in between stay deleted.

#### 32. A recovery point records each topic's configuration, its portability and its owner, and the console defaults the replication factor from the source's (PROD-05.1)

**Added.** Every backup receipt records, per named topic, the partition count,
the replication factor (read from the source's metadata), the explicit
overrides and the effective value of each semantic key, each with its source and
a portability class, and the topic's declarative owner: a Strimzi `KafkaTopic`
(`--kafka-topic-resources <file> [--strimzi-cluster <name>]`) or the plan's
`source.topic_owners`, with where the run looked (`owner_detection`). A topic
whose configuration read was denied records NO entries; keys Kafka 4.0 removed
are marked `removedInKafka4`; a sensitive entry is recorded by key, never by
value. The receipt and the catalog point record are format **1.3.0**
(`topic_configuration`); both readers check arms 12 to 21;
`verify_scorecard.py` is 1.20.0. The product API publishes
`PointView.topics[]` with each topic's `applyRoute` and
`PointView.ownerDetection`, and the console's restore wizard defaults the
replication factor to the largest selected topic's source factor, capped at the
target's broker count
([backup-receipt.md](formats/backup-receipt.md#topic_configuration--the-topic-configuration-model-format-130)).
Phase −1 refuses, exit 3, a `source.topic_owners` that names an unplanned topic,
a kind other than `strimzi` or `external`, a blank, over-long (256) or
control-character reference, or one topic twice. A `Backup` or `BackupSchedule`
records `owner_detection: []` (the controller passes no owners yet).
**Do:** nothing for existing plans. Pass `--kafka-topic-resources`, or declare
`source.topic_owners`, for topics an operator such as Strimzi or Terraform
manages.
**Scope:** one row per receipt arm, the corpus and parity gate, catalog, view, API and console rows, and live compose rows (`e2e/tests/topic_configuration.rs`) on 3.9.2 and 4.3.1; the catalog and console rows are the PoC upgrade's.
**Rollback:** an older runner writes 1.1.0 or 1.2.0 receipts again with no
model; the 1.3.0 documents already written stay valid. A catalog synced by an
older runner lists no topics, and the console then defaults as before and says
why.

#### 33. SASL/PLAIN over TLS, SCRAM-SHA-256 and mTLS; a connection presents only its own credential (PROD-01.3) — required action

**Changed.** A connection (`KafkaCluster`, a spec's `auth:` block, the console
form, `logweir cluster-probe --auth-mode`) may use **`scramSha256`**,
**`plain`** (SASL/PLAIN: Confluent Cloud API keys, Azure Event Hubs connection
strings) or **`mtls`** (a client certificate from `auth.clientCertificate`, a
`kubernetes.io/tls`-shaped Secret) beside `plaintext` and `scramSha512`.
**`plain` without `tls: true` is refused, never dialled** (`PlainWithoutTls`, at
the CRD, the controller, `refusal-reason=PlainWithoutTls` at exit 3, and both
client builders). A receipt or catalog point naming a new mode is format
**1.4.0**, a scorecard **1.5.0**; `verify_scorecard.py` is 1.21.0. **A
connection presents only a credential bound to it:** every runner refuses a
password or client key whose Secret lacks the connection's `logweir-binding`
(UID and endpoint digest) with `CredentialBindingMismatch`, before any client
exists. The console API takes the credential ONCE (`auth.credential`), creates
the bound Secret itself and refuses `auth.credentialRef`
(`422 existing_credential_refused`); a console CA comes from a ConfigMap.
**Do:** suspend the schedules that use a credentialed connection, apply the
CRDs, roll controller, runner and console together, then bind each existing
credentialed connection's Secret **one at a time, by a command that names both
the connection and the Secret, after an inventory** ([kubernetes.md](kubernetes.md)
§20.9), and resume. A Secret that two connections name is an incident to
investigate, never one to split or bind in a loop. Binding needs Secret
`patch`, as strong as `get` for this Secret. A product-API client that named
`credentialRef` sends the password in `auth.credential`; a same-key retry that
changes only the value replays the first create ([api.md](api.md)). A
`BackupDestination` created with `secret.new` by an earlier build: remove its
`api.logweir.dev/request-sha256` annotation, or rotate the key. The chart's
admission policy also admits `logweir.dev/kafka-client-certificate`.
**Scope:** compose rows per listener shape, both readers VALID and every refusal (`e2e/tests/auth_modes.rs`, 8/8), the binding refusal on the shipped binaries against a never-dialled sentinel, and API rows; no managed provider was dialled, so Confluent Cloud and Event Hubs are `untested` ([support-matrix.md](support-matrix.md)); the live `KafkaCluster` journey is the PoC upgrade's.
**Rollback:** an older controller and runner ignore the binding pair and the
new fields; a bound Secret keeps working. An older build cannot parse a spec
naming a new mode, and an older reader refuses a 1.4.0/1.5.0 document that
names one. Roll the console with the controller (an older console sends
`credentialRef` again).

#### 34. A point-bound restore restores its point's own set, or nothing (FX-16)

**Changed.** A plan bound to a recovery point (`source.point`) verified the
point and then restored whichever set `source.backup` named, `latestCompleted`
included. The runner now refuses such a run, exit 3, the message opening
`PointBindingSetMismatch` (`refusal-reason=GuardRefused`; a `Restore` reads
`exitReason: GuardRefused`):
- **before any broker is contacted**, when `source.backup` is not the receipt's
  set (`backup: latestCompleted` beside a bound point is always refused), or
  when, under the plan's storage, the engine (which loads
  `<prefix>/<backup_id>/manifest.json`) would read the set's manifest anywhere
  but where the receipt attests it (a copy under another prefix, or a parent
  prefix with another same-id set); the refusal names the prefix;
- **before any target topic exists**, when the set about to be restored has
  another set id, manifest digest or manifest version than the one the binding
  verified.
The binding reads the receipt, its signature and the manifest through the
restore's own archive handle (the controller-named credential and CA).
Console and catalog-route restores already name the point's set. A
copy that keeps the original keys, in another bucket, restores as before.
**Do:** before the runner image rolls, change every standalone point-bound plan
that says `backup: latestCompleted` to name its point's set (`source.backup:
<the receipt's backup_id>`, which `logweir catalog list` shows), and approve it
again.
**Scope:** binding, phase-sequence, real-binary and store rows with mutants, and live compose rows (`e2e/tests/point_set_binding.rs`); the PoC's catalog-point restore is the upgrade's.
**Rollback:** an older runner restores whatever `source.backup` names again,
`latestCompleted` included. No archive, catalog or evidence object changes.

#### 35. The engine is Logweir's build of `kafka-backup` 0.23.3, for amd64 and arm64; the images are signed (PROD-00.2)

**Changed.** The runner's engine is **Logweir's build of the vendored OSO
0.23.3 source** (OD-3): the checksummed tarball, the ordered patch folder
(`third_party/kafka-backup-patches/`), compiled `--locked` for **linux/amd64 and
linux/arm64**, so the runner image has an arm64 variant. `kafka-backup
--version` prints `kafka-backup 0.23.3+logweir.1`, which `logweir doctor`
accepts exactly; OSO's own `0.23.3` passes only as the declared rollback. The
first patch bumps rustls 0.23.45 (RUSTSEC-2026-0285), h2 0.4.19
(RUSTSEC-2026-0258) and spin 0.9.9 in the lockfile. Engine vulnerabilities are
in `SECURITY.md`'s scope. One version names one build
(`third_party/kafka-backup-builds.txt`; a reused version is refused). The image
declares its engine in `/etc/logweir/engine-identity`, and every scorecard and
receipt signs it: `engine.version: 0.23.3+logweir.1` and an `engine.digest`
that is the build-input digest (`third_party/kafka-backup-build.env`).
**The controller no longer puts `LOGWEIR_ENGINE_VERSION`/
`LOGWEIR_ENGINE_DIGEST` in any Job**; a drill or backup asks its engine for
`--version` before it signs and refuses a version the binary does not print.
From the first `main` publication after the merge, CI signs all images
keylessly, attests the runner's SBOM and records SLSA provenance.
**Do:** roll the controller and the runner image together. **Never pair this
controller with a runner image published before this change:** that image
declares no engine, and its runs are refused (exit 1, before the engine
spawns, nothing signed). Verify a digest before deploying it with the pinned
commands in [install.md](install.md#verify-the-images), never with an identity
regular expression. A standalone CLI install replaces its engine with
Logweir's build (copy it out of the runner image, or
`scripts/engine-source.sh build`) and exports the new
`LOGWEIR_ENGINE_VERSION=0.23.3+logweir.1` and digest (build 2 since item 41;
[quickstart.md](quickstart.md), step 4). On arm64 nodes, runner
Jobs run natively. The identity bootstrap image stays amd64-only until it is
re-pinned.
**Scope:** parity drills of OSO's binary against both builds with a negative control that flips one byte ([decision record](to-do/decisions/PROD-00-engine-route.md) §13.5), `scripts/check-image.sh` check 8 on both platforms, guard rows and fourteen mutants; keyless signing first runs on `main`; the runner's signed engine identity is the PoC upgrade's.
**Rollback:** for one release OSO's released binary stays buildable: `docker
build --platform linux/amd64 --build-arg ENGINE_SOURCE=oso` produces a runner
that carries it and declares OSO's identity
([install.md](install.md#rolling-the-engine-back)); set `runnerImage` to it. It
is amd64-only. An older controller and runner pair runs OSO's 0.23.3 again, and
its `doctor` refuses `0.23.3+logweir.1`. Archives are unaffected in either
direction (patch 0001 changes no source file).

#### 36. An early-stopped restore is never signed `pass`; `max_partitions` samples every topic first (FX-23)

**Changed.** A restore whose engine was stopped early (a SIGTERM finishes the
current topic, exits 0 and never starts the rest) could be signed `pass` when
`sample.max_partitions` was below the partitions with records in the window.
The sampled verification now (a) holds every mapped partition to its own count
bound, so an empty partition the archive proves holds records in the window
fails by name; (b) keeps one partition of every topic before a second of any
under `max_partitions`, and names the topics it could not reach in the new
optional `sample.unsampled_topics`; (c) fails a restore whose engine offset
report has no entry for such a partition. All three are new causes for the
existing `fail-integrity`, exit 2. Every sampled scorecard this build signs is
format **1.6.0**
([stability.md](stability.md#scorecard-format-160-sampleunsampled_topics-and-a-stricter-sampled-check-fx-23));
`verify_scorecard.py` is 1.22.0, and both readers say for every sampled `pass`
whether its version proves these checks ran (only 1.6.0 or later). The
in-cluster runner was not exposed (`logweir` is PID 1 there).
**Do:** a consumer that matches a scorecard's exact `format_version` must
accept `1.6.0`. A sampled `pass` from an earlier build over a plan with
`max_partitions` below the partitions in the window is worth re-checking
([verify-a-scorecard.md](verify-a-scorecard.md#what-a-sampled-pass-guarantees-and-what-it-does-not)).
An archive whose timestamps run backwards within a segment can now fail the
per-partition bound
([the limitation](stability.md#out-of-order-timestamps-inside-a-segment)).
**Scope:** a live compose restore whose engine got SIGTERM mid-topic (`e2e/tests/stopped_restore.rs`: `pass` before, `fail-integrity` now, both readers accepting), and unit rows with 25 mutants (`crates/logweir/tests/stopped_restore.rs`); the sampled restore rows are the PoC upgrade's.
**Rollback:** an older runner samples the first N partitions again and ignores
the engine report; the 1.6.0 scorecards already written stay valid under both
readers.

#### 37. One crate holds all `unsafe` code; consumer-group and ACL reads land behind it (PROD-04.0b)

**Added, inside the build; no command uses it yet.** The librdkafka calls the
safe `rdkafka` API lacks (OD-6 (a2); ADR 0004's amendment in
[architecture](architecture.md)) go through ONE crate,
`logweir-rdkafka-ffi`, the only place `unsafe` may appear. Every other package
forbids `unsafe_code` in every target (the root `Cargo.toml`'s
`[workspace.lints.rust]`); `scripts/check-unsafe-scope.sh`, in `just lint`,
checks that and scans the tree. On top of it, `logweir-kafka` decides which
consumer groups a capture may take: classic and KIP-848 groups are capturable;
share, streams and other groups are excluded `GroupTypeNotCaptured`; an unseen
id is `GroupNotFound` only on a complete listing, `NotVisibleToPrincipal` when
a targeted describe is refused. Known librdkafka behaviour
(`docs/to-do/decisions/PROD-04.0-admin-path.md` §14): a describe refused for
visibility leaks 224 bytes per call; the legacy listing reports only the last
broker's error; below Kafka 3.8 no group has a type, so every group is
excluded `GroupTypeNotCaptured`. (The ACL read was withdrawn by item 74.)
**Do:** nothing.
**Scope:** FFI unit rows with a 100,000-call soak, per-trap rows with mutants, the gate with its negative control (`crates/logweir/tests/unsafe_scope_gate.rs`), `e2e/tests/group_admin.rs` on 4.3.1, 3.9.2 and 3.7.1, and leak checks.
**Rollback:** an older build has no FFI crate and no workspace lint table. No
archive, catalog, evidence or API object changes.

#### 38. Every other credential reference presents only a credential bound to it (FX-20) — required action

**Changed.** PROD-01.3's binding (item 33) now guards every place a writable
object names a credential Secret beside an endpoint its author chooses: a
`BackupDestination`'s `SecretKeys` grants (bound to its UID and archive route),
a `RetentionPolicy`'s delete-capable key (the policy, its destination's route
and its scope), and an inline `archive.secretRef` on a `Backup`, a schedule or a
`Restore`'s `sourceArchive` (every field that shapes the URL: scheme, bucket,
endpoint, region, addressing style and `allowHttp`). An S3 region that is not a
region name (`^[a-z0-9-]{1,32}$`) is refused `StorageRegionInvalid`
(`refusal-reason=GuardRefused`) by every runner, renderer and object-store
client. Every runner refuses an unbound or foreign Secret with
`CredentialBindingMismatch` **before it builds a store or composes a request**:
exit 3 on a `Backup` or `Restore`, the check code on a `Preflight` row, and
`Enforced=False/CredentialBindingMismatch` on a `RetentionPolicy` (nothing is
deleted). The expected values are published on
`BackupDestination.status.credentialBinding` and
`RetentionPolicy.status.credentialBinding`. A Secret may carry several
bindings (whitespace- or comma-separated). The product API refuses
`secret.existing` on a create (`existing_credential_refused`),
`:update-access` accepts only a Secret the destination already names, and a
rotation's `secret.new` goes to a new bound Secret
`lwd-<destination>-<role>-<suffix>`. The chart's `archive.s3.region` and
`evidence.controllerIdentityLocations[].region` refuse a non-region spelling.
The `ProtectionPolicy` sink bindings this item also added went with the alert
sinks (item 74).
**Do:** suspend the schedules that use a credential Secret, apply the CRDs,
roll the controller, the runner and the console together, then bind every
existing credential Secret **one at a time with `scripts/bind-credential.py`**:
dry first, then `--apply --confirm-endpoint` once the credential's owner has
confirmed the endpoint it prints ([install.md](install.md), *Bind every
existing credential Secret after the upgrade*). The tool refuses a Secret any
other object also names (an incident), one owned by or minted for another
object, and one already bound elsewhere. Remove the pre-PROD-01.3
`api.logweir.dev/request-sha256` from a console-made destination, and rotate a
guessable key. Resume the schedules. Until a Secret is bound its runs are
refused, closed.
**Scope:** per-site controller and API rows with negative controls, the shipped binaries against a never-dialled sentinel, one binding fixture read by the product and the tool (`e2e/fixtures/credential-binding/bindings.json`), and the tool's offline rows; binding the `primary` destination's Secrets and the refusal rows against a sentinel are the PoC upgrade's.
**Rollback:** an older controller and runner ignore every binding variable and
the new status fields; bound Secrets keep working. Roll the console with the
controller (an older console offers `existing` again, which this API refuses).

#### 39. A topic phase 0 creates is read, or handed to the engine, only once the cluster serves it (FX-18)

**Changed.** Kafka answers a create before every broker serves the new topic,
so an immediate read or engine produce could meet `UnknownTopicOrPartition`,
`LeaderNotAvailable` or `NotLeaderForPartition`. Phase 0 now waits, bounded,
until every partition of each topic it created has a leader that answers. A
topic still not served at the bound is exit 1 naming it, never a pass. The
`LogAppendTime` probe's configuration read retries the same answers. A run
whose target never serves a created topic leaves those topics for an operator
to remove.
**Do:** nothing.
**Scope:** pure classification rows, phase rows (a topic never served, exit 1), a guard over every e2e helper that creates a topic, and the compose suite (the race gone; 3 of 13 runs without the wait); the restore rows are the PoC upgrade's.
**Rollback:** an older runner uses the topics at once again. Nothing stored
changes.

#### 40. A connection that sends nothing is closed at the console's header deadline; the console speaks HTTP/1.1 only (FX-24)

**Changed.** The console (`logweir-api`) timed out a partial request head at
ten seconds, but never a connection that sent no byte, so 256 silent sockets
(the connection ceiling) could hold every slot. The console now serves HTTP/1.1
only, and the ten-second header deadline starts when a connection is accepted.
A client that opens with the HTTP/2 preface (prior knowledge, `h2c`) is closed
at its first line. Browsers, ingress controllers dialling an HTTP backend and
kubelet probes speak HTTP/1.1. Bodies and stalled readers are bounded by item 44
([api.md](api.md#conventions)).
**Do:** nothing, unless an ingress controller dials the console's Service with
HTTP/2 (an `h2c` or gRPC backend, never the chart's setting): return it to
HTTP/1.1, or the console is unreachable through it.
**Scope:** rows on the built binary (`crates/logweir-api/tests/local_admin.rs`) with twelve mutants, and the binary live on the host (a silent socket closed at 10.0 s); the shared-mode probe is the PoC upgrade's.
**Rollback:** an older console serves HTTP/2 prior knowledge again and leaves a
silent connection open; nothing is stored.

#### 41. A replication factor the archive does not record is never read as matching; the engine records every topic's (FX-21)

**Changed.** Engine 0.23.3 records the source replication factor in the
manifest for the FIRST topic a backup saves only, and phase 7 used the restored
topic's own factor in place of a missing one, so other topics' differences were
signed as no divergence. Phase 7 now compares the factor only where the
source's is recorded: the manifest's, else the bound point's receipt
(`topic_configuration`, 1.3.0). Where neither records it, the scorecard names
`"<target topic>: replication_factor (notRecorded)"` in
`topic_parity.not_assessed`, with its twin in `unexpected_divergence`, and both
readers print it in `configuration parity: NOT ASSESSED for …`. A partition
count the manifest does not record (before engine 0.17) is the same case. The
engine is fixed too: patch `0002-manifest-replication-factor.patch` records
every topic's factor, so the engine is `kafka-backup 0.23.3+logweir.2`
(build-input digest
`sha256:2bca49d72b92fc9d96d69ff2a8b64faef2c837bfbff8ba92326723f549197db8`);
`logweir doctor` accepts exactly that version. No format moves (MINOR under
OD-7's third case,
[stability](stability.md#a-source-replication-factor-the-archive-does-not-record-is-not-assessed-fx-21)).
**Do:** roll the controller and the runner image together, as for item 35. A
standalone CLI install replaces its engine with build 2 and exports
`LOGWEIR_ENGINE_VERSION=0.23.3+logweir.2` and its digest
([quickstart.md](quickstart.md), step 4). No field marks FX-21's documents: a
reader tells them apart by `engine.version` (`0.23.3+logweir.2` or a later
`+logweir.<n>`) with `engine.digest` (`sha256:2bca49d7…`). For a scorecard with
any other `engine.version`, or one a standalone CLI signed, a topic whose
`topics[].source_replication_factor` the manifest lacks has no
replication-factor finding, whatever the scorecard lists.
**Scope:** rule, phase-7 and reader rows (arm NR-5) with ten mutants, and live compose rows on `cluster3` over three engines (OSO's and build 1 recording one factor, build 2 all three), with parity drills between builds; a multi-topic backup and restore and the new engine identity are the PoC upgrade's.
**Rollback:** an older `logweir` writes the old silence again; documents this
build wrote stay valid. An engine rollback (`+logweir.1`, or OSO's 0.23.3 with
`ENGINE_SOURCE=oso`) records one factor per backup again, which this build then
names `notRecorded` unless a 1.3.0 receipt is bound. Archives are unaffected.

#### 42. A restore can select a window start, and the scorecard signs it (PROD-11.1)

**Added.** A drill or restore plan may state an inclusive window START:
`restore.point_in_time: "<start>/<end>"`
([drill-spec.md](formats/drill-spec.md#a-window-start-restorepoint_in_time-startend-prod-111)).
Every partition of every named topic is restored from the start. A start before
the archive's coverage, a start at or after the end, and a window no segment
overlaps are refused (exit 3) before any target topic is created. Phases 4 and
7 judge the window only. The restore preflight previews it
(`WindowStartBeforeCoverage`, `SelectionEmpty`, `SelectionInvalid`). The
scorecard is format **1.7.0** and carries `source.selection {window_start_ms,
window_end_ms}`
([stability.md](stability.md#scorecard-format-170-sourceselection-a-restore-from-a-stated-window-start-prod-111));
`verify_scorecard.py` 1.23.0 and `logweir drill verify` check arms SEL-1 to
SEL-3 and print a `replay selection:` line. Only a complete verification that
passed says no record before the start was restored. `restore.partitions` was
refused here (`PartitionSubsetsAwaitOwnerDecision`) until item 51.
**Do:** nothing for a plan without a start. A runner older than this release
refuses a plan with a start (`drill spec does not parse`, exit 1), so roll the
runner forward before submitting one.
**Scope:** compose rows with their own oracle (inclusive start, equal and non-monotonic timestamps, a topic subset under both coverages, each refusal with a control), and an older runner refusing a plan with a start.
**Rollback:** an older runner refuses plans with a start and ignores
`restore.partitions`. 1.7.0 scorecards stay valid under older readers, which
ignore the block; `sample.window_start` and `sample.coverage_note` still name
the start.

#### 43. A `Restore`, a rehearsal and the console can ask for complete coverage, and every surface says sampled or complete (PROD-08.1a)

**Added.** A `Restore` declares `spec.coverage: complete` (and
`spec.completeMaxRecords`) beside a plan that says the same; the controller
refuses a declaration the plan bytes do not say, in either direction, with
`ExecutionSpecInvalid`, before any approval is waited for. The product API's
create route carries both fields; every restore read carries
`coverage {requested, recorded, covered, incompleteReason}`; the operation
view's `verificationScope` adds `coverage`, a complete block with every
partition's counts, and `unsampledTopics`. The controller copies the signed
coverage onto `Restore.status.integrity`, read by a `COVERAGE` printer column.
The console's restore wizard offers complete coverage as a closed advanced
choice with its cost, never as the default, and its lists say sampled or
complete. The runner's metrics gain `logweir_drill_integrity_coverage` and
`logweir_drill_integrity_complete_covered` ([kubernetes.md](kubernetes.md#coverage-sampled-or-complete),
[api.md](api.md#the-restores-coverage-prod-081a),
[metrics.md](metrics.md)). **`covered: false` is never a pass, anywhere:** the
`Verified` condition reads `CompleteNotCovered`, the API's `verifiedSuccess`
and the console refuse it, and `logweir_drill_integrity_complete_covered` is 0.
The rehearsal side (`spec.bounds.coverage`, and the standing authorization's
format 1.1.0 with `coverage` and `maxPartitions: 0`) was withdrawn by item 73;
1.1.0 documents already signed still verify.
**Do:** apply the CRDs (both fields are additive; absent means sampled). A
plan written with the CLI that asks for complete coverage needs
`spec.coverage: complete` on its `Restore`, or the controller refuses it.
Budget about a minute per GiB of one-KiB records.
**Scope:** core, controller, API, runner and console rows (`ui/tests/complete-coverage.spec.js`) with mutants, and a console-built plan run complete on the compose stack; the k8s rows are the PoC upgrade's.
**Rollback:** an older controller ignores the spec fields (an older CRD prunes
them) and does not copy the status fields; the console then reads "not
recorded".

#### 44. A client that stops reading an answer, or stops sending a body, meets the console's stall deadline (FX-24b)

**Changed.** A connection may now wait on its client for **thirty seconds with
no progress** (`IO_STALL_TIMEOUT`): an answer's pending write, or a request
body's pending read, that moves no byte for that long fails, and the
connection and its slot are released. It restarts on every byte, so a slow but
steady reader keeps its connection; an operation event stream is held to its
own 300-second ceiling instead. A JSON mutation body must also arrive whole
within **sixty seconds** (`JSON_BODY_DEADLINE`). A body that stops, or misses
the total, is answered `400 malformed_request` ("The request body stopped
arriving before it was complete.", or "The request body was not received
within 60 seconds.") and the connection is closed. Item 53 adds a body rate
floor and one peer's share of the connections ([api.md](api.md#conventions)).
**Do:** nothing is required. A client that pauses mid-transfer for more than
thirty seconds, or sends a mutation body over more than sixty, sees its
connection closed and retries. In shared mode on a cluster that enforces
NetworkPolicy, consider `api.console.networkPolicy.enabled: true`, with the
ingress controller's selectors and the identity provider's egress
(`oidcCIDRs` or `oidcPeers`; [chart README](../charts/logweir/README.md)).
**Scope:** rows on the built binary (`crates/logweir-api/tests/local_admin.rs`: 256 non-reading clients ended, a steady reader and an event stream uncut, a stopped and a trickling body) and unit rows with mutants; the shared-mode probe is the PoC upgrade's.
**Rollback:** an older console leaves a stalled connection open again; nothing
is stored.

#### 45. A controller no longer rewrites a status whose content has not changed (FX-29)

**Changed.** A `BackupSchedule` whose latest slot was skipped (`Missed`) after a
slot that ran rewrote its status on every reconcile, about 120 times a second
on the PoC: a merge patch left the previous slot's
`status.lastSlot.backupRef`, which the controller read as a new decision on
every pass. Now every status block the schedule controller writes replaces the
stored one, a "when" field (`lastSlot.decidedAt`) is kept unless its block
changed, and the controller is woken by a new schedule, a spec change or its
own requeue (at most 30 s), never by its own status writes. The same loop was
fixed for a `BackupDestination` whose CA `ConfigMap` went away (stale
`caBundleSha256`) and for every `RetentionPolicy` whose catalog resolved.
`RetentionPolicy` `status.lastEvaluation.at` means "when these findings were
first reached" ([kubernetes.md](kubernetes.md) §7f).
**Do:** nothing is required. If a schedule was suspended to stop the loop,
resume it after the controller rolls. The first reconcile after the upgrade
writes each affected object once.
**Scope:** controller rows over a store that applies RFC 7386 merge patches and counts writes (`crates/weirkeeper/tests/schedule_status_churn.rs`: the PoC's schedule written once in forty passes), the class-sweep rows, and the timed requeue pinned.
**Rollback:** an older controller restores the loop the next time a slot is
skipped after one that ran, or a destination loses its CA; suspending the
schedule, or restoring the `ConfigMap`, stops it.

#### 46. Topics whose records need a schema registry are flagged from the archived bytes: "registry not captured" (PROD-03.0)

**Changed.** Every backup judges, per topic, whether its archived keys or
values carry Confluent framing (a zero byte, a 4-byte schema id, a payload),
from the segments it just wrote; **no registry is contacted**. It records the
verdict (`schemaDependent`, `notDetected` or `notAssessed`) in the signed
receipt (format **1.5.0**, `schema_dependency`: the basis,
each side's framed share, and the 16 smallest schema ids with a count). A side
is dependent when at least one in ten non-null records is framed. The catalog
point copies it (1.5.0), the product API publishes
`PointView.topics[].schemaDependency`, and the console says on the restore
review and the catalog page: **"Registry not captured: applications may not read these
records after restore."** Nothing is blocked. A receipt before 1.5.0, or a
topic the backup could not judge, reads **not assessed**. Detection is bounded
and never fails a backup: at most two segments per partition and eight
partitions per topic, stopping at 16 MiB stored, an 8 MiB zstd window, 256 MiB
decompressed per segment, and 120 s per backup (`notAssessed`:
`segmentTooLargeForDetection`, `detectionTimeBudgetExceeded`,
`segmentUnreadable`). It adds at most about **17 MB** to the runner's memory.
`verify_scorecard.py` **1.24.0** and `logweir drill verify` check arms 22 to 29.
Stated limit: a binary key that is a big-endian 64-bit integer from 2^24 to
2^56 looks framed; schema ids in headers, Apicurio's 8-byte ids and other
registries' framing are not detected
([the contract](formats/backup-receipt.md#schema_dependency--does-a-restore-need-a-schema-registry-format-150)).
**Do:** nothing is required. A `Backup` with a tight `spec.deadlineSeconds`
should allow up to 120 s more after the engine. Points recorded before this
item read not assessed.
**Scope:** detector, bound and memory rows, arms 22–29 in both readers with the corpus and parity gate, one fixture read by runner, API and console rows, and a live compose row through Karapace with the registry stopped; a backup's `schema_dependency` and the catalog's `topics[].schemaDependency` are the PoC upgrade's.
**Rollback:** an older runner writes 1.3.0/1.4.0 receipts with no block (read
not assessed); the 1.5.0 documents stay valid. An older controller or console
ignores the field.

#### 47. A sign-in whose identity provider stalls is answered at the provider deadline (FX-28)

**Changed.** The shared console's OIDC client applied its ten-second provider
deadline (`PROVIDER_DEADLINE`) to the response head only. Now one deadline
covers the connection, the request, the head and the whole body of every
provider request (discovery, the key set, the token exchange), and a document
over 512 KiB is refused by name. A stalled request fails at ten seconds:
`/auth/login` answers `503 kubernetes_unavailable`, the callback
`401 unauthenticated`, with the new audit code `provider_timeout`. A callback
makes at most four requests, so a provider answering each just inside the bound
can take forty seconds. A dial has its own twelve-second bound, and provider
connections carry TCP keepalive (30 s idle, three probes ten seconds apart).
While a provider stalls, about 77 client addresses at the sign-in limit can
still keep every console connection busy ([api.md](api.md#sign-in)).
**Do:** nothing is required. Alert on `provider_timeout` beside
`provider_unreachable` and `code_exchange_failed`.
**Scope:** rows over a loopback provider that stalls (`crates/logweir-api/tests/oidc_provider_deadline.rs`), a raw-socket row on the built binary in shared mode, and mutants; a stall cannot be simulated on the PoC's Dex.
**Rollback:** an older console reads a stalled provider's body with no deadline
again; nothing is stored.

#### 48. A destination's *Test access* compares every grant's binding, and is never READY for a destination a backup would refuse (FX-20c)

**Changed.** A readiness check compared a credential's binding (item 38) only
where it opened a store, so a destination whose `archiveWrite` grant named
another destination's Secret tested **ready** while its backup was refused
`CredentialBindingMismatch`. Now a `Preflight` lists every `SecretKeys` grant
the run would present (on *Test access* every grant the destination declares;
on a backup check `archiveWrite` and `evidenceWrite`; on a restore preflight
`archiveRead` and the evidence destination's `evidenceWrite`) and projects only
its `logweir-binding` key. The new blocking row
**`destination.credentialBound`** answers `ready`/`CredentialBound` or
`notReady`/`CredentialBindingMismatch`, naming each refused grant's
`spec.access` field and Secret, with one
`<grant>=bound|CredentialBindingMismatch` fact per grant. A check that does not
answer the row is `unknown`, never `ready`, and the destination's `lastTest`
follows the verdict. No Secret or binding value reaches a status, the API or
the console. A workload-identity grant carries no binding
and is not listed. Also: a `RetentionPolicy` refused `CredentialBindingMismatch`
no longer reads `Enforced=True` on the next pass; the refusal stands until a
later run is harvested, with `status.enforcement: RecommendationOnly` and
`guarantees.ageExpiry: NotEnforced`.
**Do:** roll the controller and the runner image together: an older runner
refuses a plan that lists a grant (`phase: Failed`, `CheckContractMismatch`,
naming `grantBindings`). Re-run *Test access* on each destination after the
upgrade; bind its own Secret where a grant reads `CredentialBindingMismatch`
([kubernetes.md](kubernetes.md) §20.10).
**Scope:** contract, runner, controller, API and console rows over one shared fixture, and the retention hold over real passes; the F6 thief's live re-run is the PoC upgrade's.
**Rollback:** an older controller lists no grant and an older runner emits no
binding row: *Test access* overclaims again, while every run still refuses a
foreign Secret. Nothing is stored.

#### 49. A backup receipt records each topic's ID before and after the engine; a recreated topic is a new generation (PROD-01.4a)

**Added.** `logweir backup run` reads each named topic's ID (KIP-516) through
DescribeTopics before the engine and again when it exits, and the receipt
records both in Kafka's own text (the `TopicId` `kafka-topics.sh --describe`
prints), or `null` with the reason: `noTopicId` (below
inter-broker protocol 2.8), `notAuthorized`, `topicNotFound`, `readFailed`,
`notRead` or `reservedTopicId` (`AAAAAAAAAAAAAAAAAAAAAQ`). Receipt and catalog
point are format **1.6.0** (`generations`, `topics[].identity`). Different IDs
are a new generation; equal IDs the same one only when the later capture's
second read agreed; an ID that changed during a capture is flagged; an unknown
ID is "not established" ([backup-receipt.md](formats/backup-receipt.md)).
Both verifiers check arms 36–40 and print one `generations` line per topic;
`verify_scorecard.py` is 1.25.0. Nothing compares two points yet.
**Do:** nothing. The read needs `Describe` on each topic, which the engine
already needs; a refused or failed read is recorded and never fails the backup.
**Scope:** FFI rows with a soak and leak checks, mapping and generation rows with mutants, arms 36–40 with twelve corpus cases, and `e2e/tests/topic_ids.rs` against the broker CLI on 3.7.1, 3.9.2 and 4.3.1; one scheduled backup's `generations` against the source's topic IDs is the PoC upgrade's.
**Rollback:** an older runner writes receipts with no IDs, read "not
established" against newer ones. The 1.6.0 documents stay valid under every
major-1 reader; readers before 1.25.0 ignore the IDs.

#### 50. A backup can record the committed positions of the consumer groups it names, as signed evidence (PROD-04.1)

**Added.** A backup records, for each consumer group it is asked about, where
that group would resume, read just before the engine and signed: the receipt's
`consumer_positions` block (receipt and catalog point format **1.7.0**) carries
each group's outcome and counts, and binds by digest a positions document
beside the receipt (`<run_id>.consumer-positions.json`, format 1.0.0). Name the
groups in the plan (`source.consumer_groups`), on the command line (`logweir
backup run --consumer-group <id>`, repeatable) or on a `Backup`/`BackupSchedule`
(`spec.consumerGroups`): **at most 100 exact ids**, each at most 255 bytes, a
summary at most 80 KiB, else refused by name (`ConsumerGroupSelectionTooLarge`,
`ConsumerGroupIdInvalid`, `ConsumerGroupSelectedTwice`). Each group gets one
outcome: `captured` (each committed position judged `withinArchive`,
`atArchiveEnd`, `beforeArchive`, `beyondArchive`, `beforeLogStart`,
`noArchivedData`, or `PositionBeyondEnd`), `excluded` (`GroupTypeNotCaptured`,
`GroupNotFound`) or `failed` (`NotVisibleToPrincipal`, `PositionsUnstable`,
`GroupVanishedDuringCapture`). A partition with no committed offset is counted,
never offset 0. Both readers check receipt arms 30 to 35 and, given the
document (`--consumer-positions <file>`), CP-1 to CP-14; `verify_scorecard.py`
is 1.26.0. The product API publishes `PointView.consumerPositions`
([backup-receipt.md](formats/backup-receipt.md#consumer_positions--consumer-position-evidence-format-170),
[kubernetes.md](kubernetes.md#consumer-position-evidence-specconsumergroups)).
Positions read while applications run are **not atomic** with the records; on
Kafka 3.7.x every group is `excluded: GroupTypeNotCaptured`; the API and the
console neither set nor show `spec.consumerGroups` (use `kubectl`); nothing
resets a group. An older reader says `VALID` over a 1.7.0 receipt and checks
nothing in the block
([recovering positions](formats/backup-receipt.md#recovering-positions-with-the-source-offline)).
**Do:** nothing for existing plans: a backup that selects no group writes the
receipt it wrote before. Apply the CRDs before setting `spec.consumerGroups`,
and give the backup principal Describe on each selected group (and on the
cluster, for a complete listing).
**Scope:** core, builder, seam, corpus, parity, catalog, API, console and controller rows, and live compose rows through the shipped binary (`e2e/tests/position_evidence.rs`), each verified by both readers and refused with one byte changed; a schedule selecting a group is the PoC upgrade's.
**Rollback:** an older runner ignores `source.consumer_groups`, refuses
`--consumer-group` and records no positions; the 1.7.0 documents stay valid. An
older controller refuses a run whose frozen inputs carry `consumerGroups`
(`PlanConfigMapConflict`): let those runs finish, or remove
`spec.consumerGroups` from the schedule, first.

#### 51. A restore can select a partition subset, and its scorecard is format 2.0.0 (PROD-11.1b)

**Added.** A plan may name per-topic partition subsets, `restore.partitions:
{orders: [0, 2], payments: [1]}`, beside the interval form of
`restore.point_in_time`: `"<start>/<end>"`, or `"../<end>"` from the archive's
floor ([drill-spec.md](formats/drill-spec.md#a-partition-subset-restorepartitions-prod-111b)).
Topics with different subsets restore in different engine runs. Phases 4 and 7
judge the selection only: a record in an unselected partition of a narrowed
topic fails the run. A subset the archive cannot satisfy is refused, exit 3,
before anything is created; a selected partition with no record in the window
is signed `preflight-failed`. The preflight previews it
(`PartitionNotInBackupSet`, `SelectionInvalid`). **The scorecard is format
2.0.0**, the format's first MAJOR (owner decision OD-9 (a)), written ONLY for a
restore that states a subset
([stability.md](stability.md#scorecard-format-200-a-partition-subset-restore-prod-111b-the-first-major));
every other scorecard is the 1.x document it was. `verify_scorecard.py` 1.27.0
and `logweir drill verify` read it (arms PS-1 to PS-5); every older verifier
refuses it. Schema: `schemas/logweir-drill-scorecard-2.0.0.json`; 1.7.0 is
frozen beside it. A subset beside a plain instant does not parse. **Every
surface says partial:** the controller copies the signed selection to
`integrity.selection` (`scope: partial`, bounded at 256 topics and 1024
partitions), shown by a `SELECTION` printer column after `AGE`; the API serves
`selection`; the console says `partial: partitions 0, 2 of topic orders`.
`logweir drill approve` refuses an unparseable subset plan
(`SubsetPlanUnparseable`, exit 1).
**Do:** upgrade every verifier that will read a subset restore's scorecard
(older ones refuse it), and roll the runner forward before submitting a subset
plan (an older runner refuses one). Nothing for a plan without a subset.
**Scope:** unit, phase, preview, reader, parity, corpus, controller, API and console rows beside unnarrowed controls, and compose rows with their own oracle (two subsets, a faulty engine, older runners and every older verifier refusing 2.0.0); a subset `Restore` read by both verifiers and on every surface is the PoC upgrade's.
**Rollback:** an older runner refuses subset plans and an older reader refuses
2.0.0 scorecards, so roll the verifiers back last; read the subset restores'
2.0.0 scorecards with this release's verifiers. An older CRD, API and console
show a subset restore without its selection.

#### 52. Every object-store read has a size cap; the controller reads evidence under the relay's 1 MiB (FX-31)

**Changed.** A read from an object store took the whole object, so a bucket
holding a multi-gigabyte object at a receipt key could OOM-kill the shared
controller. Every read now names a cap ([kubernetes.md](kubernetes.md) §7b.4):
it refuses on the reported size before a body byte is read, then holds a
running cap over the stream. Existence tests read no body. The caps:
- the controller reads a receipt or scorecard under **1 MiB** and a sidecar
  under **64 KiB** (the evidence relay's caps; since item 65 a backup receipt
  is read under 5,131,072 bytes);
- runner, CLI and check Jobs read documents under 64 MiB, manifests under
  256 MiB and segments under 1 GiB;
- the evidence-fetch Job relays nothing for an object over `maxBytes`;
- concurrent controller reads share ONE 128 MiB budget, a quarter of the
  chart's 512Mi limit.
Over a cap the controller writes `NotAttempted` naming the key and the cap, a
final verdict; a signing-time re-read settles as `trust.signingTimeRead:
overCap`. The CLI and runner fail operationally, naming the cap.
**Do:** nothing is required. A 1.5.0 receipt is about 3.4 KB per topic, so a
run of more than about **250–300 topics** wrote a receipt no path verified
until **item 65 lifted this limit** (its own cap, 5,131,072 bytes, and at most
1,000 topics a backup).
**Scope:** store rows (`crates/logweir-store/tests/capped.rs`), controller rows with child-process peak-RSS measurements (`crates/weirkeeper/tests/read_caps.rs`), check rows, and a live compose row against MinIO; an oversized object at a receipt key is the PoC upgrade's.
**Rollback:** an older build reads whole objects again; nothing is stored
differently. An older controller treats this build's final `NotAttempted` as
any final `NotAttempted`.

#### 53. One peer outside the trusted proxy holds at most 32 of the console's connections, and a request body must keep up 32 KiB a window (FX-24c) — required action

**Changed.** Item 44's stall ends a reader that takes almost nothing, but a
client that reads fast enough to keep the kernel taking its answer, or sends a
body a byte at a time, kept a connection. So:
- **One peer's share.** In shared mode, once `trustedProxyService` or
  `trustedProxyCidrs` names a proxy, any other peer (a pod dialling the pod IP,
  a kubelet probe, a `kubectl port-forward`) holds at most **32** of the
  console's 256 connections (`MAX_CONNECTIONS_PER_PEER`). Its next connection is
  closed when accepted, logged `closed a connection at once` (at most once
  every ten seconds). The trusted proxy is never capped; with no trusted proxy
  configured (the chart's default) nobody is; localAdmin mode has no cap.
- **A body's floor.** A request body must bring at least **32 KiB in every
  thirty-second window** while it arrives (`BODY_MIN_PROGRESS`), or it is
  answered `400 malformed_request` at thirty seconds: "The request body stopped
  arriving, or arrived too slowly, before it was complete." A body that ends
  inside its window is never cut.
- **The chart requires the trusted proxy of a console it publishes.** With
  `api.console.mode: shared` and `api.console.ingress.enabled: true` it refuses
  to render ("api.console.ingress.enabled in shared mode needs the trusted
  proxy named") unless `api.console.trustedProxyService` or
  `api.console.trustedProxyCidrs` names the proxy, or the new
  `api.console.trustedProxy: none` opts out. `none` beside a named proxy, any
  other value, and the key in localAdmin mode are refused.
- **The start line says whether the cap is on:** `logweir-api started` carries
  `per_peer_cap` (32, or 0), and a shared console with no trusted proxy warns.
**Do:** before `helm upgrade`, a shared console the chart publishes through its
Ingress names its trusted proxy, `api.console.trustedProxyService:
{namespace, name}` of the ingress controller's Service (`/readyz` then depends on
reading its EndpointSlices: [api.md](api.md#configuration),
`trustedProxyService`), or opts out with `api.console.trustedProxy: none`; otherwise the
render fails, naming the value. A shared console without the chart's Ingress
should set `trustedProxyService` too. Name any other proxy that carries many
clients (an L7 load balancer, a second ingress), or they share 32 connections;
behind a service-mesh sidecar that re-originates connections (Istio
`127.0.0.6`, Linkerd `127.0.0.1`) name the sidecar's address or opt out. A wide
`trustedProxyCidrs` range never caps anything inside it. An enforcing
NetworkPolicy (`api.console.networkPolicy.enabled`) remains the bound on many
addresses ([chart README](../charts/logweir/README.md)).
**Scope:** rows on the built binary in both modes (`crates/logweir-api/tests/local_admin.rs`), unit rows, chart refusal rows in `scripts/check-chart.sh` and `chart_lint`, mutants, and the binary live on the host before and after; the per-peer probe on a console pod is the PoC upgrade's.
**Rollback:** an older console caps no peer and reads a body a byte at a time
until its sixty-second total. Remove `api.console.trustedProxy` from the values
before rolling the chart back: an older chart's schema refuses the key.

#### 54. A probe Job Kubernetes is collecting no longer clears `reachable`, and the probe's WARN lines stop (FX-19)

**Changed.** The TTL controller deletes a finished probe Job with foreground
propagation, so the Job stays readable, pod-less, with a
`metadata.deletionTimestamp`. The controller read it as a crashed probe and
cleared `status.reachable` (about 17 s on the PoC, during which a `Restore` was
refused `ClusterNotReachable`), and its races logged about 3,000
`KafkaCluster probe reconcile failed; requeueing` WARN lines a day. Now:
- a probe Job with a `deletionTimestamp` is not read and nothing is written;
- a finished probe Job carrying the controller's marker (the annotation
  `logweir.dev/probe-verdict-recorded` = the Job's UID, sent with its TTL after
  the verdict's status write) is not re-judged when its pod is gone; a TTL alone
  is never the marker;
- `NotFound` on the pod log or the TTL patch and `Conflict` on a status write
  are ordinary outcomes, not WARNs;
- a crash, an unreadable log and a refused probe pod are logged at WARN once
  per Job.
The last verdict stands **for at most 630 s** (twice the 315 s re-probe
interval). A probe Job stuck mid-deletion or never collected then clears
`reachable` with the new reason `ProbeStale` (`Reachable=Unknown`, `observedAt`
and `clusterId` kept), logged once, so a `Restore` is refused
`ClusterNotReachable` ([kubernetes.md](kubernetes.md#saved-connections)).
**Do:** nothing is required. A `KafkaCluster probe reconcile failed` WARN now
means a real failure. A connection that reads `ProbeStale` has a probe Job
Kubernetes has not collected: look for a pod stuck `Terminating` or a finalizer
on `logweir-probe-<name>`.
**Scope:** controller rows over the fake API with the log captured (`crates/weirkeeper/tests/kafka_cluster_controller.rs`, `fx19_*`) for every deletion shape and race, class-sweep rows for every Job-owning kind, and 42 mutants; the fifteen-minute live watch is the PoC upgrade's.
**Rollback:** an older controller brings the flap and the WARN lines back and
drops the 630 s bound; the marker annotation is harmless.

#### 55. A sign-in state is single-use on each replica; a refused callback really clears the login cookie (FX-13a, FX-32)

**Changed.** The login cookie is sealed, stateless and opens for 600 seconds,
and nothing recorded that its `state` had been used. Now, before the code is
exchanged, the callback redeems the state in the console process's own
in-memory record. A replay on the same replica is refused `401
unauthenticated` ("This sign-in was already used. Start again at
/auth/login.") with audit failure `login_state_replayed`, a warning, the login
cookie cleared and **no token request**. Across replicas the provider is the
backstop: an authorization code is single-use (RFC 6749 §4.1.2), so a replay
costs one refused token request per replica (`code_exchange_failed`). A code
the provider has not consumed is not covered. The record holds 65,536 states
per process; when full the oldest is forgotten, never a sign-in refused (audit
note `usedSignInStates: full`, one warning a minute). Separately (FX-32), a
refusal's `Set-Cookie` clearing `__Host-logweir_login` now reaches the browser
([api.md](api.md#sign-in)).
**Do:** nothing is required. Alert on `login_state_replayed` beside
`code_exchange_failed`.
**Scope:** rows through the whole router over one and two console processes (`crates/logweir-api/tests/sign_in_state.rs`), the seven callback refusals in `tests/oidc_login.rs`, unit rows, and mutants.
[UNVERIFIED — no PoC sign-in has replayed a callback URL against the deployed console yet; the PoC upgrade that carries this item does.]
**Rollback:** an older console keeps no record (a copied cookie replays within
its 600 s, each replay a token request) and drops a refusal's `Set-Cookie`;
nothing is stored.

#### 56. A deleted topic can be restored under its own name, into a topic that does not exist, behind its own approval subject (PROD-15.1)

**Added.** A restore under the ORIGINAL topic names: `orders` is recreated as
`orders`, the recovery of a deleted topic or of a lost cluster onto a
replacement. Owner decision OD-2 narrowed [stability.md](stability.md)'s
Never #1 to a LIVE topic. The plan states it in `newTopic` mode,
`target.topic_naming: {prefix: "", original_name: {owners: []}}`
([drill-spec.md](formats/drill-spec.md#targettopic_namingoriginal_name-prod-151));
the `Restore` declares `spec.target.topicNaming.originalName: true` (a CEL rule
allows it only in `newTopic` mode with an empty prefix; a mismatch with the
plan is `ExecutionSpecInvalid`); the console offers it with the owner statement
it needs ([kubernetes.md](kubernetes.md#restoring-under-the-original-topic-names-prod-151)).
**Its own approval subject.** Only an approval whose signed bytes carry the
subject `originalName` authorises it, and authorises nothing else: `logweir
drill approve --approval-subject original-name` (v1 `approval_subject`) or the
console's confirmation (v2 `approvalSubject`). A mismatch is refused
`ApprovalSubjectMismatch` by the controller and again by the runner (exit 3).
**In a `confirm` namespace the names are typed** (OD-10): the requester
re-types every original topic name; the console signs them
(`originalNameConfirmation`), and the API (`typed_topics_required`,
`typed_topics_mismatch`), the controller and the runner refuse anything else. A
`strict` namespace still needs the second person. An authorization document v2
carrying either field is `formatVersion` **2.1.0**; every other stays 2.0.0.
**It requires complete verification:** a sampled plan is refused
`OriginalNameNeedsCompleteCoverage` by the runner (exit 3), `drill approve`, the
controller, a CEL rule, the API (`original_name_requires_complete`) and the
console. **It restores whole topics:** a plan with `restore.partitions` is
refused `OriginalNameNeedsWholeTopics`.
**What the runner proves first** (exit 3, nothing written): every restored name
is absent; the target is another cluster than the source (the bound point's
VERIFIED receipt only; the allowlist file's `source_cluster_id` never counts),
or every broker reports
`auto.create.topics.enable=false` (`OriginalNameAutoCreateEnabled`,
`OriginalNameAutoCreateUnknown`); an owner was looked for (the plan's
statement, `logweir restore run --kafka-topic-resources`, or the receipt's
owners) and none found
unless the plan chose the owner path (`OriginalNameOwnerNotChecked`,
`OriginalNameOwnerPresent`, `OriginalNameOwnerUnreadable`). Creation is
exclusive: a name that appears after phase 0 stops the run, exit 1,
`failure-reason=TargetTopicAppeared`. **Logweir never deletes a topic under an
original name.** A stopped creation step names what it left on the runner's
`target-topics-appeared=` line and on the Restore (`status.exitReason`
`TargetTopicAppeared` or `CreatedTopicsLeft`, `status.targetTopicsAppeared`), in
the API (`targetTopicsAppeared`) and the console, in three lists: `appeared`,
`left` (this run created it) and `unconfirmed` (no definite answer), at most 100
names each with the full count. With a runner image OLDER than the controller,
check the list against the cluster. A producer writing during the restore is
detected (each record unexpected, the run `fail-integrity`), not prevented. The
scorecard is format **1.8.0** with `target.original_name`; both readers check
arms ON-1 to ON-14 (`verify_scorecard.py` 1.28.0); schema
`schemas/logweir-drill-scorecard-1.8.0.json`.
**Do:** nothing for any other restore. Apply the `Restore` CRD before the
controller rolls, and roll the controller, the runner, the product API and the
console together. Stop every producer of a restored name before such a
restore, and repoint consumers after it (positions are not copied). Write the
plan with `sample.coverage: complete` and without `restore.partitions`. If a
Restore ends `TargetTopicAppeared` or `CreatedTopicsLeft`, read
`status.targetTopicsAppeared`, check each `left` and `unconfirmed` topic, delete
it yourself, and create a new Restore. After any `Failed` original-name
Restore whose creation step did not answer, list the plan's target names on the
cluster yourself. The controller lists no `KafkaTopic` resources (PROD-05.1a).
**Scope:** runner, controller, API and console rows for every condition and refusal, both verifiers over 21 corpus cases with the parity script, and compose rows (`COMPOSE_PROFILES=auth,cluster2,autocreate`) recovering a deleted topic on a second cluster and on the same one; not run live: a real Strimzi operator and the PoC.
**Rollback:** an older runner sees the empty prefix and refuses the plan (exit
3) before it writes anything; an older controller ignores
`topicNaming.originalName`; a reader of authorization document v2 built before
this item refuses every 2.1.0 document. Rolling the CRD back prunes
`topicNaming.originalName` and `status.targetTopicsAppeared`. 1.8.0
scorecards stay valid under older readers, which ignore the block.

#### 57. A restore preflight of a catalog point judges the archive as the runner will, and reads only that point's own receipt (FX-14)

**Changed.** A restore preflight of a point-bound plan read the set's CURRENT
manifest alone, so it could answer `ready` over a set written again after the
point was signed. Now `archive.backupSet` makes the runner's own comparisons
(the receipt's digest and point id, the attested manifest digest, the set, the
pinned version and the current digest). A set written again is `notReady`,
**`ManifestSuperseded`**; any other disagreement **`PointBindingMismatch`**; an
unreadable pinned version keeps the store's code (`AccessDenied` for a 403,
with a remedy naming `s3:GetObjectVersion`); a pin the bucket cannot check
passes on the digest with `PointPinUnchecked`. `archive.coverage` and
`archive.segments` are `BlockedByPrerequisite` behind a refusal. The preflight
does not check the
receipt's signature; the runner does (`PointUntrusted`, exit 3). **A preflight
runs before any approval, so its reads are confined:** nothing is opened unless
`source.backup` is the set it reads and the receipt key is exactly
`logweir/backups/<backupId>/<run id>.receipt.json`. An id with a `/`, a
control character, a non-ASCII character or a character a store path rewrites
(`%`, `?`, `#`, `*`, `~`) is refused with nothing read; a space is allowed. No
answer repeats a digest, a version id or a receipt field. An absent receipt and
one with other bytes get one answer where the store answers 404. On AWS S3,
with the minimal `archiveRead` grant, an absent receipt reads `AccessDenied`.
[UNVERIFIED — needs a real AWS S3 bucket and a credential source]
Three descriptions now say a `sample.records_restored` (and the API's
`recordsRestored`) counts the records READ BACK for the sample.
**Do:** roll the controller and the runner image together: an older
controller reports a result carrying either new code as `ResultUnreadable`. If
you restore catalog points with an `archiveRead` grant narrower than the
catalog-sync row, add `s3:GetObject` on `<bucket>/logweir/backups/*` and, on a
versioned bucket, `s3:GetObjectVersion` on `<bucket>/<prefix>/*`
([kubernetes.md](kubernetes.md) §7a, *The read of a pinned version*; §21.8).
**Scope:** the preflight and the runner's binding held to agree over one archive each, read-counting store rows for every refused key, the key-segment rule, 35 mutants, and live compose rows on a versioned MinIO bucket; a catalog restore's preflight is the PoC upgrade's.
**Rollback:** an older runner reads the current manifest alone again; an older
controller reports a newer runner's refusal as `ResultUnreadable`. Nothing is
stored.

#### 58. A `RetentionPolicy`'s status says what the per-run ceiling held back, and no longer counts it as kept (FX-22)

**Changed.** A retention plan names at most `maxDeletionsPerRun` points (50 by
default and for every `Report` policy), and the points beyond were listed as
`kept`. Now:
- `status.lastEvaluation` carries counts that add up: `pointsEvaluated` =
  `keptCount` + `candidateCount` + `truncatedByCap` + the points in `skipped`,
  with the `maxDeletionsPerRun` the evaluation applied. A held-back point is in
  no list.
- `kubectl get retentionpolicy` prints a `HELD-BACK` column **between
  `CANDIDATES` and `EVALUATED`**, so `EVALUATED` and `AGE` each move one column
  right.
- When no due point fits the ceiling (a set is planned whole or not at all), the
  plan is empty and `Enforced=False` carries the new reason
  `NothingFitsCeiling`, where it read `NothingToDo`.
- The product API (`lastEvaluation.keptCount`, `truncatedByCap`,
  `maxDeletionsPerRun`) and the console's panel read the same numbers.
  `lastEvaluation.accounting` is `Recorded` or `NotRecorded` (the default); a
  `NotRecorded` answer publishes no kept count and no `kept` rows. An absent
  `kept` beside `Recorded` and `keptCount: 0` means nothing is kept.
- `status.lastEvaluation.viewIncomplete` says the catalog view was not the whole
  archive (`RecoveryCatalog.status.truncated`, or `status.cursor.complete:
  false`).
- The API's `lastEnforcement.failed` says `failedTruncated: true` when cut at
  200 rows.
What a plan contains, its `planSha256` and what a run deletes are unchanged
([kubernetes.md](kubernetes.md) §7f, [api.md](api.md)).
**Do:** apply the CRDs before the controller rolls (four additive `status`
fields and one printer column). If `HELD-BACK` is above 0, raise
`spec.enforcement.maxDeletionsPerRun` (1–500) to clear the backlog in fewer
runs; under `NothingFitsCeiling` nothing is removed until you do. If
`viewIncomplete` is `true`, let the catalog's sync finish or raise its
`spec.sync.viewLimit` (100–5000). Update any script that reads `kubectl get
retentionpolicy` by column position, and any API client that reads
`lastEvaluation.kept`: read `lastEvaluation.accounting` first.
**Scope:** evaluation rows at the PoC's size (371 points at `keepLast` 300 and 10), controller, API and console rows over one chain of fixtures, and mutants; the PoC's `Report` policy is the upgrade's.
**Rollback:** an older controller lists the held-back points under `kept` again
and leaves the four new fields stale; the API then answers `accounting:
NotRecorded` once the `kept` list is not `keptCount` long, and the console
prints "not recorded". Re-applying the older CRDs prunes the fields and the
`HELD-BACK` column. An older controller writes `Enforced=False/NothingToDo`
where this build writes `NothingFitsCeiling`. Plans and approvals are
unaffected.

#### 59. The shared console shows what the product API publishes and sends what its routes require: the schema note, a restore's integrity level, an original-name restore (FX-48)

**Changed.** Three things the product API publishes never reached a page of the
shared console, and one request never left it:
- **Item 46's note** now shows on the catalog page and the restore review:
  **"Registry not captured: applications may not read these records after
  restore."**, with each topic, its sides and its ids.
- **A Restore's Integrity table:** `level` shows the scorecard's word
  (`byte-fingerprint` or `consume-only`) for a restore that passed, from
  `completion.integrityLevel`. Cells no route publishes (the integrity
  `result`, `partial reason`, objectives, measured values, target topic
  preflight, old topics) now read **not published by the product API**, with a
  sentence saying the values are on the `Restore` object and in the signed
  scorecard, where `-` read as "none recorded".
- **An original-name restore (item 56)** can now be submitted: the console
  sends `topicNaming.originalName` and the typed names, and shows the approval
  subject the API publishes.
Nothing in the API, the controller, the runner, a CRD or a signed document
changes.
**Do:** nothing. With an older console, read a point's schema dependency from
`topics[].schemaDependency` on `GET .../catalogs/{name}/points` or from its
receipt, and create an original-name `Restore` through the API.
**Scope:** console rows through its own client over a stubbed transport, five mechanical checks between `schemas/logweir-api-v1.openapi.json` and the page, and 28 mutants; batch 6's failed rows re-run at the next PoC upgrade.
**Rollback:** an older console shows `-` and no schema note again and cannot
submit an original-name restore; nothing is stored.

#### 60. A readiness check asks the endpoint what it can do; compatibility is a tested contract (PROD-01.2)

**Added.** A `Backup` or `Restore` `Preflight`, and `logweir check run` on the
same plan, carry capability rows read from the endpoint's own answers
([kubernetes.md](kubernetes.md) §21.6c). `connection.engineProtocol` (backup)
and `target.engineProtocol` (restore) are **blocking**: the engine sends each
request at one fixed version, and these rows name each request the endpoint's
ApiVersions does not serve. `connection.topicConfigsReadable` and
`connection.groupTypes` are **advisory**. The controller lists the rows in the
check plan (`capabilityChecks`). The ApiVersions rows answer only when every
broker the cluster lists, and every bootstrap address, answered within 10 s;
otherwise `unknown` ("2 of 3 broker(s) the cluster lists answered …"), never
`ready` (`brokersAnswered` counts brokers). **The compatibility contract** is
in [support-matrix.md](support-matrix.md#the-compatibility-contract): Apache
Kafka 3.7.1, 3.9.2, 4.1.2 and 4.3.1 and Confluent Platform 8.3.2 are supported;
**Redpanda v26.2.4 is a backup source only** (it serves Produce v0–v7, the
engine sends v8); no managed provider has been run against.
**Changed, to the safer side**
([stability.md](stability.md#an-endpoints-capability-is-read-from-the-endpoint-and-a-value-it-did-not-report-is-not-recorded-prod-012)):
`target.timestampBound` is `unknown` (`TimestampBoundNotReported`) for a target
that reports no bound key; `Restore.status.topicPreflight.timestampType` is
absent for a target that did not report it. `connection.authenticated` says
when the bootstrap address answered and the advertised brokers did not. An
object store's `404
XAdminUserNotFound` is a refused credential. An object store's answer is read
from its HTTP status and its error document's `<Code>`, never from the error
text (a bucket named `expiredtoken-archive` no longer moves a class); a retried
transport failure reads `EndpointUnreachable`, a retried 5xx unclassified. On
Apache Kafka 3.7 every `Backup` check carries the advisory
`connection.groupTypes` warning.
**Do:** roll the runner image with the controller: an older runner refuses a
plan listing `capabilityChecks` (`Preflight` `Failed`,
`CheckContractMismatch`). Move **`runnerImage`** with `controllerImage` (a
pinned `runnerImage` survives `helm upgrade --reuse-values`). A failed or
in-flight `Preflight` stays `Failed` or reads `unknown`: create a new one after
both images moved ([kubernetes.md](kubernetes.md) §21.9). Give the restore
identity `Read` on the target topics, `Describe` on the scratch marker topic
and `Delete` on the scratch prefix
([support-matrix.md](support-matrix.md#minimum-permissions)); without `Delete`
a scratch restore signs `pass` and leaves its topics, saying so. Take a
decommissioned address out of a `KafkaCluster`'s `bootstrapServers`.
**Scope:** unit rows per row and code, librdkafka's mock cluster (one and three brokers), the store classifier on a loopback responder, a guard over the published tables, and compose rows (`e2e/tests/compat_contract.rs`) on each supported line, Redpanda, SeaweedFS and the ACL profile; CI runs the 3.7.1 row.
[UNVERIFIED — no controller has rendered a capability plan to a check Job on a cluster yet; the PoC upgrade that carries this item runs a Backup and a Restore Preflight.]
**Rollback:** roll the controller and the runner back together. Under an older
controller this runner answers no capability row and reports an unreported
bound as `BrokerConfigsNotReadable`, a code the older controller reads; an
older runner answers `ready` for an unreported bound again and writes
`timestampType: CreateTime` again. No signed document or CRD schema changed.

#### 61. A guard-refused Restore or Backup says why in its status (FX-34)

**Changed.** When a `Restore`'s or a `Backup`'s runner exits 3, the `Failed`
condition's message now ends with the runner's own reason code and sentence:

```
the runner exited 3 (guard-refused); the code was read from status.containerStatuses[name=runner].state.terminated.exitCode; the runner's own reason, cleaned and bounded: GuardRefused: source.storage.endpoint is a plain http:// endpoint but source.storage.allow_http is false. … Set allow_http: true to state plaintext explicitly, or use an https:// endpoint.
```

`status.progress.message` carries the same text and the console's operation
page shows it; `status.exitReason` and the condition's `reason` are unchanged.
`logweir restore run` and `logweir backup run` print one more stdout line at
every exit 3, `refusal-detail={"token":"…","code":"…","message":"…"}`, just
before `refusal-reason=`, which stays last
([stability.md](stability.md#refusal-detail-carries-a-guard-refusals-reason-code-and-sentence-fx-34)).
The controller treats the log as untrusted: it gives each `Restore` and `Backup`
Job a fresh random **line token** as its last two arguments (`--line-token
<hex>`) and reads a reason only from a line carrying that token, with a code
from the kind's closed list. The sentence is cleaned (printable ASCII, a line
break becomes a space, other characters `U+FFFD`, URL queries and credential
shapes `[redacted]`) and cut to 760 bytes
([kubernetes.md](kubernetes.md#what-a-refused-run-says-about-why-fx-34)).
For exit 3 the controller reads the `runner` container's last 32 lines (512 KiB)
once; if the pod is gone or the read answers `403` or `500`, the object is still
`Failed` with `exitCode: 3` and says the reason could not be read. The check
and retention Jobs are unchanged.
**Do:** move `controllerImage` and `runnerImage` in ONE `helm upgrade`. **Every
runner image published before this change, the images of this same unreleased
entry included**, does not know `--line-token` and stops while parsing its
arguments. One `helm upgrade` of the packaged chart cannot produce that pair;
two pinned tags of which only `controllerImage` moves, floating `:latest`
pulled at different moments, or a `runnerImage` pinned apart (a mirror, an air
gap) can. **What it looks like:** each `Restore` and `Backup` ends `Failed`
with `exitCode: 1`, `exitReason: operational` and no cause on the object; the
runner pod's log opens with `error: unexpected argument '--line-token' found`;
nothing ran. Set `runnerImage` to the image published from the controller's
build ([kubernetes.md](kubernetes.md#the-runner-image-must-be-at-least-as-new-as-the-controller)).
For a tagged release's runner the roll was already required by item 35. An
alert that compared a refused run's message for equality should compare its
opening text.
**Scope:** grammar, token and cleaning rows, rows over the shipped runner binary, controller rows over a recording API double against goldens captured before the change, and console rows; the live rows are the next PoC upgrade's.
**Known limit.** The token separates the runner's line from every plan, but
whoever can read the Job can read it, and the Kafka client's own stderr lines
are not escaped. The other lines a controller reads (`refusal-reason=`,
`failure-reason=`, the evidence keys) carry no token yet.
**Rollback:** roll both back together, or the controller first. An older
controller passes no token and ignores the new line. The runner image first
leaves THIS controller over an older runner, which does not start.

#### 62. The engine's metrics port stays closed; a failed RetentionPolicy read says so (FX-27, FX-42)

**Changed.** Every backup and restore document Logweir renders ends with
`metrics: {enabled: false}`, so the engine's unauthenticated Prometheus server
no longer listens on `0.0.0.0:8080` in a runner pod or on a CLI host. A
schedule's retention panel read a failed read of the namespace's
RetentionPolicies as "no policy" and printed "Logweir never deletes from your
archive"; it now says the read failed and why.
**Do:** nothing.
**Scope:** a render row per renderer and the ten goldens, the real-engine backup row on compose holding port 8080, and console rows for a 403, a 500 and a malformed answer.
**Rollback:** an older runner opens port 8080 again during a run; an older
console prints the no-deletion sentence after a failed read.

#### 63. A restore started from a Backup is bound to its recovery point (FX-35)

**Changed.** The restore wizard opened on a `Backup` (*Restore this point* on
the History, Backups and Schedules pages) built a plan naming `source.backup`
only, so the runner read no receipt and a 1.3.0 point signed each topic's
timestamp type NOT RECORDED. The wizard now finds the run's point in a catalog
over the run's own destination and builds the catalog flow's plan for it (the
same `source.point`). When no catalog lists the point yet, the plan names the
set only and the review says it is not bound and why. The review's new
*recorded by the point* row says where the run reads each topic's timestamp
type and configuration ([kubernetes.md](kubernetes.md), *A restore started
from a Backup is bound the same way*).
**Do:** nothing. Keep a `RecoveryCatalog` over a Backup's destination synced
since the run to have its restore bound.
**Scope:** console rows over the product API's own documents (`ui/tests/backups-page-binding.spec.js`) and ten mutants; the PoC restore from the Backups page is the upgrade's.
**Rollback:** the console image; an older console builds the set-only plan
again.

#### 64. A retention policy that keeps or skips more than 500 points keeps enforcing, and an `Enforce` run never deletes from a partial or expired catalog view (FX-39, FX-40)

**Changed.** `status.lastEvaluation`'s lists (`kept`, `candidates`,
`protected`, `skipped`) are bounded at 500 by the CRD, and a policy that kept or
skipped more (hourly backups under `keepDays: 30` keep 720) had its status write
refused and stopped enforcing. Each list now holds its first 500 entries, with
new counts `protectedCount` and `skippedCount` beside `keptCount` and
`candidateCount`. An `Enforce` policy starts no run from a view that may not
hold every point (a window, an unfinished walk, a catalog that did not say:
`Enforced=False/ViewIncomplete`) or that is past, or states no,
`status.viewExpiresAt` (`Enforced=False/ViewExpired`); nothing is deleted. A
`mode: Full` catalog publishes a sync resumed from its cursor as a window
(`truncated: true`). **On v1 an archive of more than 5000 points, a view the
catalog cut for page space, and a Full catalog whose walk does not finish in one
sync are never enforced.** Under both reasons `status.enforcement` and
`guarantees.ageExpiry` still read `LogweirWorker` and `LogweirEnforced`; the
`Enforced` condition is the authority. A run from a complete, current view
deletes what it did ([kubernetes.md](kubernetes.md) §7f).
**Do:** apply the CRDs before the controller rolls (two additive `status`
fields). Before the upgrade, read `status.lastEvaluation.viewIncomplete` of
every `Enforce` policy: where it is `true`, the policy stops deleting until the
catalog's view is whole. Re-create a `RecoveryCatalog` whose
`spec.sync.viewLimit` cut the view with a larger one (`spec.sync` is immutable;
5000 at most); a stopped walk's `Synced` condition says why; a `mode: Full`
catalog needs a `maxObjectsPerRun` that finishes in one sync, or `mode: Index`.
**Scope:** controller rows over a fake API (720 kept and 600 skipped accepted by the generated schema, each refusing view started nothing, plan bytes identical over FX-22's 437 fixtures) and catalog rows; the PoC's counts are the upgrade's.
**Rollback:** an older controller writes the lists whole again (a policy over
500 stops enforcing again) and starts runs from a partial view; the stale
`skippedCount` then reads "not recorded" once the counts no longer add up.
Re-applying the older CRDs prunes the two fields.

#### 65. One backup names at most 1,000 topics, and a backup of many topics stays listed and verified (FX-33) — required action

**Changed.** A backup of about 80 topics wrote a catalog record over the walk's
256 KiB and the point **vanished from the catalog**; one of about 300 topics
wrote a receipt over the controller's 1 MiB and was **never verified**. One
budget now bounds what a backup signs ([kubernetes.md](kubernetes.md) §7b.5):
- **One backup names at most 1,000 topics**, at a budget of
  5,000 bytes a topic. The largest receipt is 5,131,072 bytes and the largest
  catalog record 6,131,072 bytes; a topic at broker defaults measures 3,090
  bytes, at most 4,003 after the engine.
- **A selection over it is refused before the engine runs** with
  `BackupSelectionTooLarge`: at `logweir backup run`'s phase −1, at the
  controller's freeze, in a schedule's run policy (`Ready=False`) and in the
  dynamic selection, and by the bytes the run could sign. Nothing is left out to
  make a selection fit.
- **The catalog walk, the controller and the evidence relay read a receipt
  under the same 5,131,072 bytes**; a scorecard keeps 1 MiB.
- **The catalog lists every point it counts.** A point that is not available
  says why, with its size against the bound; a point whose record could not be
  read is listed by point id, never selectable.
The maximum is lower than it was: a dynamic selection resolved up to 5,000
names, and a 5,000-topic receipt (about 15 MB) no relay carries.
**Do, before the upgrade:** find every `Backup` and `BackupSchedule` that
names, or last resolved to, more than 1,000 topics with the two commands in
[kubernetes.md](kubernetes.md#upgrade), and split each across schedules; after
the upgrade such an object is **refused by name at each run**. An older point
over its bound (about 1,650 topics or more) is listed `Unreadable` and not
offered; `logweir restore` and `logweir verify` still read it.
**Do, after the runner image rolls:** set each `RecoveryCatalog`'s
`spec.syncRequest` to a new value, so points of about 80 topics and more are
listed.
**Scope:** runner, check and controller rows (`topic_budget.rs` in `logweir` and `weirkeeper`) with child-process memory measurements, and a live compose backup of 500 topics listed, relayed, verified by both readers and restored; no signed format changes.
[UNVERIFIED — no cluster has run this build: the controller and API rows are unit and mock-cluster rows, and the live rows ran the runner and the controller's verification code against the compose stack's real documents. The PoC upgrade that carries this item runs the controller itself.]
**Rollback:** an older runner accepts more than 1,000 topics again and an older
controller reads a receipt under 1 MiB again, so a backup of about 300 topics or
more is `NotAttempted` again. Roll the runner and the controller together: this
controller asks an evidence-fetch Job for up to 5,131,072 bytes, and an OLDER
runner refuses any request over 1 MiB, so under that pair no backup the
controller cannot read itself is verified (`NotAttempted`). An older controller
with this runner works as before.

#### 66. Two-person approval in the console: the second person signs in and clicks Approve, no key (PROD-16.2)

**Added.** A third approval mode, `two-person`
([kubernetes.md](kubernetes.md#two-person-approval-in-the-console-prod-162) §8):
a namespace bound to `approvalPolicy.policies[].mode: two-person` runs a restore
after a SECOND person, signed in to the shared console as an Approver and not
the requester, opens the Restore's approval page and clicks Approve. Nobody
holds a key. The console verifies its own signature on the request, shows the
whole scope, and signs the same document with `approver` and `approvedAt`
added (authorization document **2.2.0**) with its `ConsoleConfirmation` key;
the controller and the runner re-check the signature, the binding, that the
approver is a second person (issuer and subject, case ignored) and the window.
The chart renders `mode: Governed` with `approverSignature: Console`, only with
`api.console.mode: shared`. A scorecard of such a run is **1.9.0** (or
**2.1.0** for a partition subset) with `approval.console`;
`verify_scorecard.py` 1.29.0 prints who asked and who approved. Product API:
`GET .../restores/{name}/approval-request` and `POST
.../restores/{name}/console-approval` (OpenAPI additive).
**Do:** nothing, until you bind a namespace `two-person` (`docs/install.md`
§5f): bind the approver the console role Approver there. A two-person request
names at most 1024 topics; split a larger restore or bind it `strict`.
**Scope:** core, controller, runner, API and console rows for every must-have and its negative control, a 32-case corpus with the parity script, and mutants; the PoC rows are in [PROD-16.2-console-approval.md](to-do/decisions/PROD-16.2-console-approval.md).
**Rollback:** remove the two-person policies first: an older controller or
console refuses to start on `approverSignature`, and an older runner refuses
every 2.2.0 document; a pending console-approved Restore is then refused and
submitted again. Scorecards stay valid under older readers. SECURITY.md states
the residual: whoever controls the console, its key or the identity provider
can produce both halves.

#### 67. An archive holding transaction markers is refused unless the plan allows them (PROD-01.1a)

**Changed.** The engine archives every transaction commit and abort marker as
an ordinary record, and a restore wrote each one, with the records of aborted
transactions, as data. Before it creates anything, a restore now reads the
segments its window overlaps and every segment after the window's end, and
counts records shaped like a marker. Any marker refuses the restore, as a drill
or as a `newTopic` restore: exit 3, `refusal-reason=TransactionMarkersInArchive`,
the sentence naming the topics, partitions and count (on the `Restore` through
item 61's `refusal-detail=` line). The plan field
`restore.allow_transaction_markers: true` (inside `plan_hash`) restores the
archive as before, and the scorecard's `sample.coverage_note` says the check was
skipped. An unreadable segment after the window does not refuse the run; the
note counts it. A user record of the same shape is counted too. Not detected: a
marker not archived, a transaction opened after the backup began, a marker in an
unreadable segment after the window or in a segment before a stated window
start, and another marker encoding version
([stability.md](stability.md#an-archive-holding-transaction-markers-is-refused-unless-the-plan-allows-it)).
**Do:** nothing, unless you restore transactional topics (Kafka Streams or
Connect exactly-once, Flink): add `restore.allow_transaction_markers: true` to
the plan and approve the changed plan.
**Scope:** shape-predicate rows, the whole run over doubles in both target modes, and live compose rows with engine `0.23.3+logweir.2` (`e2e/tests/record_semantics.rs`: 7 markers refused, the non-transactional twin and the override `pass`); one ordinary point restored unchanged is the PoC upgrade's.
**Rollback:** an older runner ignores the plan field and restores such an
archive without the check. An older controller's status says "the runner gave
no readable reason" (item 61) while `exitReason` names the refusal: roll the
controller with the runner.

#### 68. A backup of a source whose records all expired is a signed, named empty capture (PROD-02.1)

**Changed.** A scheduled backup of a topic retention had emptied failed exit 1
"operational". Now `logweir backup run` reads every named partition's low and
high watermark before the engine and records them in the receipt (format
**1.8.0**, optional `source_watermarks`). When nothing was captured and every
partition was empty, the run exits 0 as an **empty capture**: the signed
receipt counts no record and carries the empty window `[t, t)`, no catalog
point is written (`catalog sync` counts it `empty-capture`), and the `Backup`
completes with reason `EmptyCapture` once the controller verifies the receipt,
with no `windowCovered`. An empty capture is not a recovery point: a
`ProtectionPolicy` never counts it, and a restore of its set is refused by
name, exit 3. A partly empty source is an ordinary backup that names its empty
partitions. Nothing captured over a partition that held records, or with the
watermarks unread, is still exit 1. Both verifiers (`verify_scorecard.py`
1.30.0) check arms 41–45 and print "coverage not recorded" for a receipt
without the block. The block is recorded only up to 32 KiB (about 430–650
partitions); a larger source's empty capture stays exit 1.
**Do:** nothing.
**Scope:** `e2e/tests/empty_capture.rs` on compose with the shipped engine, runner, catalog, controller and receipt rows, the corpus and `scripts/check-verifier-parity.sh`; the `Backup` status over an emptied source is the PoC upgrade's.
**Rollback:** an older runner fails the empty source exit 1 again; receipts
already signed keep verifying with this build, and an older verifier refuses an
empty capture's receipt (arm 4), the safer verdict.

#### 69. A failed restore removes or names the scratch topic it created (FX-44)

**Changed.** A restore that fails after its creation step completed (phase 6
on, exit 1, 3 or 4) now tears down the topics that step created by phase 9's
rule: deleted in `scratch` mode under `teardown: delete`, never in `newTopic`
mode, never a topic it did not create. It logs `the restore failed after it
created its target topics; teardown removed 1 scratch topic: <topic>`. A topic
the broker would not delete is named by the teardown warning (with the grant
when the broker refused authorization: ``…; the restore principal lacks
`Delete` on topic `<topic>` ``) and by a new stdout line, `teardown-failed=`,
which the controller copies to `Restore.status.teardown.failed`; the API's
`teardown` and the console's Teardown section show it.
**Do:** nothing. Give the restore identity `Delete` on the scratch prefix.
**Scope:** unit rows, controller rows over a recording API double, and the compose ACL row with `Delete` removed.
[UNVERIFIED — no controller has read a `teardown-failed=` line on a cluster yet; the PoC upgrade that carries this item runs it.]
**Rollback:** roll back the runner and the controller in either order. An
older controller ignores the line; an older runner prints none and leaves a
failed run's topics.

#### 70. The console's "Restore under the original topic names" box takes effect when ticked (FX-49)

**Changed.** Ticking *Restore under the original topic names* in the restore
wizard, or its owner statement, now redraws the wizard at once (the owner
statement, the locked *Verify every record* and the plan appear). The approval
panel now says a `newTopic` restore's topics stay on the target cluster.
**Do:** nothing.
**Scope:** console rows (`ui/tests/original-name.spec.js`, `ui/tests/console-approval.spec.js`) driving the mounted wizard by `change` events.
**Rollback:** the console image; an older console needs another field edited
after the tick.

#### 71. A catalog's `viewLimit` window holds the newest points, and a partial view says so (FX-50)

**Changed.** A `RecoveryCatalog` over more points than `spec.sync.viewLimit`
listed the first `viewLimit` points its walk met; a `Full` rescan meets them in
point-id (digest) order. The runner now keeps the newest `viewLimit` points
with a record in either mode; a tie goes to the smaller point id. Counts and
the histogram still cover the whole walk. The `Synced` message of a view from a
resumed `Full` rescan ends `It is a partial view: <n> point(s) from a resumed
rescan; the archive holds more`, and that of an unfinished walk `… from a walk
that did not finish; the archive may hold more`. `status.truncated` and
`status.cursor` are unchanged, so an `Enforce` run still refuses both views
(item 64).
**Do:** nothing. A `Full` catalog lists its newest points from its first sync
after the runner image rolls; set `spec.syncRequest` to a new value to see it
at once.
**Scope:** runner rows over an in-memory archive (386 points, a tie at the edge) and controller rows over a recording API double.
[UNVERIFIED — no PoC catalog has listed its newest points under this build yet; the PoC upgrade that carries this item runs it.]
**Rollback:** roll back the runner and the controller in either order. An
older runner lists the first points its walk meets; an older controller drops
the sentence.

#### 72. CI tests on Apache Kafka 4.3.1, in four shards; nothing in the product changes (PROD-01.5a, FX-41)

**Changed.** Only how the repository is tested. CI's e2e default broker is
Apache Kafka 4.3.1 instead of 3.7.1. 3.7.1 stays a legacy line:
`KAFKA_VERSION=3.7.1` (or `e2e/compose/stack-env.sh --kafka 3.7`) starts it, and
the weekly `engine-matrix.yml` still runs the whole suite on it. The e2e job runs
in four shards by test binary, and `check` fails when a binary is in no shard or
in two.
**Operator action:** none. No binary, image, chart value or signed document
changes ([support-matrix.md](support-matrix.md#broker-versions-apache-kafka)).
**Scope:** the e2e package on 4.3.1 with the `auth` profile, `full_drill` on 3.7.1, and the shard guard with its two controls.
**Rollback:** revert the change; nothing deployed is affected.

#### 73. Scheduled rehearsals and standing authorizations are withdrawn (PROD-17.1 C1)

**Changed.** The `RehearsalSchedule` kind, its reconciler, CRD and RBAC, the
product API's `GET .../rehearsal-schedules[/{name}]` with its `rehearsals`
capability flag, and `logweir drill approve --standing` are removed: a standing
authorization needs a personal approver key, which v1 does not ship (OD-12,
after OD-8). A `Restore` carrying `spec.authorization` is refused
`StandingAuthorizationRefused` before its `Approval` is read and before any Job
exists, and an `Approval` whose `spec.subjectRef.kind` is `RehearsalSchedule`
is refused `StandingAuthorizationRefused` and never `Verified`. Both fields stay
in the CRDs, so objects written before v1 still parse. `drill approve
--standing`, `--subject-kind RehearsalSchedule`, `restore run
--standing-authorization` and `--authorization-keys` are usage errors, refused
by name; a Job whose execution contract names the `standing` kind is refused,
exit 3. Every standing authorization already signed (formats 1.0.0 and 1.1.0)
and every scorecard a rehearsal signed still verify.
**What an upgraded install sees.** `helm upgrade` leaves the
`rehearsalschedules.logweir.dev` CRD (Helm never deletes `crds/`), and its
objects stay, unreconciled. A rehearsal `Restore` that had not started is
refused; one whose Job was running finishes; a finished one keeps its status.
To remove them:

```
kubectl --context "$LOGWEIR_CONTEXT" get rehearsalschedules -A
kubectl --context "$LOGWEIR_CONTEXT" delete crd rehearsalschedules.logweir.dev
```

Deleting the CRD deletes every `RehearsalSchedule` with it.
**Do:** before the controller rolls, list `RehearsalSchedule` objects with the
first command. Run restore tests as per-run `Restore`s, which the console
confirms.
**Scope:** controller rows over a recording API double (each refusal with no `POST` and no `Approval` read, and its control), CLI rows through the shipped binary, and the regenerated CRDs, chart renders and OpenAPI document.
[UNVERIFIED — no live install has run this build's refusals yet; PROD-14.1's clean install carries them.]
**Rollback:** roll the chart, the controller and the runner back together.
Helm does not re-create a deleted CRD: if you deleted it, re-apply the older
chart's `crds/` (`kubectl apply --server-side --force-conflicts -f
charts/logweir/crds/`) before rolling the controller back. Its controller then
resumes any schedule left in the cluster, under standing documents that still
verify until they expire.

#### 74. Withdrawn for v1: alert sinks, the drill's notification post, an unused ACL read and phase 7's engine validation call (PROD-17.1)

**Withdrawn** (owner decision OD-12); each part did nothing v1 uses.
- **Phase 7 no longer calls the engine's `validation run`**, which the engine
  never implemented. No verdict changes; `engine_subreport` stays `null`.
- **Alert sinks.** A `ProtectionPolicy` delivers no alert to PagerDuty, Slack
  or a webhook: no delivery Job, no protection event, no `logweir notify
  deliver`, no sink fields, no `credentialBindings` status and no chart
  `notify.allowInsecureSinks`. Health, the objectives, the alert ledger
  (`status.alerts`) and the console's `#/protection` page stay; the page's
  delivery columns and the API's `notifications`, `notifiedTransition` and
  `delivery` members are removed. A policy that still carries
  `spec.notifications` parses (the API server keeps the key and drops its
  contents), and its `NotificationsDelivered` condition reads `False` /
  `NotificationsWithdrawn`, naming the field. A values file that sets `notify`
  is refused at render; `scripts/bind-credential.py` refuses `--kind
  ProtectionPolicy`. A delivery Job an older controller left finishes and stays:
  `kubectl delete jobs,configmaps -n <ns> -l app.kubernetes.io/component=notification,app.kubernetes.io/managed-by=weirkeeper`
  removes them and their event ConfigMaps.
- **The drill's own notification post.** A run no longer posts to a webhook,
  Slack or PagerDuty. The plan's `notifications` block still parses (it is
  inside signed plan bytes): absent or empty (`webhooks: []`) is accepted, and a
  block that names a sink is refused by name by the controller's admission and
  by the runner at startup (exit 3, `refusal-reason=GuardRefused`); phase 0
  repeats the check. Alert on the runner's metrics textfile or the `Restore`'s
  conditions instead.
- **The ACL read** (item 37's `DescribeAcls` and its `unsafe` wrapper); nothing
  called it. Consumer positions are unchanged.
**Do:** remove the `notify` block from your values before `helm upgrade` and
the sinks from every plan that names one (then approve it again), apply the
CRDs, and remove `spec.notifications` from each `ProtectionPolicy` when
convenient.
**Scope:** runner, controller, API, chart and bind-credential rows each with a control, the plan refusal at admission, phase 0 and the binary, and the unsafe-scope gate.
[UNVERIFIED — no cluster has applied the narrowed ProtectionPolicy CRD over an older policy yet; the PoC upgrade that carries this item runs it.]
**Rollback:** roll back the chart, the CRDs, the controller and the runner
together. The older CRD types `spec.notifications` again, but the API server
already dropped its contents, so write each route again; an older runner posts
a plan's notifications again.

#### 75. Surfaces withdrawn for the v1 cut (PROD-17.1, OD-12)

**Changed.** `POST /api/v1/namespaces/{ns}/destinations:from-legacy` and the
console's *Adopt an existing archive location* form are gone; the route could
only refuse. It now answers `404 not_found`, and the problem codes
`legacy_location_unknown` and `legacy_location_mismatch` are no longer
published. A legacy schedule's archive keeps working. The legacy `kubectl
proxy` page is withdrawn too (C4): the chart's `ui.*` values and their objects,
the `logweir-ui` image (main CI publishes three images, and the chart names
three), the laptop `just ui` path and the page's legacy mode in `ui/`. The page
is served by the console (`api.console.*`) only. The chart refuses any `ui`
block by name, `ui.* is withdrawn`. After the block is removed, `helm upgrade`
deletes the `<release>-ui` Deployment, Service, ServiceAccount, ClusterRoles and
RoleBindings. `scripts/helm-demo.sh` step 9 fetches the console's page and
session through a port-forward.
**Do:** create a destination explicitly with `POST .../destinations` (or the
console's *Create a destination* form), naming the bucket, prefix and endpoint
your runs used. Remove `ui:` from every values file; for a page, enable the
console (`api.console.enabled`, [install.md](install.md) §5e). Delete any
`<release>-ui` RoleBinding created by hand in another namespace:
`kubectl delete rolebinding <release>-ui -n <namespace>`.
**Scope:** product API rows (route table, role matrix, published document), console contract rows, the chart gate's named refusal of `ui.*` with its control, and the console UI suite.
[UNVERIFIED — no install has been upgraded across this item yet; PROD-14.1's clean install walks helm-demo step 9.]
**Rollback:** the product API and console images, and the previous chart for
the page; an older API serves the route and refuses as before, and the
published `logweir-ui` images stay pullable.

#### 76. The Helm chart is the only install (PROD-17.1)

**Changed.** The static `kubectl apply` bundle `logweir.yaml`, its renderer
`scripts/render-install.sh`, the walks that applied it (`just k8s-demo`,
`just laptop-demo`, `scripts/kind-demo.sh`, `scripts/demo-steps.sh`), the v0.1
drill demo (`scripts/demo.sh`, `scripts/demo-approve.sh`) and quickstart Paths 2
and 4 are withdrawn (OD-12): the bundle had no signing identity, console or
approval key. `just mvp-demo` and `scripts/helm-demo.sh` stay, and the bundle's
three checks moved into `scripts/check-chart.sh`. The kustomize `config/` tree
goes too (`config/manager`, `config/rbac`, `config/overlays`, `config/crd`):
`charts/logweir/crds/` is the one CRD copy (`just crds` writes it, `just
crds-check` holds it), the chart's roles carry every grant `config/rbac` did,
and the two per-namespace ServiceAccount fragments moved to `config/samples/`.
**Do:** nothing on a chart install. An author-only install from
`config/overlays/local-images` uses
`-f charts/logweir/examples/author-only.values.yaml`; a script that applied
`config/rbac/backup-runner-serviceaccount.yaml` or
`retention-serviceaccount.yaml` applies them from `config/samples/`. An install
made with `kubectl apply -f logweir.yaml` keeps running with no newer bundle;
move it to the chart. Its CRDs, custom resources, namespace and Secrets stay.
Remove the bundle's control plane, then install the chart into the same
namespace ([install.md](install.md) (c); `identity.enabled=false` keeps the
signer you provisioned):

```bash
kubectl --context <ctx> -n logweir-system delete deployment,serviceaccount,networkpolicy \
  -l app.kubernetes.io/name=logweir
kubectl --context <ctx> delete clusterrole,clusterrolebinding -l app.kubernetes.io/name=logweir
```

A `weirkeeper-policy` ConfigMap you created by hand blocks `helm install`: set
its settings as chart values (`checks.*`, `runs.*`, `engine.*`, `evidence.*`),
then `kubectl --context <ctx> -n logweir-system delete configmap
weirkeeper-policy` before installing. RoleBindings to the five human roles keep
working: the chart recreates the roles under the same names. Helm skips CRDs
that exist; apply a newer set by hand ([the chart
README](../charts/logweir/README.md), *Upgrading the CRDs by hand*).
[UNVERIFIED — the move from a bundle install to the chart has not been run; PROD-14.1's clean install runs the chart path only.]
**Scope:** the chart gate (`just chart-check`) carries the three moved checks; rows that read RBAC, the controller Deployment and the CRDs read the chart's render and `charts/logweir/crds/`.
**Rollback:** check out a tree before this item to get the bundle back; no
image changes.

#### 77. A restore plan that does not parse is refused before its approval is read (PROD-17.1 B5)

**Changed.** The controller reads a `Restore`'s `spec.planBytes` once per
reconcile and hands that one value to every check, the admission, the approval
bundle and the Job. A plan that does not parse as a restore plan now ends the
`Restore` `Failed`, reason `PlanUnparseable`, before its `Approval` is read and
before any ConfigMap or Job exists; with `spec.coverage`,
`spec.completeMaxRecords` or `spec.target.topicNaming.originalName` declared it
is `ExecutionSpecInvalid`, as before. Until now it waited for its approval and
was refused later (`PlanUnparseable`, `ConnectionPlanMismatch`, or by the
runner, `drill spec does not parse`, exit 1). The runner parses the same bytes
into the same type, so no plan it could run is refused.
**Do:** nothing. A `Restore` refused this way needs a new one with a plan the
runner accepts.
**Scope:** controller rows over a recording API double (both kinds of unparseable plan refused with no `POST` and no `Approval` read; the parseable control admitted).
[UNVERIFIED — no live install has run this build's refusal yet.]
**Rollback:** roll the controller back; an older controller waits for the
approval again and refuses such a plan later.

#### 78. A `spec.retention` on a BackupSchedule stops reporting; use a RetentionPolicy (PROD-17.1 B1)

**Changed.** `BackupSchedule.spec.retention{keepLast, keepDays}` stops
reporting, and `status.retentionReport` is no longer written: the report only
printed `aws s3 rm` and `mc rm` suggestions, and v1's deletion path is a
`RetentionPolicy` (OD-12). A schedule that still sets the field keeps backing
up; the field stays in the CRD, the controller ignores it, and the schedule
carries the condition `RetentionReportWithdrawn=True`, reason
`UseRetentionPolicy`, message `spec.retention no longer reports; use a
RetentionPolicy`. The product API's schedule view drops
`status.retentionReport` (the `RetentionReportView` schema is gone); the create
and edit routes still accept `retention`, stored and ignored. The console's
retention panel points at a RetentionPolicy. The console's schedule create form
and Future policy panel no longer offer `keepLast` and `keepDays`; they show
the RetentionPolicy pointer instead, the console never sends `retention`, and
saving the policy of a schedule that still carries `spec.retention` removes it,
with its `RetentionReportWithdrawn` condition. After the CRD upgrade the API
server drops a stored `status.retentionReport`.
**Do:** nothing is required. For pruning advice on a destination, create a
`RetentionPolicy` in `mode: Report` ([kubernetes.md](kubernetes.md) §7f).
Remove `spec.retention` from a schedule to clear the condition.
**Scope:** a controller row over the recording store with its control, a console row, and the regenerated CRDs and OpenAPI document.
[UNVERIFIED — no live install has run this build's condition yet; PROD-14.1's clean install carries it.]
**Rollback:** roll the chart's CRDs, the controller and the console back
together; the older controller writes the report again on its next reconcile.

#### 79. A refused retention run says why on its RetentionPolicy (FX-15)

**Changed.** Every exit-3 refusal of `logweir-retention` prints one
`retention-refusal=<code>` line, plus ` variable=<name>` when the refusal is
about one binding variable (its name, never its value), and
`Enforced=False/RunFailed` and `EnforcementDegraded` name it, for example
`(exit 3, CapUnreadable (LOGWEIR_RETENTION_MAX_OBJECTS))`. Until now only
`CredentialBindingMismatch` was named; every other refusal read "REFUSED its
plan", and the reason was only in a pod log the Job's TTL collects. The 22 codes
are in [reference.md](reference.md#logweir-retention). Nothing about what is
deleted, held or scheduled changes, and no CRD changes.
**Do:** nothing.
**Scope:** controller rows for five refusal classes, each against the line-dropped run (only the two messages differ), worker rows for all 22 codes, a process row over the shipped binary, and six mutants.
[UNVERIFIED — no live install has run this build's refusal yet.]
**Rollback:** roll the controller back; an older controller shows the generic
sentence again, and an older worker prints no line for these refusals.

#### 80. A setting the existing target topic does not report counts as differing in the drill's configuration diff (FX-47)

**Changed.** When phase 3 finds a target topic that already exists, the
signed `target_diff` line lists the source settings that differ:
`differing config: [...]`. A source setting the target does not report (a
Kafka 3.5 target for `message.timestamp.before.max.ms`, or a Redpanda target)
was left out, as if it agreed. It is now listed as differing, the rule phase 7 already follows.
Only values change, not the format. The case is narrow: phase 0 refuses a run
whose mapped topic exists, so it needs a topic created between phases 0 and 2,
and that run then fails at creation.
**Do:** nothing.
**Scope:** a phase-3 row with its control (the old default restored fails it); the FX-4 collision line is unchanged byte for byte.
**Rollback:** an older `logweir` leaves an unreported setting out again;
documents this build wrote stay valid.

#### 81. SCRAM-SHA-512 over TLS, RustFS, versitygw and a directory archive have repeatable rows, and a weekly job re-runs them (PROD-01.2a, PROD-00.3o)

**Changed.** Only how the repository is tested. SCRAM-SHA-512 over TLS (Amazon
MSK's SCRAM mode), RustFS 1.0.0, versitygw v1.8.0 and a local directory
archive (`backend: filesystem`) move from untested to supported in
[the support matrix](support-matrix.md#archive-backends), each with a
repeatable row: the `auth` profile's `SCRAM512TLS` listener runs on every pull
request, and RustFS and versitygw are opt-in compose profiles. A weekly
workflow, `compat-weekly.yml`, re-runs the rows that need a profile
(Confluent Platform, Redpanda, the S3 servers beside MinIO, the directory
archive and Karapace), and its `operator-archives` job drills an archive
written by the engine every published version pinned (OSO's 0.21.0) with the
shipped engine. MSK itself is still unrun.
**Do:** nothing. No binary, image, chart value or signed document changes.
**Scope:** the new rows and their controls on compose slots 1 and 2 (a wrong mechanism refused on the SCRAM-SHA-512 listener, a directory archive missing its manifest, a stopped versitygw), and the archive drill with its corrupted-segment control.
[UNVERIFIED — compat-weekly.yml has not run on GitHub yet; its first scheduled run is the proof.]
**Rollback:** revert the change; nothing deployed is affected.

#### 82. The engine is build `0.23.3+logweir.3`: the restore checkpoint honours its interval (PROD-00.3g)

**Changed.** Patch `0003-restore-checkpoint-interval-and-hash.patch`: the
engine saves its restore checkpoint inside a topic every
`checkpoint_interval_secs`, not only after each topic, and its hash leaves out
the run's checkpoint and offset-report paths and map order. Logweir never reads
the checkpoint, so nothing Logweir does changes. The engine is `kafka-backup
0.23.3+logweir.3` (build-input digest
`sha256:e602a0a785e34e3f0e3ec7f01fef2f7146ec27f8aa69d6c9aecaf99f7ad4cef4`), and
`logweir doctor` accepts only that version. **Caveat:** a resumed restore still
has no offset mapping for the segments it skips; since this patch that can be
part of a partition, and the engine's phase 3 then extrapolates those offsets,
so offsets must not be applied after a resume. Logweir never resumes, and
renders `reset_consumer_offsets: false` and `auto_consumer_groups: false`.
**Do:** roll the controller and the runner images together, as for item 35. A
standalone CLI install replaces its engine with build 3 and exports
`LOGWEIR_ENGINE_VERSION=0.23.3+logweir.3` and
`LOGWEIR_ENGINE_DIGEST=sha256:e602a0a785e34e3f0e3ec7f01fef2f7146ec27f8aa69d6c9aecaf99f7ad4cef4`
([quickstart.md](quickstart.md), step 4).
**Scope:** the patch's unit tests (they fail on the unpatched source), two live resume rows on compose (build 2 fails both), `full_drill` and `record_semantics` with build 3, and CI's engine-oracle check.
[UNVERIFIED — no PoC drill has signed engine build 3 yet; the next image refresh runs one.]
**Rollback:** roll both images back together; the engine is build 2 again,
and its documents say so. Documents build 3 signed stay valid.

#### 83. A render that cannot ask the cluster is refused, and `identity.renderedOffline` makes Argo CD and `kubectl apply` safe (PROD-14.1)

**Changed.** The identity's create-once objects (the empty signing Secret, its
public ConfigMap, the console key's pair, each runner namespace's copy) and the
fresh install's trust grant are rendered only when Helm's `lookup` finds them
absent. `helm template`, `helm lint`, `--dry-run=client` and Argo CD answer
nothing, so they rendered all of them every time: Argo CD runs them as PreSync
hooks and deletes and re-creates each one on every sync, replacing the filled
signing Secret with an empty one (the next hook then generated a new key), and
`kubectl apply` re-created the cluster-wide trust grant, which nothing deleted.
Such a render is now refused by name unless `identity.renderedOffline` is set:
`true` renders no retained object and no trust grant (create the empty objects
once; the install starts strict), `false` renders what `helm install` would,
for a preview only. `helm install`, `helm upgrade` and `--dry-run=server` are
unchanged.
**Do:** nothing on the Helm path. Argo CD and `helm template | kubectl apply`:
back the identity up, create the retained objects if they are missing, and set
`identity.renderedOffline: true` ([install.md, *Argo CD and other offline
renders*](install.md#argo-cd-and-other-offline-renders)). If an earlier render
was applied with `kubectl apply --prune`, first remove
`kubectl.kubernetes.io/last-applied-configuration` from the retained objects
(`kubectl annotate <object> kubectl.kubernetes.io/last-applied-configuration-`),
or the next prune deletes them. A preview or a lint adds
`--set identity.renderedOffline=false`.
**Scope:** `scripts/check-chart.sh` arm 12 (the refusal by name with its
control, no placeholder, grant or trust argument with `true` and all of them in
the `false` control, adoption offline, no key material in any render) with five
mutants, and FX-10's row. The Argo CD behaviour is read from gitops-engine's
source (`pkg/sync/hook/delete_policy.go`: no policy means `BeforeHookCreation`),
not run. [UNVERIFIED — no Argo CD install has synced this chart; the clean
install proves only the Helm path (no refusal with `lookup`).]
**Rollback:** an older chart renders the placeholders again on every offline
render; remove `identity.renderedOffline` from the values first (an older
schema refuses it).

#### 84. The identity hook runs natively on arm64, and the install constraints are on the first page (PROD-14.1)

**Changed.** `identity.bootstrapImage` is re-pinned from runner rev `2fe8d907`
(linux/amd64 only, from before PROD-00.2) to the runner main CI published and
signed for rev `d4e32956`:
`docker.io/vladyslavhaina/logweir@sha256:8c5a76556236b3ff22d8618c1e2f141816eb60f6b1a476519b3b618efde348f6`,
an index of linux/amd64 and linux/arm64, so the hook no longer needs amd64
emulation on an arm64 node. Its `identity bootstrap --help` is byte for byte
the old pin's, so the chart passes the same flags. `helm show chart`, the
chart README's first screen and install.md's first section now state the
Kubernetes floor (1.29, which Helm enforces from `kubeVersion`), the Helm floor
(3.19 or 4.x), both node architectures and the supported brokers.
**Do:** nothing; the next `helm upgrade` runs the hook from the new pin and
validates the existing identity. Remove a `kubernetes.nodeSelector` you set
only to keep the hook on amd64 nodes.
**Scope:** the help of both platforms of the new pin captured with `docker run`
(2026-10-11; `uname -m` x86_64 and aarch64), the refreshed fixture with
`chart_lint_the_hook_passes_only_flags_its_pinned_image_runs`,
`chart_lint_constraints_show_before_install` with a
moved-floor control, and `scripts/check-chart.sh` arm 13 (Helm refuses 1.28.15;
1.29.0 renders), and `gh attestation verify` of the index: SLSA provenance from
`images.yml` on `main` at `d4e32956`, run 38173609124.
[UNVERIFIED — the cosign signature itself was not checked on this host (no
cosign); the clean install runs `cosign verify` before it installs.]
**Rollback:** set `identity.bootstrapImage` back to
`docker.io/vladyslavhaina/logweir@sha256:ea106328eb5f9364a4682e640ba258242c6fc949b5d2198140c57bf719a13968`;
both images run the same identity steps.

#### 85. The demo archive can outlive `helm uninstall`, and each restore target is listed once (PROD-14.1)

**Changed.** `minio.persistence.keep: true` renders `helm.sh/resource-policy:
keep` on the demo MinIO's PVC, so `helm uninstall` leaves it and the archive on
it; a `keep` annotation set by hand was ignored, because Helm reads the manifest
it stored ([install.md, *Keep the demo archive across an
uninstall*](install.md#keep-the-demo-archive-across-an-uninstall)). The default,
`false`, renders nothing new. The identity hook is given each id of
`identity.installationTrust.allowedTargetClusterIds` once, so a values list
that names the demo target `demoKafka` also adds no longer lists it twice in
`logweir-installation`.
**Do:** nothing. To keep a demo archive through an uninstall, set
`minio.persistence.keep=true` with `helm upgrade` first, and mirror it too.
**Scope:** `scripts/check-chart.sh` arms 14 (the demo target once, beside another
id; without the de-duplication it is listed twice) and 15 (keep adds exactly the
annotation on the PVC; a template that ignores the value, or always keeps, fails
it), and the value's FX-10 row.
[UNVERIFIED — no uninstall and reinstall has kept a PVC through this value yet; the next PoC reinstall runs one.]
**Rollback:** remove `minio.persistence.keep` from the values first (an older
schema refuses it); a PVC already kept stays until you delete it.

#### 86. The product API's OpenAPI document is `1.0.0-alpha.3`; six changes since `1.0.0-alpha.2` are breaking (release preparation)

**Changed.** `info.version` moves from `1.0.0-alpha.2`, the document
`v0.2.0-rc.1` shipped, to `1.0.0-alpha.3`. The document is still pre-release,
so its breaking changes move only the pre-release number, not the major
([stability.md](stability.md)). It now has 57 operations (2 added, 3 removed)
and 279 component schemas (37 added, 16 removed). The changes, one per line,
with paths under `/api/v1/namespaces/{ns}`:
- **Breaking:** `GET .../rehearsal-schedules` and `GET .../rehearsal-schedules/{name}`
  are removed, with their ten schemas and the required `Capabilities.rehearsals` (item 73).
- **Breaking:** `POST .../destinations:from-legacy` is removed (`404 not_found`), with
  `DestinationFromLegacyRequest` and the problem codes `legacy_location_unknown` and
  `legacy_location_mismatch` (item 75).
- **Breaking:** `ScheduleStatusView.retentionReport` is removed, with `RetentionReportView`
  and `RemovableSetView` (item 78).
- **Breaking:** `ProtectionPolicyView.notifications`, `AlertView.delivery` and
  `AlertView.notifiedTransition` are removed, with `NotificationsView`,
  `NotificationRouteView` and `AlertDeliveryView` (item 74).
- **Breaking:** `POST .../connections` refuses `auth.credentialRef`
  (`422 existing_credential_refused`); the credential is typed once in `auth.credential` (item 33).
- **Breaking:** `POST .../destinations` refuses a grant's `secret.existing`
  (`existing_credential_refused`), and `:update-access` accepts only a Secret the
  destination already names; the schemas do not say so (item 38).
- Added: `GET .../restores/{name}/approval-request`, `POST .../restores/{name}/console-approval`
  and `ApprovalPolicyView.consoleApprovalAvailable` (item 66).
- Added: `ApprovalPolicyView.operatorMode` and `basis`, and `RestoreRoutingView.operatorMode` (item 29).
- Added: `ConnectionAuthMode` `scramSha256`, `plain` and `mtls`, `ConnectionAuthRequest.credential`
  and `tlsCa`, `ConnectionAuthView.clientCertificateRef` and `tlsCa` (item 33).
- Added: `PointView.topics`, `topicsOmitted` and `ownerDetection` (item 32).
- Added: `PointTopicView.schemaDependency` (item 46).
- Added: `PointView.consumerPositions` (item 50).
- Added: `CreateRestoreRequest.coverage` and `completeMaxRecords`, `Restore.coverage`, and
  `VerificationScopeView.coverage`, `complete` and `unsampledTopics` (item 43).
- Added: `Restore.selection` and `VerificationScopeView.selection` (item 51).
- Added: `Approval.approvalSubject`, `Restore.approvalSubject` and `targetTopicsAppeared`,
  `RestoreTargetView.originalName`, `TopicNamingRequest.originalName` and
  `CreateRestoreRequest.originalNameConfirmation` (item 56).
- Added: `RetentionEvaluationView.accounting`, `keptCount`, `truncatedByCap`,
  `maxDeletionsPerRun` and `viewIncomplete`, and `EnforcementRunView.failedTruncated` (item 58).
- Description only: `ApprovalPolicyView.name`, `legacy` and `ordinaryConfirmationAvailable` (item 29).
- Description only: `POST .../connections`, `ConnectionAuthMode`, `ConnectionAuthRequest`,
  `ConnectionAuthView` and `NewCredentialRequest` (item 33).
- Description only: `VerificationScopeLevel` (item 43).
- Description only: the problem code `malformed_request` names the body deadlines (item 44).
- Description only: `RetentionEvaluationView.at` (item 45), and its `candidateCount`,
  `candidates`, `kept`, `pointsEvaluated` and `truncated` (item 58).
- Description only: `Restore.newTopics`, `RestoreTargetView.topicPrefix` and
  `TopicNamingRequest.prefix` (item 56).
- Description only: `CompletionView.recordsRestored` (item 57).
- Description only: `TeardownView` and its `failed` (item 69).
- Description only: `POST .../catalogs` and `ConnectArchiveRequest` (item 73).
- Description only: `OperatorModeView` names `ApprovalRoute` (item 77).
- Description only: `RetentionRequest`, `RetentionView` and `retention` on `Schedule`,
  `CreateScheduleRequest` and `UpdateSchedulePolicyRequest` (item 78).
**Do:** nothing for the console, which ships in the same image as the API. A
script that calls a removed route or reads a removed member stops doing so;
no client outside this repository is supported.
**Scope:** `just schema-check` and
`crates/logweir-api/tests/contract.rs::the_checked_in_openapi_document_is_what_the_types_generate`
hold the checked-in document to the generator; the list is a diff of the
operations, schemas, members, required members, enumerations and problem codes
of `v0.2.0-rc.1`'s document against this one.
**Rollback:** an older console image serves `1.0.0-alpha.2` again; roll it
back with the items it names.

#### 87. A refused retention run stays named on its RetentionPolicy until the next run (D-R1)

**Changed.** Item 79 named a refused run (exit 3) on the harvest only: the
next pass, seconds later, read `NothingToDo` ("this plan has already run in
the slot"), and after three runs `EnforcementDegraded` said the last run
"named no per-point code". The refusal is now recorded in
`status.lastEnforcement.refusalCode` (and `refusalVariable`, a variable's name,
never its value), and `Enforced` and `EnforcementDegraded` keep naming it until
the next run starts. A pass with something else to fix first (the mode, the
schedule, the catalog view, the ceiling or the approval) shows that on
`Enforced`; the record and `EnforcementDegraded` still name the refusal. A
refused run counts toward the three failed runs that stop scheduling, as
before. Nothing about what is deleted, planned or scheduled changes.
**Do:** nothing beyond applying the CRDs before the controller, as every
upgrade does ([install.md](install.md#upgrade-crds-before-upgrading-the-controller)).
**Scope:** controller rows over the recording API double for six refusal
classes across the reconcile after the harvest, and for three refused runs
and the pass after them; each fails on the previous controller; eight
mutants.
[UNVERIFIED — no live install has run this build's refusal yet.]
**Rollback:** roll the controller back; it neither writes nor reads the two
fields, and the refusal is named only on the harvest pass again. The newer
CRD is harmless to it. After a rollback and a roll-forward, a refused run the
older controller harvested can be read with the previous run's code until the
next run starts, in the messages and the console's fields only.

#### 88. The console's check commands, Rotate access, Retention panel, "What uses this" and Restore detail say what is so (PROD-14.1)

**Changed.** Five defects the PROD-14.1 visual pass found on the console:
- A Backup's *Check it yourself* fetches from the bucket of the destination the
  run names, with `--endpoint-url` and `--region` when the destination has them.
  It printed the destination's name as the bucket (`s3://primary/…`). A
  destination that now names another location than the run froze, or that this
  login may not read, gets a placeholder bucket and the reason; a run with no
  evidence keys yet says so instead of fetching the bucket root. A Restore's
  fetches carry the endpoint and region of its plan's evidence store too, a
  path-style store at an endpoint gets a note naming
  `aws configure set s3.addressing_style path`, a run missing either key prints
  no command, and the Python line passes its three files as the positionals
  `verify_scorecard.py` reads (it printed the Rust flags, which the script
  refuses).
- *Rotate access* starts every grant on `keep`, which sends the grant as the
  destination has it. It started the three optional grants on `absent`, so
  rotating one key removed the others and turned evidence verification off.
  Choosing `absent` for a grant the destination holds says what turns off. A
  Secret whose data keys are not `access-key-id` and `secret-access-key` cannot
  be kept from the console, and the form says so and sends nothing.
- A schedule's *Retention* panel shows the `RetentionPolicy` whose
  `spec.destinationRef` names the schedule's destination: a `Report` policy
  deletes nothing, an `Enforce` one is shown as deleting, and two such policies
  are the `Conflict` the controller marks them. A policy that is not
  recommendation-only and names another destination is listed under it, since
  two destinations can share a bucket and prefix. "Logweir never deletes from
  your archive" appears only when the policies were read and none names the
  destination.
- `GET …/destinations/{name}/usage` lists the schedules and Backups whose
  `spec.destinationRef` names the destination, from at most 1,600 of each kind,
  and says `truncated` past either bound. It selected on a label no schedule or
  Backup carried, so the console said nothing used a destination that a schedule
  and its Backups used.
- The `Restore` view gains `integrityResult`, `integrityPartialReason`,
  `topicPreflight` (`TopicPreflightView`) and `oldTopics` (OpenAPI
  `1.0.0-alpha.3`, additive), and a Restore's detail shows them. The objectives
  and the measured values, which no route publishes, are one sentence in place
  of six "not published" rows.
**Do:** nothing. `:update-access` still takes the complete grant block; a script
that sends only the grant it changes still removes the others.
**Scope:** `ui/tests/visual-pass-a.spec.js` (`h1_*`, `h2_*`, `h3_*`, `m9_*`),
`crates/logweir-api/tests/destinations.rs` (usage) and
`crates/logweir-api/tests/list_projection_additions.rs` (the Restore members),
each with a control that fails on the old code.
[UNVERIFIED — no PoC page has shown these fixes yet; the PoC re-check after merge walks each page again.]
**Rollback:** an older console image serves the old pages and the old usage
route; nothing is stored, and an older console ignores the four new members.

#### 89. The console's fixes from PROD-14.1's visual pass: where Create goes, who sees which form, identifiers on one line (PROD-14.1)

**Changed.** The console only (`ui/`); no API document, controller, chart value
or signed format changes. What a person sees:
- *Create the Restore* opens the run's operation view (`#/operations`), as
  step 6 says, and a Restore waiting for its approval reads "Waiting for
  approval" instead of "Pending: no Job exists, and none will ...".
- A role that cannot create a connection or a schedule (a viewer, an approver)
  is not offered those create forms, and an approver lands on Approvals instead
  of a refused Clusters page.
- In a table a long identifier (a key id, a point id, a backup set, a signer)
  is one line, cut between 8 and 14 characters wide, whole on hover, and copied
  by a click. In the Approvals table an approver reads as their identity
  provider's host and a short form of their subject, its cut marked by an
  ellipsis, or "you". Where an approval is decided or audited (the Approval
  card's approver, the two-person panel's "requested by" and "approved by"),
  the full principal is shown, wrapped, so one subject at two identity
  providers, or two subjects sharing a head and tail, never read alike. The
  restore picker's archive points list the point and its set under
  the catalog, so their STATE column is in sight at 1024 px.
- With no topic ticked, step 5's Check readiness is disabled and says why;
  under a confirm-mode namespace step 5 says "you confirm".
- The operation view labels a restore's scorecard key as the scorecard, and
  its state chip has the phase chip's colour.
- At 390 px an instant stays inside its card, and COVERED keeps "from" and
  "to" together.
- The Protection list's "OPEN ALERTS" column is OPEN PROBLEMS: delivery is
  withdrawn (item 74), and the ledger records problems.
**Do:** nothing. A script that waited for `#/history` after a confirm-mode
Create waits for `#/operations?...&kind=restore` instead (the repository's two
journeys that did are updated).
**Scope:** `ui/tests/console-visual-pass.spec.js`, one row per defect with its
negative control, in the UI behaviour suite. The layouts were measured in
Chromium at 1440, 1024 and 390 px over the page modules and the stylesheet,
before and after. [UNVERIFIED — not yet seen on the PoC; the re-check after
merge walks each page as each role.]
**Rollback:** an older console image renders the old pages; nothing is stored.

#### 90. A transient connection error no longer fails a run's evidence write (FX-51)

**Changed.** Every create-only evidence write (a receipt and its signature, the
consumer positions document, a scorecard, a catalog record) is re-sent up to
three times, 200 ms, 400 ms and 800 ms apart, when its connection dropped in
under 2 seconds with no answer, such as a pooled connection the store had
already closed. Before, one such drop failed a backup with exit 4 and
`signing: storage: Generic S3 error: Error performing PUT ... - HTTP error:
error sending request`, though the archive was written. A timeout, a refused
connection (which the store client already retries) and any answered status
(a 4xx) are not re-sent, so a put that fails still fails within about 7
seconds of the time it took before. Create-only still holds: a re-sent write
that finds the key taken reads the object back and succeeds only when it holds
exactly the bytes this run wrote (the lost attempt landed them), where the
writer may read the key; a runner holding only `s3:PutObject` on `logweir/*`
still fails such a run, as before. Any other object is refused as before and
never overwritten. Not in `v0.2.0-rc.2`.
**Do:** nothing.
**Scope:** `logweir-store`'s `evidence_put_retry_tests`, over the real S3
client and a scripted transport: a fast drop is re-sent and lands; a lost
answer over the same bytes succeeds; over different bytes, longer or of the
same length, it is refused; a denied read-back is an error; a 403, a timeout
and a slow drop are each sent once; a first `AlreadyExists` is refused without
a read. Live on the compose stack: the e2e row that failed this way in CI
(`position_evidence`) and `full_drill`'s main row.
**Rollback:** an older runner image fails the run on such a drop, as before;
nothing is stored differently.

#### 91. A scratch restore can be kept for a declared time, then deleted by what its scorecard signed (PROD-11.1a)

**Changed.** A scratch restore can now be kept for a declared time:
`target.teardown: {ttl_seconds: N}` (five minutes to thirty days;
`CloneTtlOutOfRange`, and `CloneTtlNeedsScratch` outside scratch mode, both
exit 3 before anything is read). A passing clone keeps its topics; the
controller then runs one cleanup Job (`logweir restore cleanup`) at the expiry,
or at once when the `Restore` is deleted, behind the finalizer
`logweir.dev/clone-cleanup`. The cleanup deletes only the topics the run's
signed scorecard records, only on the cluster it records
(`CloneTargetClusterChanged`) and only while each topic's ID is the one
recorded at creation (`CloneTargetReplaced` otherwise), signs what it did
(`logweir/drills/<run>.cleanup.json`) and reports it on `status.clone` and
`status.teardown`. Anything it cannot settle is `RemovalFailed` with the
reason, and the finalizer stays until an operator removes it. Every run that
creates topics now signs them, with their topic IDs and
`lineage_headers: kept`, in the scorecard's new `target.creation` block
(format 1.10.0, 2.2.0 for a subset; `verify_scorecard.py` 1.31.0). The restore
preflight names a selected partition with nothing in the window
(`emptyInWindow`).
**Do:** apply the CRDs (`Restore.status.clone`) and keep the runner image as
new as the controller: a runner from before this release refuses a clone plan
(`drill spec does not parse`, exit 1) and creates nothing.
**Scope:** compose rows C40 to C46 on Kafka 4.3.1: a clone kept, then cleaned
up to exactly its recorded topics while a stranger under the prefix survives;
a recreated topic left as `CloneTargetReplaced`; a repointed cluster refused
with nothing deleted; both readers accepting the block; an older runner
refusing the plan; a failed clone torn down at once; the preview. Unit and
mock-API rows cover the controller's finalizer paths, each fail-closed path
with its control, and mutants on the deletion rule and the finalizer were all
killed.
**Scope (PoC):** rows P7 and P8 of `poc-v2a-r2` (build `sha-cc59922e`,
2026-10-14, the PoC's least-privilege MinIO grants): a clone kept for 300 s
goes `Removing` at expiry and then `Removed`, its `teardown.deleted` equal to
the scorecard's `target.creation`, with the attestation written, while a
stranger topic under the prefix survives; a clone deleted before expiry is
gone in 7 s with its topics. The first pass (`poc-v2a`, `sha-4db6e5bb`) found
D1 here (item 98).
**Rollback:** an older runner refuses a clone plan, and an older controller
neither sets nor reads `status.clone`. After a rollback, delete a remaining
clone's topics by hand and remove its `logweir.dev/clone-cleanup` finalizer.
Scorecards that carry `target.creation` stay valid: an older verifier ignores
the block.

#### 92. A restore can rebuild its topics' configuration from the recovery point (PROD-05.2)

**Changed.** A restore plan bound to a recovery point may carry
`target.topic_configuration: {transition: afterVerification | none, owners?}`.
Each topic is then created from the point's configuration model: the source's
partition count, a replication factor bounded by the target's brokers (and
why), and the source's portable overrides (`min.insync.replicas` capped at the
factor), beside Logweir's own settings that keep every restored record until
the verdict. After a passing verdict, phase 9 applies the source's compaction,
retention and timestamp settings with one full-set `AlterConfigs` per topic,
only to a topic still holding what it was created with, and proves each by
reading it back. Inherited values, reassignment throttles, provider keys and
secrets are never applied and are listed with the source's value; a
removed-in-Kafka-4 key reaches only a 3.x target. A topic a declarative owner
manages is created with Logweir's settings only and never altered: its
desired state is put beside the scorecard as `KafkaTopic` YAML, bound by
digest. The restore preflight previews the per-topic diff, both
`validate_only` verdicts and what the new retention deletes; phase 0 refuses,
before creating anything, `TopicConfigurationNotRecorded`,
`TopicOwnerNotChecked`, `TopicClassDisagrees` and `TopicConfigRefusedByTarget`
(naming the setting). `Restore.status.topicConfiguration` and the API show the
factor, the before and after overrides and the outcome (`applied`, `partial`
with each topic's remaining step, `notRun`); the phase-9 attestation signs it.
Logweir never retries a failed transition. The scorecard is 1.11.0 (2.3.0 for a
partition subset) with the optional `target.topic_configuration`, arms TC-1 to
TC-5; `verify_scorecard.py` 1.32.0 checks them. Schemas `-1.11.0.json` and
`-2.3.0.json`; `-1.10.0.json` and `-2.2.0.json` are frozen.
**Do:** apply the CRDs first (`Restore.status.topicConfiguration`). The restore
principal needs `DescribeConfigs` and `AlterConfigs` on its restored topics for
the transition.
**Scope:** compose rows C20 to C27 on Kafka 3.9.2 and 4.3.1: a 3.9 point
restored to 3.9 and to 4.3 with its model and the removed key; the factor
bounded by fewer target brokers and `min.insync.replicas` capped; compaction
applied after a complete verification; 30-day-old records restored under a
bounded broker; settings never applied listed; an owned topic exported and its
neighbour transitioned; a partial transition naming what was not applied, read
back. Unit rows and 13 mutants on the stage table, the compare, the factor, the
cap, the class rule, the pass gate and the read-back, all killed.
**Scope (PoC):** row P6 of `poc-v2a` (build `sha-4db6e5bb`, 2026-10-14): a
compacted three-partition topic restored with `transition: afterVerification`
reports RF `{1, 1, sourceFactor}`, `before` holding Logweir's during-restore
set and `after` the source's settings, which the broker then reads exactly;
both readers accept the 1.11.0 scorecard.
**Rollback:** an older runner ignores `target.topic_configuration`, creates its
topics as before and signs no block, so nothing is claimed; an older
controller ignores `status.topicConfiguration`. Scorecards that carry the
block stay valid: an older verifier accepts the document and ignores the
block.

#### 93. Backups record Strimzi owners from the controller (PROD-05.1a)

**Changed.** A `KafkaCluster` may name its Strimzi cluster
(`spec.strimzi: {namespace, cluster}`). With the chart values
`strimzi.readKafkaTopics: true` and `strimzi.namespaces` listing that
namespace, a `Backup` lists its `KafkaTopic`s (read only, a Role per listed
namespace, never a ClusterRole rule), freezes them with the run, and the
receipt records each topic's owner, so a restore exports desired state for it
(item 92). A namespace the chart does not list, a refused list or no served
`KafkaTopic` resource fails the backup `OwnerDetectionFailed` before any Job.
**Do:** apply the CRDs first (`KafkaCluster.spec.strimzi`); set the two chart
values only where a Strimzi cluster's topics should be read.
**Scope:** mock-API rows (C28): listed `KafkaTopic`s reach the run's inputs, its
plan and its argv, and the core's owner rule reads them back to one owner; a
403, an unserved resource and an unlisted namespace are each
`OwnerDetectionFailed`, the last before any read; no `spec.strimzi`, nothing
listed.
**Scope (PoC):** row P10 of `poc-v2a` (build `sha-4db6e5bb`, 2026-10-14), with
only Strimzi 0.45.0's `KafkaTopic` CRD installed: the controller's Role and
RoleBinding in the listed namespace only; the receipt records owner `strimzi`
for the managed topic and none for its neighbour; the restore exports the
owned topic's YAML (its digest the scorecard's) and applies the neighbour.
Controls: a `spec.strimzi.namespace` outside the list, and the values off, are
each `OwnerDetectionFailed` with no Job.
**Rollback:** a controller older than this field refuses such a frozen plan
(`PlanConfigMapConflict`); let those runs finish first. An older controller
prunes `spec.strimzi`, and a restore with the plan block and no owner
statement is then refused `TopicOwnerNotChecked`.

#### 94. A restore no longer loses a record held out of timestamp order (PROD-01.1b)

**Changed.** The archive describes each segment by its first and last record
timestamps and the engine selects segments by them. Before it creates
anything, a restore now reads every segment of the partitions it restores once
(the pass the transaction-marker check made, now also under
`restore.allow_transaction_markers: true`) and uses each record's own
timestamp: the window starts at the oldest archived record, so a record older
than every segment's first is restored; the count bounds use each segment's
oldest and newest record, so a correct point-in-time restore no longer fails
them; and a window that selects a record the engine would skip (in a segment
whose first record is after the point, or whose last is before a stated start)
is refused, exit 3, `refusal-reason=PointInTimeSelectionIncomplete`, naming
the segment and the point or start that reads it. The time basis (FX-8) reads
the same bounds. Archives in timestamp order restore as before. A segment the
restore reads and cannot decode is now refused under the marker override too.
**Do:** a `Restore` or `RehearsalSchedule` slot over such an archive that used
to run, and could sign a pass with a record left out, is now refused; use the
point the refusal names. Roll the controller with the runner.
**Scope:** the three record-semantics rows and the stated-start row on compose
(Kafka 4.3.1, engine `0.23.3+logweir.3`), orchestrator rows, 11 mutants.
**Scope (PoC):** rows P11 to P13 of `poc-v2a` (build `sha-4db6e5bb`,
2026-10-14): an out-of-order point is refused `PointInTimeSelectionIncomplete`
naming the segment and the point that reads it, with no topic and no
scorecard, and the control point restores every record, the older one
included (P11); the floor restores the record older than its segment's first
(P12); a point taken before the upgrade, in order, restores as before (P13,
standing in for the withdrawn `RehearsalSchedule` row).
**Rollback:** an older runner selects by first and last record again; an
older controller shows `exitReason: PointInTimeSelectionIncomplete` without
the runner's sentence.

#### 95. Consumer positions follow a restore, under review, and a `Switchover` applies them (PROD-04.2)

**Changed.** A complete restore of a recovery point that captured consumer
groups now maps every captured position onto the restored topics (`exact`,
`approximate` with the count of records the restore's own selection skipped,
or `unavailable` with its reason) and binds the mapping into the signed
scorecard (format 1.12.0, 2.4.0 for a subset,
`integrity.verification.positions_mapping`; both readers check it,
`--positions-mapping` verifies the document; `verify_scorecard.py` 1.33.0).
Nothing is committed by a restore. A new `Switchover` kind applies a reviewed
plan (`logweir switchover plan`) to stopped groups under its own approval
(subject kind `Switchover`): each group is classified, held to the target's
bounds and compare-and-set against the plan, and committed by Logweir's own
non-member client, which the broker refuses while the group has a member. The
target topics must be the ones the restore created: their IDs are compared
with the scorecard's `target.creation` (item 91), so a recreated topic is
`TargetGenerationChanged`. A signed switchover record
(`logweir/switchovers/<run_id>.record.json`) records priors, commits and
readbacks, and `logweir switchover rollback-plan` reverses it within stated
limits. A `Preflight` with operation `Switchover` (check kind
`switchoverReadiness`) reviews the same decisions before anything is
committed.
**Do:** apply the CRDs (the new `Switchover`, and the `Approval` and
`Preflight` additions) and roll the controller and the runner image together.
Upgrade every verifier that will read a 1.12.0 or 2.4.0 scorecard's mapping,
and stop each group's consumers before its switchover.
**Scope:** compose rows C1 to C17 on Kafka 4.3.1 (C14 and C2 also on 3.9.2):
the paused consumer resumes at its record; a live group is refused
`GroupActive` and a new run recovers; partial application and re-runs; the
compare-and-set; approximate and not-restored mappings; a compaction hole; an
emptied partition; a recreated target topic; only a passing complete restore
maps; a streams group refused by type; a hidden group never created; a
transactional archive; the rollback and its refusal; the subject kind. The
review row reads every position before the switchover. 14 mutants on the
mapping rule, the compare, the admission and the apply were killed; the API's
approval gates are held to the restore's by a parity table.
**Scope (PoC):** rows P1 to P5 and P9 of `poc-v2a` (build `sha-4db6e5bb`,
2026-10-14) and, after item 98, P1, P2 and P5 of `poc-v2a-r2` (`sha-cc59922e`):
a backup with three captured groups, a passing complete restore, and a
Switchover that applies `k1-app`, whose consumer's first record carries
`x-original-offset=40`; a live member refused `GroupActive` and a new run
then applied; a recreated target topic refused `TargetGenerationChanged` with
nothing committed; the two-person request and approval, and a self-approval
refused (P3); the subject kind (P4); the rollback hazard of item 98's D5 (P9).
**Rollback:** an older controller ignores `Switchover` objects, so nothing is
applied, and a Helm rollback leaves the CRD and its objects inert. **But an
`Approval` naming a `Switchover`, or a `Preflight` with operation `Switchover`,
fails an older build's whole list of that kind** (`unknown variant`): every
new `Restore` then stays `ApprovalNotVerified`, or no `Preflight` is
reconciled. Record and delete both before rolling back (the next entry's
rollback step; [the commands](kubernetes.md#rolling-back)); pending switchovers
then need a new approval after rolling forward. An older verifier ignores the
mapping block and refuses the `switchover-record` payload type. Positions
already committed stay committed: a switchover record's `rollback-plan`
reverses them.

#### 96. Every runner line a controller acts on is sealed with the Job's token, and a runner older than the controller is named (FX-43)

**Changed.** The controller gives every runner Job `--runner-line-token <hex>`
and reads only the lines the runner prints sealed with it
(`@<token> <line>`). That covers `Restore`, `Backup`, the `KafkaCluster`
probe, the check Jobs of `Preflight`, evidence fetch, `TopicDiscovery`, a
dynamic `Backup`'s discovery and the `RecoveryCatalog` sync, the
`RetentionPolicy` Job, the `Switchover` Job (item 95) and a clone's cleanup
Job (item 91). A check seals its end frame, which binds every other frame by
digest. The lines read this way are:
- the evidence keys;
- the `failure-reason=`/`refusal-reason=` states and the stopped creation
  step's lists;
- `topic-preflight=` and `topic-configuration=` (item 92);
- the teardown lines and the progress lines;
- the switchover's and the cleanup's lines;
- the probe's two lines.

A line a plan string, a broker or the Kafka client got into the pod log is
no longer read. The Kafka client's own log lines are printed by the runner
on one line each. A bootstrap entry or a storage location holding a control
character or a line break is refused before any client exists, by the
runner, by `cluster-probe` and by a pattern on
`KafkaCluster.spec.bootstrapServers`. The terminal log read is one bound at
every exit code: 256 lines, 4 MiB.

**Do:** roll `controllerImage` and `runnerImage` together. A runner image
published before this change (v0.2.0 included) stops at the new flag, and the
object says so:
- a `Restore` or `Backup`: `exitReason: RunnerOlderThanController`, and a
  schedule does not retry it;
- a `KafkaCluster`: reason `RunnerOlderThanController`;
- a check: `CheckContractMismatch` naming it;
- a retention run: `refusalCode: RunnerOlderThanController`, with nothing
  deleted;
- a `Switchover` or a clone's cleanup: failed or `RemovalFailed` naming it.

Set `runnerImage` to the controller's build.

**Scope:** unit and controller rows over fake pod logs, including the
released runner's real refusal of the flag (Restore, Backup, probe, check,
retention), and the binary's sealed block. 32 mutants were killed. The
Switchover and cleanup Jobs' wiring is compile-checked and covered by their
existing rows only.
**Scope (PoC):** rows P14 to P23 of `poc-v2a` (build `sha-4db6e5bb`,
2026-10-14): a `Backup`, a `Restore`, a refused restore, the probe, the
client-log escape, a check's end frame and a retention run each act on their
sealed lines only, the plain copies last (P14 to P21; a bootstrap entry with a
control character refused by the CRD pattern, P17); a runner older than the
controller is named `RunnerOlderThanController` on two Backups (no retry), the
probe, a `Preflight` and a retention run (nothing deleted) (P22, P23).

**Rollback:** roll both together, or the controller first. An older
controller reads this runner as before: FX-34's `--line-token` gets FX-34's
output, and the plain lines are still printed last. Runs created by an older
controller are read as that controller read them.

#### 97. The control-evidence mapping page is removed (owner's decision)

**Changed.** `docs/control-evidence.md` was removed, together with the test
that held it to the evidence fields. That page mapped Logweir's evidence onto
the backup and restore-testing clauses of DORA, ISO/IEC 27001, SOC 2, NIS2 and
HIPAA. The scorecard's limits are stated in
[docs/verify-a-scorecard.md](verify-a-scorecard.md#what-the-scorecard-does-not-claim),
which no longer links to the mapping, and the README's documentation table no
longer lists it. No product behaviour, format or evidence changes.
**Do:** nothing. A bookmark to the old page finds nothing; read the auditor
guide instead.
**Scope:** `just links` and doc_lint over the tree without the page.
**Rollback:** an older release's documentation still carries the page.

#### 98. A `Switchover` and a clone's cleanup read the restore's evidence with the evidence destination's read grant; a store that refuses the read is never a verdict (theme A's PoC defects D1 and D4)

**Changed.** The `Switchover` Job (item 95) and a TTL clone's cleanup Job
(item 91) read the restore's signed evidence (the scorecard, its sidecar, the
positions mapping, a rollback's record) with the evidence destination's
`evidenceRead` grant, projected in the Job's archive slot and named on the
runner's new `--evidence-read-grant`, and write the switchover record or the
cleanup attestation with `evidenceWrite`. Before, both read with the write
grant: on a least-privilege writer (`PutObject` under `logweir/` only, as the
grant table advises) every switchover was refused `MappingDigestMismatch` and
every clone ended `RemovalFailed`. A read the store refuses is now an
operational failure by name, never a verdict on the evidence: the switchover is
`Failed`, reason `EvidenceUnreadable`, its message naming the key, the grant
and the store's class (`could not read logweir/drills/<run>.json with grant
evidenceRead of BackupDestination <ns>/<name>: AccessDenied`); a clone's
cleanup attempt carries the same sentence on its line and in
`status.clone.reason`; the `switchoverReadiness` review answers the store's own
code. `MappingDigestMismatch` keeps its meaning: the bytes are not the bound
ones. An `evidenceRead` no pod can hold (`ControllerIdentity`, absent) falls
back to `evidenceWrite` and says so; that write grant then needs `s3:GetObject`
on `logweir/*`. A `Switchover`'s `status.message` now ends with the runner's
own sentence for any whole-run refusal or failure, and a clone's
`RemovalFailed` reason names the last attempt's cause, pointing at
`status.teardown.failed` only when topics were left. That cause is cleaned
before it is printed and again before it is stored: access key ids, S3 error
bodies and URL queries never reach `status.clone.reason`. A cleanup attempt
rebuilt from an older template (the run Job, or a cleanup Job an older
controller built) runs this controller's runner image; one copied from this
build's own attempt keeps that attempt's image. The `Switchover` Job mounts the
restore's plan `ConfigMap` at `/plan`, so an evidence destination with a
private CA (`spec.transport.caBundle`) resolves its CA file there, as the
restore's own Job did.
**Do:** give the evidence destination an `evidenceRead` grant a pod can hold
(`SecretKeys` or `WorkloadIdentity`, `s3:GetObject` on `<bucket>/logweir/*`)
before relying on a switchover or a clone, and roll the controller and the
runner image together: the controller passes the new flag. After the upgrade a
cleanup Job an older controller built is never started. A suspended one is
replaced by the next attempt, built with this release's grants and runner
image, at the clone's expiry; a clone such a Job left `RemovalFailed` is
re-armed automatically: at least one more attempt, retried as any failed
attempt is up to the third (exactly one when the older build held it at
attempt 3), and it holds as before if the last one fails too. A
`Switchover` the older build refused `MappingDigestMismatch` is terminal:
create a new one with the same plan and a new approval.
**Scope:** unit rows on the grant table, its fallback and the renderer list,
the Switchover Job's environment and argv, a copied cleanup Job rewired, the
403 mapping in the switchover, the cleanup and the review, the status message
and the `RemovalFailed` reason; mock-API rows for the upgrade (an older
suspended cleanup Job replaced, a `RemovalFailed` clone re-armed once, each
with its control); compose rows on Kafka 4.3.1 with the PoC's least-privilege
MinIO policies (a switchover and a clone cleanup succeed with the read grant;
the write grant alone fails `EvidenceUnreadable` by name, nothing committed or
deleted); after the fix round's review, rows for a rewired retry's image (with
a copied attempt's as the control), the cleaned cause on both sides, and the
`Switchover` Job's plan mount with its CA paths. 17 mutants were killed.
**Scope (PoC):** `poc-v2a-r2` (build `sha-cc59922e`, 2026-10-14), on the PoC's
least-privilege MinIO grants: the Switchover (P1) reads with
`evidenceRead of BackupDestination logweir-poc/primary`, writes with the
writer's grant and holds no variable of the archive reader's; through a
destination whose `evidenceRead` is the writer's credential it ends `Failed`
`EvidenceUnreadable`, naming the key, the grant and `AccessDenied`, with
nothing committed (P1c); a clone the older build left `RemovalFailed` is
re-armed once by the upgrade, at attempt 4, and `Removed` (P7u); an older
build's suspended attempt-1 Job is never resumed and attempt 2 removes the
clone (P7u); a clone whose attempt 1 failed under the older build just before
the upgrade gets attempt 2 on this build's runner image with the flag, and is
`Removed` (P7u-c).
**Rollback:** an older controller builds both Jobs with the write grant again,
and an older runner refuses the new flag, so roll both together. Nothing
stored changes.

#### 99. A create refused for a taken approval name leaves nothing behind (theme A's PoC defect D3)

**Changed.** Under an approval policy, a `POST …/restores` or `…/switchovers`
whose approval name (or its `-confirmation`) already belongs to another
object's `Approval` is answered `409 state_conflict` before the `Restore` or
`Switchover` exists ("…was not made for this request… Nothing was created.").
Before, the subject was created and then refused, and it failed
`ApprovalSubjectMismatch`. A replay of an interrupted request is unchanged.
**Do:** nothing; submit again under another approval name.
**Scope:** API rows for both kinds and one under a Governed policy, whose
taken name is the `-confirmation`, each with its control under a free name,
and the replay rows; 5 mutants were killed.
**Scope (PoC):** row P3 of `poc-v2a-r2` (`sha-cc59922e`): a resubmission whose
approval name is taken answers `409 state_conflict` "…Nothing was created…"
and the Restore count is unchanged; a fresh plan answers 201 and runs.
**Rollback:** an older API creates the subject before refusing again.

#### 100. Both verifiers read a public key file the same way (L1)

**Changed.** `logweir drill verify --public-key` accepts whitespace after the
`-----END PUBLIC KEY-----` line, so an extra blank line no longer refuses the
key, as `verify_scorecard.py` already accepted it; `verify_scorecard.py` 1.34.0
refuses text after the block, as `drill verify` already did. Whitespace is the
same five characters to both (space, tab, line feed, form feed, carriage
return); a vertical tab is text, refused by both. RFC 7468's explanatory text
before the block stays ignored by both.
**Do:** nothing.
**Scope:** a key-file loop in the reader-parity gate
(`scripts/check-verifier-parity.sh`: plain, trailing blank lines, text before,
text after, a trailing form feed, a trailing vertical tab); 3 mutants were
killed.
**Rollback:** an older `drill verify` refuses the extra blank line again, and an
older `verify_scorecard.py` accepts text after the block.

#### 101. Under an older runner, a Preflight's pod rows say the pod ran (D6)

**Changed.** When a check runner is older than the controller
(`CheckContractMismatch`, `RunnerOlderThanController`), `runner.image` names
it, `runner.pod` says the pod ran and exited, and the projection rows say they
were in place, instead of "the check pod has not started".
**Do:** nothing; roll the runner image to the controller's (item 96).
**Scope:** one controller row with its control; 1 mutant was killed.
**Scope (PoC):** row P23 of `poc-v2a-r2` (`sha-cc59922e`), under an older
runner set through Helm values: `runner.image` reads `notReady`
`CheckContractMismatch` with the `RunnerOlderThanController` sentence and
`runner.pod` "the check pod ran and its runner exited 1".
**Rollback:** an older controller says "the check pod has not started" again.

#### 102. The console is rebuilt, and `logweir-api` serves it at `/ui/` (PROD-18.1)

**Changed.** The console is new: React, TypeScript and PatternFly, built from
`console/` into the `logweir-console` image. It keeps every flow the old page
had, under grouped navigation (Overview; Protect: Backups, Schedules,
Protection; Recover: Restores, Switchovers, Approvals; Evidence: Recovery
catalog, Trust; Settings: Connections, Destinations), with a five-step restore
wizard in place of six, a live restore page, theme A's switchover review, and
light and dark themes. The restore review no longer names schema-dependent
topics; the Recovery catalog page does. Addresses are `/ui/#/<namespace>/<page>`; an old
address such as `#/backups?ns=team-a` reaches no page (it reads as a namespace
named `backups`). The image serves
the build's page and its hashed files under `assets/` (`immutable`), and
nothing else: the old page's files, its served `runtime.js` and the
`/ui/next/` preview are gone. The API, the CSP, the session and the sign-in
round trip are unchanged. The release no longer attaches `ui-files.sha256`:
the console is the `logweir-console` image, by digest, and
`scripts/check-image-api.sh` holds its `/ui` to the tagged commit's build.
**Do:** nothing; update bookmarks to the new addresses.
**Scope:** the console's Vitest rows (every flow, its refusals and the plan
goldens, which `console_plan.rs` also parses into `RestoreSpec`), its
Playwright rows against `logweir-api` on the host, and the image check; the
journeys on a cluster are the PoC pass's.
**Scope (PoC):** `poc-v11` (build `sha-519ab67c`, 2026-10-14) rows 9 (the
served files, their cache headers and the CSP), 73 (the ported specs, through
a browser that accepts the PoC's local CA) and 74 (`/ui` is this tree's build,
14 files); the deep UI validation's parts 1 to 3 (`deep-ui-1`, `deep-ui-2`,
`deep-ui-3`, the same build) used every page under the seven validation roles,
light and dark, at 1440 and 390 px, by clicks only. What they found is fixed
in items 109 to 115.
**Rollback:** an older `logweir-console` image serves the old page again;
nothing is stored differently.

#### 103. A complete verification reads each archived segment under a stored and a decompressed cap, and never holds a record whole (FX-30)

**Changed.** A complete verification streams each archived segment, hashing
each record as it is read and never holding it whole. The pre-restore pass
already stops a run for a stored segment over 1 GiB (exit 1), and for a
segment the restore reads whose body decompresses past 1 GiB (exit 3). The
complete verification's own caps cover a segment outside the restore window
whose body decompresses past 1 GiB (a corrupt segment or a decompression
bomb), and an object that grew past the stored cap between the two reads.
Such a partition is not compared, and the signed block reads `covered: false`
with a reason that names both caps. It is never a crash or a `pass`, and no
consumer positions are mapped over it. Measured: about 1 to 3 MB of added peak
memory for a 1 GiB bomb, an oversized object and a 190 MiB segment, against
391 MB for the old whole decode. The console's restore page names the reason,
the unverified segments and the partitions not compared.
**Do:** nothing.
**Scope:** the streamed-digest rows over every codec and a real upstream
segment, the child-process memory row, the complete-lane rows, one compose row
through the shipped engine (`0.23.3+logweir.3`, Kafka 4.3), 26 mutants over
items 103 and 104, and the console's rows.
**Scope (PoC):** K-FX30-1, row 48 of `deep-ui-2` (build `sha-519ab67c`): a
restore of a catalog point with complete coverage ended `pass`,
`complete.covered: true` with no `incompleteReason`, 400 of 400 records. The
PoC holds no oversized or corrupt segment, so the caps rest on the rows above;
the restore page could not show the result then (DUI-16, item 111).
**Rollback:** an older runner decodes each segment whole again; no scorecard
field or format changed.

#### 104. A capped, cut or partly unreadable read says partial: protection, the catalog, the restore preflight, a teardown and a switchover (FX-38)

**Changed.**
- A ProtectionPolicy over a namespace with more than 1,000 `Backup` objects
  now reads `Unknown`, reason `BackupHistoryTooLarge`, instead of a verdict
  over the runs it happened to read. `Backup`s are listed by name, so the
  newest were the ones left out. It publishes no newest point, last attempt or
  failure streak then, and opens no alert from them.
- A catalog line that cannot be read is no longer skipped silently:
  protection reads the view as `CatalogUnreadable`; the restore preflight
  answers `CatalogPointViewUnavailable` instead of "not found"; the API's
  points page says `incomplete: true`; and the API publishes a catalog that
  has not said whether its view is a window as `truncated: true`, never
  `false`.
- A restore's teardown that left more than 256 topics says how many it left:
  `teardown-failed-count=`, `Restore.status.teardown.failedCount`, and the
  API's `failedCount`. A TTL clone's cleanup also records `deletedCount` and
  `failedCount`.
- A catalog's `Synced` message calls the view a window only when it is one,
  and names entries left out for page space, which a larger `viewLimit` does
  not cure.
- A `Switchover` whose group lines could not all be read, or whose log was not
  read, says its groups and counts are partial, on the object and through the
  API's `message`, instead of publishing zero counts.

The console reads `truncated` as "not known to be the whole archive", shows
the window sentence only when `Synced` says so, and shows "not known" for the
point, the attempt and the streak under `BackupHistoryTooLarge`.
**Do:** where a policy reads `BackupHistoryTooLarge`, delete old terminal
`Backup` objects (the archive keeps their points) and the policy evaluates
again. Apply the CRDs first: the `Restore` CRD gains
`status.teardown.failedCount` and `deletedCount`.
**Scope:** the protection, catalog, preflight, restore and clone controller
rows, the API rows (`d3_reads`, `operation_view`, `switchovers`), the mutants
of item 103, and the console's rows.
**Scope (PoC):** on build `sha-519ab67c`, 2026-10-14: `poc-v11` row 4
(K-FX38-4: no policy reads `BackupHistoryTooLarge` over 9 Backups; a transient
policy reads `WithinObjective`), row 5 (K-FX38-L4: neither catalog's `Synced`
calls a whole view a window) and row 6 (K-FX38-6: a synced catalog serves its
status's `truncated`, an unsynced one `truncated: true`, and a whole view's
points page has no `incomplete`); `deep-ui-2` (K-FX38-8: two clones `Removed`
at expiry, `deletedCount` equal to the length of `deleted`, `failedCount` 0);
`deep-ui-3` row 77 (K-FX38-SW: a Succeeded Switchover's page shows its
message and the counts line). The PoC holds no namespace of more than 1,000
Backups and no teardown that left topics.
**Rollback:** an older controller ignores `teardown-failed-count=` and judges
protection over the runs it read; an older `Restore` CRD prunes the new
teardown fields; an older API publishes an unsynced catalog as
`truncated: false`.

#### 105. A restore bound to a recovery point reads only that point's own receipt (FX-36)

**Changed.** The runner refuses, before reading anything, a plan whose
`source.point.receipt_key` is not
`logweir/backups/<source.backup>/<run id>.receipt.json`, and answers a receipt
that is absent, has other bytes or is over the 64 MiB read cap with one exit-3
`PointBindingMismatch` that does not say which (an absent receipt was exit 1).
The same confinement covers the positions document a restore maps consumer
positions from, the switchover's scorecard, mapping and rollback record (a
rollback plan's `rollback_of.key` must be a switchover record key), the clone
cleanup's scorecard, and the catalog's deep check, which lists a record naming
any other key `Unreadable` and reads neither key, and a record relabelled onto
another point's receipt `Conflict`. A switchover's `MappingDigestMismatch` now
also covers an absent mapping and a scorecard key that is not a scorecard's.
The console shows a refused run's sentence verbatim and offers neither kind of
point.
**Do:** a point-bound `Restore` whose receipt is gone now ends refused, not
failed and retried; restore a point the catalog lists. A rollback plan names
its record as `logweir/switchovers/<run id>.record.json`.
**Scope:** the binding, positions, switchover, cleanup and catalog rows, which
count every store read; compose rows on Kafka 4.3.1 (the point-set binding row
with a live negative control, theme A's switchover and clone rows); 23
mutants over items 105 and 106, and the console's rows.
**Scope (PoC):** on build `sha-519ab67c`: K-FX36-1, row 48 of `deep-ui-2` (a
catalog point's restore logs `recovery point binding verified`, and no
`PointBindingMismatch`); K-FX36-2, `poc-v11` row 2 (a sync on the new runner
and controller: all 411 points keep their availability and signature).
[UNVERIFIED — K-FX36-3 (a switchover plan whose `rollback_of.key` is not a switchover record, answered 422) has not run on a cluster; it needs an API call the UI-only passes do not make.]
**Rollback:** an older runner reads the receipt key the plan names, and an
older catalog sync lists a record by the keys it names.

#### 106. The controller judges evidence at its reconcile pass's own instant (FX-12)

**Changed.** A signing key's trust window and `verifiedAt` come from the
pass's one clock read, not a second read inside the verifier; the trust
policy, roster, destination and discovery reconcilers and the runner's phase 0
take an instant too, so their rows run at fixed instants and none depends on
the year.
**Do:** nothing.
**Scope:** every verification path judged at a handed instant (2001, 2026 and
2999), the shifted-clock run at +26752 days with its control, the mutants of
item 105.
**Scope (PoC):** K-FX12-1, `poc-v11` row 3 (build `sha-519ab67c`): every
Backup's and Restore's evidence verdict and every `TrustPolicy` key's
`effectiveState` are what they were before the upgrade, after the new
controller evaluated them again.
**Rollback:** an older controller reads the clock again inside the verifier;
away from a key's window edge its verdicts are the same.

#### 107. A backup whose principal may not READ a source topic is named before the engine (FX-45)

**Changed.** The Backup Preflight's `connection.topicsReadable` turns blocking
`notReady` (`TopicAuthorizationFailed`) when the broker's authorized
operations (KIP-430) say the principal may not READ a selected topic, naming
the topic and the grant. `logweir backup run` (and every Backup Job) refuses
such a topic before the execution claim and the engine, exit 3, instead of
failing with "kafka-backup backup exited 1". The standalone commands now run
PROD-01.2's capability rows first:
- `backup run` refuses a source `connection.engineProtocol` is `notReady` for;
- `drill run` refuses a target `target.engineProtocol` is `notReady` for at
  phase 0, before anything is created (a restore into Redpanda v26.2.4 now
  stops there, exit 3, instead of failing in the engine and leaving its scratch
  topic);
- `logweir doctor` reports it as check 8, `target engine`.

An answer that could not be observed is never a refusal. The console's backup
readiness check names the missing READ, with the topics and the grant.
**Do:** grant READ on each topic a refusal names to the principal the
connection authenticates as, or remove the topic from the selection.
**Scope:** compose rows on Kafka 4.3.1 through the shipped runner image (an
ACL principal without READ, Redpanda), the doctor row, the crate rows, 16
mutants over items 107 and 108, and the console's rows.
**Scope (PoC):** on build `sha-519ab67c`: K4, `deep-ui-1` row 32 (the
schedule form's readiness lists `connection.topicsReadable` under "Only the
run itself can answer", "the broker reports READ … on all 2") and row 31
(*Back up now* runs succeed); K5, `deep-ui-2` row 49 (a scratch restore
`Succeeded` `pass`, its runner log carrying phase 0's `target.engineProtocol`
line).
[UNVERIFIED — K4's runner-log lines (READ reported for the backup principal, the source's engine protocol before the engine starts) were not read on a cluster; the PoC re-check reads them.]
**Rollback:** an older runner starts the engine and fails exit 1 again; an
older controller reads the row as before.

#### 108. A connection test no longer calls a cluster reachable when only its bootstrap address answers (FX-46)

**Changed.** `logweir cluster-probe` asks every broker the cluster advertises.
A silent one makes it `reachable=false`, exit 1, with a third line
`brokers-answered=<answered>/<listed>` (`0/?` when not even the broker list
could be read through the bootstrap address), and the `KafkaCluster` reads
`AdvertisedBrokersUnreachable`. A `SourceConnection` Preflight (Test
connection) now runs `connection.engineProtocol`, `target.engineProtocol` and
`connection.groupTypes`, advisory, so it says whether the endpoint is usable
as a backup source and as a restore target (Redpanda: a source, not a target).
The console says `AdvertisedBrokersUnreachable` in its own words and, on a
connection's page, what the endpoint is usable for.
**Do:** keep the runner image at least as new as the controller: an older
runner refuses the new `capabilityChecks` field of a connection test.
**Scope:** compose rows on Kafka 4.3.1 (an unreachable advertised listener,
Redpanda), a librdkafka mock-cluster row for the silent broker, the probe and
controller rows, the mutants of item 107, and the console's rows.
**Scope (PoC):** on build `sha-519ab67c`: K1, `poc-v11` row 1 (every
`KafkaCluster` stays `Reachable`, "1 of the 1 broker(s) the cluster
advertises answered", each probe log `brokers-answered=1/1`); K3, `deep-ui-1`
rows 22 and 23 (Test connection on the Kafka 3.7.1 source: "Usable as a source
and as a target", consumer group positions "No", the three lines again after a
reload) and `deep-ui-3` (on Kafka 4.3.1 all three lines "Yes").
[UNVERIFIED — K2 (`AdvertisedBrokersUnreachable` on a cluster) has not run: no PoC listener advertises an address its namespace cannot reach, so the compose row is the only evidence.]
**Rollback:** an older runner prints no third line and the reason stays
`ProbeReportedUnreachable`; an older controller asks a connection test for no
capability row.

#### 109. *Test access* tests the credential a destination has, and a stale edit is "changed since it was read" (DUI-1, DUI-2)

**Changed.** With no grant ticked, a destination's *Test access* now tests
every grant a run uses: Archive write, Archive read and Evidence write (unset,
the last two use the archive-write credential), and Evidence read when it is
set. It used to test only the grants the destination set. A destination with
only an archive-write key was then tested for that grant alone, which no check
dials, so a key the endpoint did not know read **Ready**; it now reads **Not
ready**, `destination.archiveListable` `InvalidCredentials`. No test writes
with the archive-write grant itself: it is dialled through Archive read or
Evidence write when they fall back to it, else only the run checks it (a
destination with its own `archiveRead` and `evidenceWrite` credentials, or the
write probe off). A destination whose `archiveRead` and `archiveWrite` are two
different workload identities, with `evidenceWrite` unset and the write probe
on, now gets `ExecutionContextConflict` from the default test; ticking grants
still works. `:update-access` and `PUT …/schedules/{name}` now check `expectedGeneration` before any field rule:
a request written against an older generation is `412 precondition_failed`
("…it changed since it was read, and nothing was written. Read it again."),
whatever its fields say. A stale *Rotate access* used to get
`422 existing_credential_refused` on a field the dialog does not show. A
schedule edit for a name that does not exist is now `404` before its field
rules (it was `422`). The console marks an API field error about an input the
chosen source hides on the control that shows it (the grant's Credential
select, the CA's Change select), in *Rotate access* and *Add a destination*.
**Do:** re-run *Test access* on every destination that sets only an
archive-write grant.
**Scope:** API rows for the default set (control: named roles are sent as
named), and for a stale rotation and a stale schedule edit (412 before any
field rule; control: the same body at the current generation gets its field
error); a controller row holding the default set to the roles that resolve to
a credential; a live row on a compose MinIO (an unknown key reads `notReady`
`InvalidCredentials` under the default set and `ready` under the old one;
control: the good key reads `ready`); the console's placement rows; a mutant
on each rule was killed.
[UNVERIFIED — not yet run on a cluster; the deep UI re-check runs DUI-1 and DUI-2 on the PoC.]
**Rollback:** an older API tests the old set, and answers a stale edit by its
field rules again; nothing stored changes.

#### 110. A backup of a topic the source does not hold is refused by name, a failed run says why, and a schedule names its consumer groups (DUI-5, DUI-25, DUI-3, DUI-13, DUI-31)

**Changed.**
- **A selected topic the source does not hold.** When the source's
  DescribeTopics answers `UNKNOWN_TOPIC_OR_PARTITION` for a selected topic and
  DescribeCluster says the principal may Describe the cluster, `backup run`
  refuses before the execution claim and the engine: exit 3, `GuardRefused`,
  "topic `x` does not exist on the source …", with the remedy, and nothing is
  written. Such a run used to fail "the runner exited 1 (operational)" with no
  cause. The Backup Preflight's `connection.topicsReadable` is blocking
  `notReady` `TopicNotFound` for that topic, ahead of a missing READ
  (item 107). Without the cluster's Describe, and for any other describe
  failure, nothing changes: the engine's own fetch answers.
- **Every other exit 1.** A `Backup` or `Restore` that fails at exit 1 carries
  the error it ended on in its `Failed` condition, its
  `status.progress.message`, the API's operation `message` and the console's
  run page: "…; the runner's last error, cleaned and bounded: …". For an
  engine failure it is the engine's last line saying "error". The runner
  prints it as a new `failure-detail=` line, sealed with the Job's
  `--runner-line-token` only (item 96) and first in its final block; the
  controller reads it only sealed, cleans it again and cuts it to 760 bytes.
  `exitReason` and the retry classification are unchanged.
- **Consumer groups from the console.** `POST …/schedules` takes an optional
  `consumerGroups`, checked by the runner's own rule before anything is stored
  (`duplicate`, `invalid_consumer_group` or `count_out_of_range` on the field)
  and stored as `spec.consumerGroups`; an edit keeps the stored list. The
  schedule and run views (`Schedule.consumerGroups`, `Backup.consumerGroups`)
  and pages list the groups, and the restore wizard says when a point maps no
  consumer position, so its restore cannot be switched over.
- **The console:** the Overview's newest recovery point is the newest of each
  policy's point, every verified backup and each catalog's newest selectable
  point, and names its source; a new schedule shows its next runs without a
  reload, and an edit keeps what was typed after a `412`; the Schedules list's
  last run counts *Back up now* runs; *Back up now* reads the schedule again
  before it offers a revision; the Overview's failed-run item opens the last
  failed run.

**Do:** nothing. To record consumer positions, name the groups when you create
the schedule (`kubectl` can still set `spec.consumerGroups`).
**Scope:** the access rows (a missing topic is absent only with the cluster's
Describe), the `backup run` and Preflight rows, each with its control, and a
compose row on Kafka 4.3.1 (`[existing, missing]` reads `notReady`
`TopicNotFound`, exit 3, no receipt and the engine not started; control: the
existing topic alone backs up); the `failure-detail=` rows in the core, the
runner, both reconcilers, the forged-line table and the API, and a live
compose row (a password the broker refuses: exit 1, the cause in
`status.progress.message`, the password nowhere; control: no line without the
token); the API's `consumerGroups` rows (a 46-character id stored and read
back; five refusals, nothing written; control: an empty list is stored
absent); the console's rows; a mutant on each guard was killed.
[UNVERIFIED — not yet run on a cluster; the deep UI re-check runs DUI-3, DUI-5, DUI-13, DUI-25 and DUI-31 on the PoC.]
**Rollback:** an older runner starts the engine for a missing topic and prints
no `failure-detail=`, which an older controller ignores; the console and the
API roll back as one image, and schedules keep their stored
`spec.consumerGroups` (rollback step 5).

#### 111. Every finished restore's page opens, and the wizard asks who owns the topics (DUI-16, DUI-17, DUI-18, DUI-22, DUI-23)

**Changed.**
- A restore whose target broker sets no timestamp bound (the default of
  `log.message.timestamp.before.max.ms`) crashed its page once the run ended
  ("Invalid time value"), so no verdict, coverage, evidence, Clone or Topic
  configuration card could be read. The bound is now read as a duration
  ("Any age: the broker sets no bound"), a number no date holds reads "not a
  time", and every page has an error boundary: a page that cannot render says
  so, and the navigation stays.
- When a restore takes the source's topic configuration from a point whose
  backup did not look for owners, the wizard asks who owns the topics ("No
  topic here is managed by an owner", or "Some are: name them") and holds Next
  until it is answered; the plan carries the `owners` list the runner reads
  (item 92). The check step holds Next and Create while the configuration
  preview refuses. Before, the wizard built a plan the runner always refused,
  `TopicOwnerNotChecked`.
- A confirm namespace's restore reads "The controller is verifying your
  confirmation" in its first seconds, not "an approver who is not you". The
  controller's 30 s wait before it reads the approval again is unchanged.
- The Restores list says "clone, deletes in 45 min" for a kept clone; the
  wizard opens each step at its top, names each point's radio, and writes check
  details and instants in words; a restore waiting for its approval reads
  "Waiting for approval".

**Do:** nothing.
**Scope:** the console's rows: a finished restore over a target with no bound,
and a number that is not a time (control: a real instant); a page that throws
(control: the other pages); the owners question with its four refusals
(control: a point that looked is not asked); the plan golden
`plan-owners.golden.yaml`, which `console_plan.rs` parses into `RestoreSpec`
and the runner's owner rules (control: the clone golden's `owners: []`); the
confirm card, the wizard and the clones list; mutants over each were killed.
[UNVERIFIED — not yet run on a cluster; the deep UI re-check runs DUI-16, DUI-17, DUI-18, DUI-22 and DUI-23 on the PoC.]
**Rollback:** an older console image crashes on such a page again and builds
no `owners` list; nothing stored changes.

#### 112. A switchover waiting for its approver is listed in Approvals, and a requester is named by a signed, provider-verified identifier (DUI-20, DUI-21, DUI-27, DUI-28, DUI-30, DUI-31)

**Changed.**
- **Approvals.** A `Switchover` held at `Pending`, `ApprovalNotVerified`, is
  listed beside the waiting restores, counted in the navigation when it is
  offered to this session, and opens its own approval page under two-person
  (the Switchover's page under strict or files). The console's own two-person
  request record (`Verified=False`, `GovernedApprovalRequired`) reads "Request
  recorded", not "Refused".
- **Who asked.** At request time the API signs a requester-display document
  with the console key, under its own payload type
  (`application/vnd.logweir.requester-display+json`), and stores it in the
  `logweir.dev/requester-display` annotation and its signature, never in the
  request's signed bytes. It holds the requester's email only when the
  provider marked it verified (`email_verified: true`) and it has an address's
  shape (printable ASCII, at most 254 bytes, one `@`), exactly as received,
  and a name only from a claim the install opted into. The request views serve
  `requesterEmail` and `requesterName` only when that signature verifies for
  this request (the same requester, approval name and plan hash); otherwise
  they serve `requesterDetailsNotVerified: true` and neither. The console leads
  "Requested by" with that email, else the signed principal in full, never
  shortened; an opted-in name follows, marked as not verified. The switchover
  approval page names its requester too, and the Approvals list shows
  principals in full.
- **The sign-in.** The `name` claim is opt-in (`oidc.displayNameClaim`, no
  default): a session's display name is the provider-verified email, else the
  subject. The default scopes ask for `email` too (`openid profile email
  groups`).
- **The cutover** reads the restore afresh on every visit, so a just-finished
  restore is not refused from an old copy, and offers "Review again" after
  every ended switchover (its page, the cutover's result and the Switchovers
  list). A role without `restoreCreate` is offered no review and no "Review
  again", and the cutover sends nothing for it. A switchover with no phase yet
  reads "Submitted", and both approval pages write the server's instants in
  words.

**Do:** nothing. A chart install takes the API's defaults: `email` is now
requested, and no name claim is read, so its sessions show the
provider-verified email, else the subject. See *Required operator actions*
for the `email` scope.
**Scope:** the API rows: a tampered email (signature kept), and a display
signed for another plan hash or approval name, each not served and flagged
(control: a valid display is served); the email shape (a zero-width space,
whitespace, non-ASCII, 255 bytes, each refused; 254 bytes kept whole); the
sign-in (an email the provider did not mark, `email_verified` as a string, the
subject as the fallback; the `name` claim read only when configured); the
console's requester rows (control: an older request shows the principal only);
the Approvals, cutover and viewer rows (controls: a Succeeded switchover is not
listed; the requester's own request counts nothing; an operator's cutover
sends its one review); mutants over each were killed.
[UNVERIFIED — not yet run on a cluster; the deep UI re-check runs DUI-20, DUI-21, DUI-27, DUI-28, DUI-30 and DUI-31 on the PoC.]
**Rollback:** an older API reads the `name` claim by default again, ignores the
requester-display annotations and serves none of the three fields; an older
console lists no switchover in Approvals. Either build reads the other's
session cookie (its new `em` field is optional); a session sealed before the
upgrade shows the subject until it ends.

#### 113. Long topic and consumer group names are shown whole, and their switchover applies (DUI-26)

**Changed.** A topic name or consumer group id of 40 or more characters was
read as a credential and shown as `[redacted]`: in the catalog's consumer
groups, the restore preflight's samples and details, and the switchover
review. A switchover planned from that review was always refused
`PlanDisagreesWithMapping`. These names now travel in typed fields: details
`topic`, `mappedTopicExists`, `group` and `target`; catalog `groupId` and topic
`name`; a check row's topic samples. A legal name (a topic of
`[a-zA-Z0-9._-]`, at most 249 characters; a group of printable ASCII, at most
255 bytes) passes the specific secret-shape rules (PEM, S3 XML, URL userinfo,
a keyed value, an access key id) but not the 40-character rule, so it arrives
whole. Free text is redacted as before, so a refusal sentence that names a
long group (`GroupActive: …`) still reads `[redacted]` there; the typed field
beside it carries the name.
**Do:** roll the runner and the controller together (the controller's own pass
would still redact a newer runner's long names); then set each
`RecoveryCatalog`'s `spec.syncRequest` to a new value and review each cutover
again, so the names show. Catalog rows synced before keep `[redacted]` until
that sync, and a finished Preflight keeps its details.
**Scope:** the core rows (60-character names survive the typed walk; the
free-text rule alone gives `[redacted]`; a secret-shaped run in `message` or
under an untyped key is still redacted; an access key id planted in a topic is
removed); the runner's review, catalog and sample rows and the controller's
details row, each with its control; a live compose switchover on Kafka 4.3.1:
a 60-character group over a 60-character topic is reviewed, planned from the
review's details as the console plans it, approved and applied (exit 0,
`Succeeded`, the offsets read back); control: the old pass names both
`[redacted]` and the plan built from it is refused `PlanDisagreesWithMapping`,
exit 3, nothing committed. One mutant was killed.
[UNVERIFIED — not yet run on a cluster; the deep UI re-check runs DUI-26 on the PoC.]
**Rollback:** an older runner or controller redacts such names again, and a
switchover planned over one is refused again; nothing stored changes.

#### 114. The console warns before a session ends and keeps the page after it (DUI-4, DUI-9)

**Changed.** About two minutes before the session's `expiresAt` (the cap is
`sessionMaxAgeSeconds`), a warning offers **Sign in again in a new tab**, which
renews the cookie this tab shares. A later `401` no longer sends the page to
the sign-in page: a blocking dialog, "Your session ended: sign in again in a
new tab, then continue here", keeps the page and what was typed on it, with no
browser storage, until a read of the session succeeds (Continue here, or coming
back to the tab). Reads refused while signed out are read again, and a
different person signing in gets a Reload dialog. At the local expiry the page
asks the service, which decides. The sign-in link follows the address when it
changes in the same tab.
**Do:** nothing.
**Scope:** the console's rows: a `401` keeps a half-typed form, and Continue
before the sign-in still says signed out; the warning at about two minutes
(control: at ten, none); the sign-in link follows a hash change (control: the
first `next`); mutants on the re-read, the link and the dialog were killed.
[UNVERIFIED — not yet run on a cluster; the deep UI re-check runs DUI-4 and DUI-9 on the PoC.]
**Rollback:** an older console image sends a page whose session ended to the
sign-in page again; nothing is stored.

#### 115. Tables and forms fit, links go where they say, and states read in words (DUI-6, DUI-7, DUI-8, DUI-10, DUI-11, DUI-12, DUI-14, DUI-15, DUI-19, DUI-24, DUI-29, DUI-31)

**Changed.**
- **Layout.** Table cells marked to break wrap long values anywhere, and
  column headers wrap between words instead of ending in an ellipsis; the
  check table gives its columns fixed shares; form toggles and dialog titles
  wrap, so every form and dialog fits a 390 px screen; a compact list wraps a
  long name beside its status; a new page opens at its top (a filter change
  keeps the place); the Overview's policy column is no longer crushed.
- **Words.** `ProbeReportedUnreachable` and a probe running again carry the
  controller's words, and an unreachable connection's Overview item says what
  it stops by its role; a `DuplicateCatalog` leads its catalog's page and the
  list, and *Connect an archive* warns before it is sent; the catalog list
  names only catalogs whose view is a window; problem details show the API's
  code spans as code (as text); readiness notes are a list; the Evidence card,
  the installation-default trust policy and an administrator's empty
  Protection page say what they mean; "1 topic".
- **Links.** The schedule page's source and destination, and the Overview's
  schedule names, link to their pages; the Overview's cells are dated; the
  Protection ledger lists `RecoveryCompleted` under "Recoveries recorded", not
  as a problem.
- **The user menu** shortens the subject, with the whole value in its title,
  and a theme choice closes it.

**Do:** nothing.
**Scope:** the console's rows (code spans, instants and check details; the
readiness list; the duplicate and window catalogs; the ledger; the links; the
probe gloss; the scroll reset, with a search-only change as its control), and
two screenshot walks over the single-file mock build at 1440 and 390 px, light
and dark (72 and 46 shots): 0 screens wider than the viewport, 0 children past
a card or dialog edge, 0 list labels over a name, 0 axe violations; mutants
over the rows were killed.
[UNVERIFIED — not yet run on a cluster; the deep UI re-check runs DUI-6 to DUI-8, DUI-10 to DUI-12, DUI-14, DUI-15, DUI-19, DUI-24, DUI-29 and DUI-31 on the PoC.]
**Rollback:** an older console image shows the old layout and words; nothing is
stored.

### Required operator actions after `v0.2.0-rc.1`

In addition to the next entry's six, in its order.

**Before `helm upgrade`, in the values file:**
- **Remove the `notify` block** (item 74): the chart refuses it by name. With
  `--reuse-values`, pass `--set notify=null`.
- **Remove any `ui` block** (item 75): the chart refuses `ui.*` by name. Use the
  console for the page.
- **Move a source-level `kafka.markerTopic` to `kafka.target.markerTopic`**:
  the chart never read the source's key (rc.2 accepted and ignored it) and now
  refuses it by name. With `--reuse-values`, pass `--set kafka.markerTopic=null`.
- **Name the trusted proxy of a shared console the chart publishes** (item 53):
  with `api.console.mode: shared` and `api.console.ingress.enabled: true`, set
  `api.console.trustedProxyService` to the ingress controller's Service (or
  `api.console.trustedProxyCidrs` to its pods' range), or
  `api.console.trustedProxy: none` deliberately. Otherwise the render fails,
  naming the value.
- **Rename any approval policy named `default-confirm-v1`** (item 29): the
  chart refuses it at render, and the controller and the console at start.
- **An install made with `kubectl apply -f logweir.yaml` moves to the chart**
  (item 76); the two commands there remove the bundle's control plane and keep
  everything else.
- **A render that cannot ask the cluster now stops** (item 83): Argo CD,
  `helm template | kubectl apply` and `helm lint` set
  `identity.renderedOffline` (`true` to apply, `false` for a preview).

**Before the controller rolls:**
- **List every `RehearsalSchedule`** (item 73): after the upgrade none runs,
  and its CRD stays until you delete it.
- **Read `status.lastEvaluation.viewIncomplete` of every `Enforce`
  `RetentionPolicy`** (item 64): where it is `true`, the policy deletes nothing
  until its catalog's view is whole. Re-create a catalog whose `viewLimit` cut
  its view with a larger one (`spec.sync` is immutable), and read a stopped
  walk's `Synced` condition. An archive of more than 5000 points is not enforced
  on v1.
- **Split every `Backup` and `BackupSchedule` that names or resolves to more
  than 1,000 topics** (item 65); the two commands in
  [kubernetes.md](kubernetes.md#upgrade) find them. After the upgrade such a
  selection is refused by name at each run.
- **Apply the CRDs first**, as the next entry's step 6 orders: items 33, 38,
  43, 50, 51, 56, 58 and 64 add fields the controller writes or reads.
- **Update any script that reads `kubectl get retentionpolicy` by column
  position** (`HELD-BACK` is new, before `EVALUATED`), and any API client that
  reads `lastEvaluation.kept`: read `lastEvaluation.accounting` first (item 58).

**Before the runner image rolls:**
- **Read the plan preview of every `Enforce` `RetentionPolicy` with
  `requireApprovedPlan: false`**, or set it to `true` until you have read its
  first plan after the re-sync below: scheduled sets it never expired become
  candidates (item 31).
- **Remove the sinks from every restore plan that names one** (item 74) and
  approve it again: a plan whose `notifications` block names a sink is refused.
- **Name the point's set in every standalone point-bound plan that says
  `backup: latestCompleted`**, and approve it again (item 34).
- **Add `restore.allow_transaction_markers: true` to the plan of every restore
  of a transactional topic** that must restore as it is (Kafka Streams or
  Connect exactly-once, Flink), and approve it again (item 67).
- **A consumer that matches a scorecard's exact `format_version`** must accept
  `1.6.0` (item 36).

**The roll:**
- **Move `controllerImage` and `runnerImage` in one `helm upgrade`, and never
  the controller alone** (items 28, 35, 41, 48, 57, 60, 61, 65, 67, 82, 108 and 113). This
  controller gives every `Restore` and `Backup` Job a `--line-token` argument,
  and every runner image published before that change, the images of this
  same entry included, stops on it: the object says `exitCode: 1`,
  `exitReason: operational`, and the runner pod's log opens with
  `error: unexpected argument '--line-token' found`. A `runnerImage` pinned
  apart from the controller (a mirror, an air gap) must move with it, and a
  pinned `runnerImage` survives `--reuse-values`. A `Preflight` that failed
  `CheckContractMismatch` stays `Failed`; create a new one. To roll back, move
  both together or the controller first.
- **Roll the console with the controller and the runner** (items 33, 38 and
  56).
- **Let the identity provider serve the `email` scope** (item 112): a chart
  install takes the API's defaults, and `email` is now requested
  (`openid profile email groups`). A provider that refuses a scope refuses the
  sign-in; `email` is a standard OpenID Connect scope.
- **Verify an image digest before you deploy it** with the pinned commands in
  [install.md](install.md#verify-the-images) (item 35). A standalone CLI install
  replaces its engine with Logweir's build and exports
  `LOGWEIR_ENGINE_VERSION=0.23.3+logweir.3` and its digest (items 28, 35, 41
  and 82),
  and every spec that names an `http://` archive endpoint says
  `allow_http: true` (item 28).
- **Back up the console key too, once it exists** (item 29):
  `logweir-console-confirmation` and `logweir-console-trust`, beside the
  installation identity the next entry's step 1 names
  ([install.md](install.md), *Back up and recover the installation identity*).
- **An ingress controller that dials the console with HTTP/2** (`h2c` or gRPC)
  goes back to HTTP/1.1 (item 40).
- **Check the `archiveRead` grant of every destination you restore catalog
  points from** (item 57): `s3:GetObject` on `<bucket>/logweir/backups/*` and,
  on a versioned bucket, `s3:GetObjectVersion` on `<bucket>/<prefix>/*`;
  without them the restore preflight answers `archive.backupSet`
  `AccessDenied` ([kubernetes.md](kubernetes.md) §7a).
- **Check a restore identity's Kafka ACLs** against
  [the measured minimum](support-matrix.md#minimum-permissions) (items 60 and
  69): `Read` on the target topics, `Describe` on the scratch marker topic and
  `Delete` on the scratch prefix.

**Credential Secrets (items 33 and 38), around the roll:** suspend the
schedules that use a credentialed connection or a credential Secret before the
roll; apply the CRDs and roll the controller, the runner and the console
together; then bind, one at a time and never in a loop:
- each credentialed connection's Secret, after an inventory of which
  `KafkaCluster` names which Secret, stopping on any Secret named twice (an
  incident, not a split), by a command that names both, once its owner confirms
  the endpoint ([kubernetes.md](kubernetes.md) §20.9);
- every destination, retention and inline-archive credential Secret with
  `scripts/bind-credential.py` ([install.md](install.md);
  [kubernetes.md](kubernetes.md) §20.10).
Resume the schedules. Until a Secret is bound its runs are refused
(`CredentialBindingMismatch`), closed. A connection with no credential needs
nothing. Remove `api.logweir.dev/request-sha256` from each `BackupDestination`
an earlier build created with `secret.new`, or rotate its key: the create's
audit record keeps that hash, so only a rotation retires it (item 33).

**After the upgrade:**
- **Set each `RecoveryCatalog`'s `spec.syncRequest` to a new value** once the
  runner image has rolled, so its view is published again by the new runner
  (items 31, 65, 71 and 113), and review a cutover again before you plan a
  switchover from it (item 113).
- **Re-run *Test access* on each destination** (items 48 and 109), and bind its
  own Secret where a grant reads `CredentialBindingMismatch`.
- **Upgrade every verifier that will read a partition-subset restore's
  scorecard** before one is run (item 51): older verifiers refuse format 2.0.0.

### Verification scope after `v0.2.0-rc.1`

- **Complete coverage can be asked for by a plan, a `Restore` and the console**
  (items 30 and 43). A `Restore` that does not ask still verifies a sample. The
  signed scorecard says which in `integrity.verification.coverage`; the
  `Restore`'s status, the product API (`Restore.coverage`,
  `verificationScope.coverage`) and the console repeat it. A complete
  verification that did not cover the restore (`covered: false`) is never a
  pass.

### Migration and rollback after `v0.2.0-rc.1`

An upgrade from `v0.2.0-rc.1` (publication `2c277dc1`) crosses items 28, 29, 30,
31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50,
51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70,
71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97,
98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114 and 115, in the order of the next
entry's upgrade path. What each item changes, and what it needs beyond the actions above:

| Item | Changes | Needs |
|---|---|---|
| 28 | the engine in the controller and runner images | the two rolled together |
| 29 | the identity hook, console and chart values (`identity.bootstrapFeatures.consoleKey`, `approvalPolicy.default`) | nothing until set |
| 30 | the runner's scorecard (1.4.0) | nothing |
| 31 | the runner's catalog sync and the console's text | a re-sync of each catalog |
| 32 | receipts and records (1.3.0), the catalog's view, the product API, the console | nothing |
| 33 | the controller, the runner, the console, the `KafkaCluster` CRD | each credentialed connection's Secret bound |
| 34 | the runner | standalone `latestCompleted` plans renamed |
| 35 | the runner image (its engine and platforms) and the controller's Job environment | the two rolled together |
| 36 | the runner's sampled verification (1.6.0) | nothing |
| 37 | library calls no command uses | nothing |
| 38 | the controller, every runner, the retention worker, the API, the console, two CRDs' status | each credential Secret bound |
| 39 | the runner's phase 0 | nothing |
| 40 | the console | an `h2c` ingress backend returned to HTTP/1.1 |
| 41 | the runner's phase 7 and the engine build (`+logweir.2`) | the two rolled together |
| 42 | the runner and the restore preview (scorecard 1.7.0) | nothing without a window start |
| 43 | the `Restore` CRD, the controller, the runner's metrics, the API, the console | the CRDs applied |
| 44 | the console | nothing |
| 45 | the controller (and two CRD descriptions) | nothing |
| 46 | receipts and records (1.5.0), the catalog's view, the API, the console | nothing |
| 47 | the console | nothing |
| 48 | the controller and the runner (the check plan and a row) | the two rolled together |
| 49 | receipts and records (1.6.0) | nothing |
| 50 | receipts and records (1.7.0), the catalog's view, the API, two CRDs | nothing until a group is selected |
| 51 | the runner, the preview, both verifiers, the `Restore` status, the API, the console | verifiers upgraded before a subset restore |
| 52 | the controller, the runner, the check Jobs | nothing |
| 53 | the console and the chart | a published shared console's trusted proxy named |
| 54 | the controller | nothing |
| 55 | the console | nothing |
| 56 | the `Restore` CRD, the controller, the runner, the API, the console | the CRD applied before the controller |
| 57 | the runner's preflight and two codes the controller relays | the two rolled together; a narrow `archiveRead` widened |
| 58 | the controller, the `RetentionPolicy` CRD, the API, the console | the CRDs applied; column scripts updated |
| 59 | the console | nothing |
| 60 | the controller's check plan, the runner's check rows, one `Restore` status field | the runner rolled with the controller |
| 61 | the runner, the controller, the console's text | one `helm upgrade` for both images |
| 62 | the runner's engine documents and the console | nothing |
| 63 | the console | nothing |
| 64 | the controller, the `RetentionPolicy` CRD, the console | the CRDs applied; `viewIncomplete` read |
| 65 | the runner, the check Jobs, the controller, the API | large selections split; a re-sync |
| 66 | the controller, the runner, the API, the console, the chart | nothing until a namespace is bound `two-person` |
| 67 | the runner and the controller's restore refusal codes | the two rolled together; the plan field for transactional topics |
| 68 | the runner (receipt 1.8.0), the controller, both verifiers | nothing |
| 69 | the runner and the controller | nothing, in either order |
| 70 | the console | nothing |
| 71 | the runner and the controller | nothing, in either order |
| 72 | only how the repository is tested | nothing |
| 73 | removes a kind, a route and a flag | the `RehearsalSchedule` CRD left for the operator to delete |
| 74 | removes sinks, the drill's post, the ACL read, a validation call | `notify` and plan sinks removed |
| 75 | the API, the console, the chart, the published image set | any `ui` block removed |
| 76 | no image | a bundle install moved to the chart once |
| 77 | the controller | nothing |
| 78 | the CRDs, the controller, the API, the console | nothing |
| 79 | the controller and the retention worker | nothing, in either order |
| 80 | the runner's phase 3 | nothing |
| 81 | only how the repository is tested | nothing |
| 82 | the engine in the runner image | the controller and runner images rolled together |
| 83 | the chart only | an offline render (Argo CD, `helm template`) sets `identity.renderedOffline` |
| 84 | the identity hook's image | nothing |
| 85 | the chart only | nothing until `minio.persistence.keep` is set |
| 86 | the API document's version (the console image) | nothing |
| 87 | the controller and the `RetentionPolicy` CRD | the CRDs applied |
| 88 | the console image (the API and the page) | nothing |
| 89 | the console | nothing; a script waiting for `#/history` after Create waits for `#/operations` |
| 90 | the runner's evidence writes | nothing |
| 91 | the `Restore` CRD (`status.clone`), the controller (a finalizer and a cleanup Job), the runner (scorecard 1.10.0), both verifiers, the API | the CRDs applied; the runner rolled with the controller |
| 92 | the runner (scorecard 1.11.0), the `Restore` CRD (`status.topicConfiguration`), the controller, both verifiers, the API | the CRDs applied; the restore principal granted `AlterConfigs` |
| 93 | the `KafkaCluster` CRD (`spec.strimzi`), the controller, the chart (`strimzi.*`) | nothing until `strimzi.readKafkaTopics` is set |
| 94 | the runner's restore selection and one controller refusal code | the two rolled together |
| 95 | a new CRD (`Switchover`), the `Approval` and `Preflight` CRDs, the controller, the runner (scorecard 1.12.0, a new record), both verifiers, the API, the chart's RBAC | the CRDs applied; the runner rolled with the controller |
| 96 | the controller and every runner image (a new flag on every runner Job), the `KafkaCluster` CRD (a pattern) | `controllerImage` and `runnerImage` rolled together |
| 97 | the documentation only | nothing |
| 98 | the controller (the `Switchover` and clone cleanup Jobs' grants and argv, `status.message`, a re-armed `RemovalFailed`), the runner (`switchover run`, `restore cleanup`, the review), the `Restore` CRD's description | an `evidenceRead` grant a pod can hold; the runner rolled with the controller |
| 99 | the API | nothing |
| 100 | both verifiers (`verify_scorecard.py` 1.34.0) | nothing |
| 101 | the controller | nothing |
| 102 | the console image (`logweir-console`) and the API's served files | nothing; bookmarks move to the new addresses |
| 103 | the runner's complete verification | nothing |
| 104 | the `Restore` CRD (`status.teardown.failedCount`, `deletedCount`), the controller, the runner (a new line), the API | the CRDs applied |
| 105 | the runner, the catalog sync, the switchover plan grammar (the API and the controller refuse a bad `rollback_of.key`) | the runner rolled with the controller |
| 106 | the controller | nothing |
| 107 | the runner (`backup run`, `drill run`, `doctor`) | nothing |
| 108 | the runner (the probe and the connection test), the controller, the `KafkaCluster` CRD (a description) | `runnerImage` at least as new as `controllerImage` |
| 109 | the API (the default access test, the precondition first) and the console | *Test access* re-run on archive-write-only destinations |
| 110 | the runner (`backup run`, the Backup Preflight, a new sealed line), the controller, the API (`consumerGroups` on create and in two views), the console | nothing, in either order |
| 111 | the console | nothing |
| 112 | the API (sign-in, the request's signed display, three view fields), the console | nothing; an identity provider that refuses the `email` scope allows it |
| 113 | the runner (details, catalog entries, samples) and the controller (the details pass) | the two rolled together; a re-sync of each catalog |
| 114 | the console | nothing |
| 115 | the console | nothing |

To roll back to `v0.2.0-rc.1`, in this order, on top of the next entry's
rollback steps:

1. **Remove every `two-person` policy** (item 66) before rolling an image back:
   an older controller or console refuses to start on a document that carries
   `approverSignature`.
2. **Remove `approvalPolicy.default`** (item 29): an older binary refuses a
   document carrying `defaultMode` at start. Unbound namespaces of a fresh
   install are `legacy-governed-v1` again under the older build.
3. **Remove `api.console.trustedProxy` from the values** (item 53),
   `identity.renderedOffline` (item 83) and `minio.persistence.keep` (item 85),
   before rolling the chart back: an older chart's schema refuses all three.
4. **Finish or delete any original-name `Restore`** (item 56), so none waits on
   a runner that refuses it; rolling the `Restore` CRD back prunes the field.
5. **Before rolling the controller back past item 50, let every run whose
   frozen inputs carry `consumerGroups` finish**, or remove
   `spec.consumerGroups` from its schedule: an older controller refuses such a
   frozen plan (`PlanConfigMapConflict`).
6. **If you deleted the `RehearsalSchedule` CRD** (item 73), re-apply the older
   chart's `crds/` (`kubectl apply --server-side --force-conflicts -f
   charts/logweir/crds/`) before rolling the controller back; Helm does not
   re-create it.
7. **Delete every `Approval` whose `subjectRef.kind` is `Switchover`, and every
   `Preflight` whose `spec.request.operation` is `Switchover`** (item 95),
   after recording them:
   `kubectl get approvals -A -o json | jq '[.items[] | select(.spec.subjectRef.kind == "Switchover")]' > switchover-approvals.json`,
   then the delete pipeline in [kubernetes.md](kubernetes.md#rolling-back).
   An older controller and console decode both kinds as closed sets: one such
   object fails the whole list, so no `Approval` is verified (every new
   `Restore` stays `Pending`, `ApprovalNotVerified`, while the controller logs
   ``unknown variant `Switchover` ``) and no `Preflight` is reconciled. It loses
   the approvals of switchovers not yet admitted. Let every `Backup` whose
   `KafkaCluster` sets `spec.strimzi` finish too (item 93: an older controller
   refuses its frozen plan), and every TTL clone settle or have its topics
   deleted and its finalizer removed (item 91).
8. **Roll the chart, the CRDs, the controller, the runner and the console back
   together** (items 28, 33, 60, 61, 65, 67, 74, 78 and 82). After the roll an
   older runner accepts more than 1,000 topics again and an older controller
   reads a receipt under 1 MiB again, so a backup of about 300 topics or more
   is `NotAttempted` again (item 65). If it must be two steps, the controller
   goes first: this controller over an older runner image starts no `Restore`
   and no `Backup` (item 61), and an older runner refuses a request for a
   receipt over 1 MiB (item 65). The older runner signs scorecard 1.3.0 and
   receipts 1.1.0 or 1.2.0 again; every document already written stays
   valid under this release's readers. A connection using a mode item 33 added
   stops working (an older build cannot parse it), so delete or re-create those
   first. The bound Secrets keep working (items 33 and 38). After the roll,
   write each `ProtectionPolicy` route again if you need the older alert sinks:
   the API server dropped their contents (item 74).
9. **To roll back only the engine** (item 35), use the one-release rollback
   image (`ENGINE_SOURCE=oso`, linux/amd64;
   [install.md](install.md#rolling-the-engine-back)) rather than an older
   runner; its documents say which engine ran.
10. **Roll the verifiers back last** (items 33, 51 and 68): an older reader
   refuses a 1.4.0/1.5.0 document that names a new auth mode (item 33), the
   2.0.0 scorecards a subset restore signed (item 51) and an empty capture's
   receipt (item 68).

---

## Unreleased — `main` after `v0.1.5`

The last tag is `v0.1.5` (`9cc78a3`). This entry covers `main` through
`fdb48cd8` (2026-09-25): the platform tracker's shipped tasks, the operator
actions collected for PLAT-20.2 and after it, and the upgrade from the last
published image. Items 21 (FX-2), 22 (FX-5), 23 (FX-10), 24 (FX-3), 25
(FX-13), 26 (FX-11) and 27 (FX-8), from the product-expansion tracker's fix-now
rows, land after `fdb48cd8`, and so do FX-7's additions to item 11 (the
execution-claim set check, receipt and catalog format 1.2.0, the pin's read
by version id) and FX-4's format 1.1.0, which has no item of its own. No tag is
cut at `fdb48cd8`, so the candidate record below is `v0.2.0-rc.1`'s. The
shipped task list, the publications the PoC ran, the tested environments and
the results were in the release handoff of `fdb48cd8`, now in git history.

### Candidate record

Fill every row for the exact candidate; a row left as `—` is an unrecorded
fact, not a pass. A previous run does not validate new bytes. A tag does not
rebuild the images: it gives the `sha-<commit>` publication `main` CI already
made the version tag, so the release dry run's `release.json` (workflow
artifact `release-assets`) gives the publication commit and the image digests
**before** the tag. The tag run packages the chart and builds the three CLI
archives again, and neither is byte-reproducible, so the chart and archive rows
come from the **tag run's** `release.json`, after the tag
([the release checklist](tag1-checklist.md), *Cutting a release candidate*).

| What | Value |
|---|---|
| Candidate commit (the tagged commit) | `56a60ebe509f2449c649fc1899756100d961832f` (the pre-tag record; it differs from the publication commit only under `docs/`) |
| Version tag | `v0.2.0-rc.1` (pushed 2026-10-08 with the owner's standing approval of 2026-10-07) |
| Publication commit (`release.json` `.images.publication`: the `sha-<commit>` images and chart the tag promotes) | `2c277dc11521c337748fbf9059cbaff76c36e82c` |
| CI run (`ci.yml`) for the publication commit, its `publish` job green | [37725467323](https://github.com/VladyslavHaina/logweir/actions/runs/37725467323): `check`, `e2e` and `publish` (build amd64, build arm64, promote) green |
| Release dry run (`release.yml` dispatched on the candidate) | [37730468445](https://github.com/VladyslavHaina/logweir/actions/runs/37730468445) on `2c277dc1`, every job green; `release.sh verify` on its `release-assets`: 14 assets verified, `SHA256SUMS` 14/14 |
| Release run (`release.yml` on the tag) and release drill | [37733173995](https://github.com/VladyslavHaina/logweir/actions/runs/37733173995): every job green, including `drill / drill-from-artifact` (job 113167927725), `publish-images` and `github-release`; the GitHub Release is a pre-release |
| Runner image digest (`linux/amd64`) | `docker.io/vladyslavhaina/logweir@sha256:affa8075492614ff0964bf29ca79a0caa61aa2f8aa47d022460f855e92024054` |
| Controller image digest (manifest list; amd64 and arm64) | `docker.io/vladyslavhaina/weirkeeper@sha256:fac312ba31df831e4cf96dd4546266ee47766528752f97a931eb125974dd56c6` |
| Console image digest (`logweir-console`) | `docker.io/vladyslavhaina/logweir-console@sha256:b7402cb143aee14356707ee10fb644e5f5c25a4993ff285315adfccf5b13b0a7` |
| UI image digest (`logweir-ui`; withdrawn in v1, item 75) | `docker.io/vladyslavhaina/logweir-ui@sha256:2a43b5c0a7ec42688076986c96f63d34449060981fc746a9c8e4ac397e305daf` |
| Chart (`logweir-chart` version and package sha256 from the tag run's `release.json` `.chart`; OCI digest from the release's notes, as an anonymous `helm pull` reports it) | `oci://registry-1.docker.io/vladyslavhaina/logweir-chart` `0.2.0-rc.1`, package sha256 `515be23262193931c1c4028654c24a2e2b72a96f9473232724e2cd12e5ce561b`, OCI digest `sha256:d10e9772159da2934fd27e86daa822623e82366021acbbaa09fc21871ca6b727`; an anonymous `helm pull` is byte-identical to the release asset |
| CLI archives (three; the tag run's `release.json` `.archives`, each with its sha256 and run-time needs) | `logweir-x86_64-unknown-linux-gnu.tar.xz` `6b45d0787a0cae4edd952c50b2218eeb5730148a498a7323bd799ee18d3ce02d` (glibc 2.34+; libssl3, libsasl2-2, zlib1g); `logweir-aarch64-unknown-linux-gnu.tar.xz` `32e07061a67f1b29554d5a336118fcf3bfee97f0d635af9a0de8fbf273e70839` (the same); `logweir-aarch64-apple-darwin.tar.xz` `a2ddd05df432c0b7fcb09f749ee8f4b75297dcc802a6679fb0741c9990792a66` (macOS 11+, Homebrew `openssl@3`) |
| `ui/` bundle, file by file | the output of the command below, which the release asset `ui-files.sha256` also carries |
| Kubernetes exercises run on this candidate (context, auth mode, storage, limits) | — |
| Checks deliberately deferred, each with its reason | — |

**The `ui/` bundle is listed by digest** because the page runs with the
viewer's authority (README, *Threat model*): the bytes a browser executed must
be comparable with the bytes that were released. List exactly the files the
images ship, the selection `scripts/check-image-api.sh` check 1 pins:

```bash
find ui -type f ! -name '*.md' ! -path 'ui/tests/*' | LC_ALL=C sort | xargs shasum -a 256
```

### Supported behaviour in this release

- **One supported path** for a new installation: the Helm chart with the
  managed installation identity, saved connections and destinations, schedules,
  the product console (`logweir-api`, `localAdmin` or `shared`), restore of a
  chosen point with an approval, verification, and a disaster restore from a
  connected archive with no `Backup` objects at all
  ([quickstart.md](quickstart.md)). The static page behind `kubectl proxy`
  remained a legacy view in this release (withdrawn in v1, item 75).
- **A versioned PoC install profile**, [deploy/poc/](../deploy/poc/README.md):
  Traefik, cert-manager with a local CA, Dex and the shared console, all by
  Helm from the published OCI chart and its `sha-` images, with no post-install
  patch. Installed at `86a554e6`, upgraded from `v0.1.5` and from
  `sha-f49849d…`, rolled back, and upgraded in place five times, the last to
  `fdb48cd8` on 2026-09-25.
- **The chart is published** as `oci://registry-1.docker.io/vladyslavhaina/logweir-chart`,
  beside the images and versioned with them ([install.md](install.md), *(c) The
  Helm chart*); each publication pulls anonymously and names its own commit's
  images.
  **On first publication, `vladyslavhaina/logweir-chart` must be Public in Docker Hub**, or `main` CI's
  chart step fails closed (its anonymous pull-back is refused) until the repository is made Public and
  the job is re-run.
- **Fourteen kinds** on `logweir.dev/v1alpha1`, all additive over `v0.1.5`
  ([install.md](install.md), *Upgrade CRDs before upgrading the controller*).
- **Trust has a lifecycle.** A `TrustPolicy` governs a namespace's keys with
  retirement and revocation; `TrustRoster/default` is the deprecated fallback
  and still works unmigrated ([keys.md](keys.md)).
- **Approval policy is per namespace.** Unbound namespaces keep
  `legacy-governed-v1` (an out-of-band signed approval); a namespace may be bound
  to `Ordinary` (the console's own confirmation) or `Governed` (the console's
  confirmation plus an independent approver) ([kubernetes.md](kubernetes.md)
  §8, *Approval policy*).
- **Retention can delete, and only when an administrator says so**: see
  *Retention authority* below.

### Required operator actions, in upgrade order

Do these before rolling the controller, in this order. Each is also listed
under its item below.

1. **Back up the installation identity** (`logweir-signing-key` and
   `logweir-signing-trust`) and export the trust material
   (`logweir trust export`): [install.md](install.md), *Back up and recover
   the installation identity*.
2. **Grant every retention delete credential `s3:GetObject` on
   `<bucket>/<prefix>/*`** (item 1).
3. **Move every `RetentionPolicy` on a versioned or Object Lock bucket to
   `mode: Report` or `mode: ExternalLifecycle`** (item 2).
4. **Review alert rules that match `NoExitCode`** (items 4 and 26).
5. **Bring a shared-console values file in line** (`controller.watchNamespaces`
   and the proxy CIDR bound) before `helm upgrade`, or the render stops
   (item 6).
6. **Apply the CRDs** (`kubectl apply --server-side --force-conflicts`), **wait
   for all fourteen to be established**, then roll the controller **and the
   runner image together** (items 5 and 13), with no Backup in flight (item 11)
   and no Restore running (item 12), then the console, then any approval-policy
   binding (item 7). On the Helm path these are one release and successive
   `helm upgrade`s: first the image values (controller, runner and console move
   together), then the `approvalPolicy.*` values.

**Pre-upgrade checks.** Each prints nothing when the upgrade changes nothing
there.
- **Which compromise revocations become installation-wide** (item 16). This
  build applies every `KeyCompromise` revocation a `TrustPolicy` records to
  every namespace, and holds that policy on deletion while anything else lists
  the key:

```bash
kubectl --context <ctx> get trustpolicies -o json \
  | jq -r '.items[] | .metadata.name as $p | .spec.keys[]
      | select(.state == "Revoked" and .revocationReason == "KeyCompromise")
      | "\($p)\t\(.keyId)"'
```

  Each line is a policy and a key id that will read as compromised in EVERY
  namespace; every document that key signed turns `Untrusted`. Compare with the
  roster's key ids:

```bash
kubectl --context <ctx> get trustroster default -o json \
  | jq -r '.spec.signingKeys[].keyId, .spec.approverKeys[].keyId'
```

  A key that must not be compromised everywhere cannot be un-revoked (G3, and
  G9 in this build): delete that policy before the upgrade, or re-issue the key.
  Do not deploy until every listed record is one you mean installation-wide.
- **Which `Restore`s and `RehearsalSchedule`s carry a `runnerResources` block**
  (item 21): run item 21's inventory before the controller rolls.
- **Which `RecoveryCatalog`s share a destination** (item 18): run item 18's
  listing; only the oldest survives.
- **Which namespaces refuse Logweir pods, and which connections read
  `reachable` from an older probe** (item 26): run item 26's two reads.
- **Which source topics are `LogAppendTime`** (item 27): run item 27's
  inventory before the runner image rolls, and add `restore.time_basis:
  producerTime` where a restore by the producers' clocks is acceptable.
- **Whether the evidence store honours conditional create** (item 11): turn on
  `writeProbe: CreateOnlyMarker` for one run of the destination check.

**After the upgrade:**
- Re-approve any retention plan digest approved over a shared backup set
  (item 3); it changed.
- Run `logweir catalog sync` once per archive to list `v0.1.5` points in a
  catalog, and re-run any readiness check made before the upgrade (item 14).
- Remove `checks.discovery.defaultMaxTopics` and
  `checks.preflight.defaultTimeoutSeconds` from your values; with
  `--reuse-values`, upgrade once with `--reset-then-reuse-values` (item 23).
- Compare the topics of console Restores created from a resumed draft with the
  ones you meant (item 22).

### The twenty-seven operator-facing changes

Items 1–20 were collected for PLAT-20.2 from merged changes; the defect names
are the platform tracker's. Items 17–20 were found by the PoC rounds and proven
on the running install by the in-place upgrade that carried each. Items 21,
22, 23, 24, 25, 26 and 27 are the product-expansion tracker's fix-now rows
FX-2, FX-5, FX-10, FX-3, FX-13, FX-11 and FX-8. Each item's **Scope** line says
what proves it.

#### 1. Retention needs `s3:GetObject` — required action

**Changed.** The retention worker HEADs every key before deleting it, to refuse
a versioned bucket where a delete by key would only write a delete marker
(OBJECT-LOCK-DELETE-MARKER). **Do:** grant the retention delete credential
`s3:GetObject` on `<bucket>/<prefix>/*` **before** upgrading. Without it the
enforcer deletes nothing and records every point `Kept` with
`VersionProbeRefused`. A policy degraded for that reason re-probes 24 h after
its last run, or at once on any spec edit ([install.md](install.md) §3.11,
[kubernetes.md](kubernetes.md) §7a).
**Scope:** unit and controller tests on `main` `b57753b`; live on lab-refresh-9 (`306cebf`): the grant set 9/9 and `VersionProbeRefused` without the grant.
**Rollback:** the extra grant is harmless to an older worker.

#### 2. `Enforce` refuses versioned and Object Lock buckets

**Changed.** `Enforce` on a versioned or Object Lock bucket deletes nothing and
degrades with `VersionedBucket`. **Records written by earlier builds on such
buckets that say `Deleted` are false**: the data is still there as noncurrent
versions behind delete markers. **Do:** use an unversioned bucket or
`mode: ExternalLifecycle` (a provider rule such as
`NoncurrentVersionExpiration`). Do not change a bucket's versioning while a
retention run is in flight, and do not enforce on a bucket whose versioning was
ever enabled and later suspended: there a deletion removes only the newest copy
while being recorded `Deleted` (accepted residual
RET-VERSIONED-SUSPENDED-REWRITE).
**Scope:** found live by harness-rows-11, fixed on `main` `b57753b`; live on lab-refresh-9: no delete marker written on a versioned or Object Lock bucket.
**Rollback:** set such policies to `mode: Report` **before** rolling back; the
older worker records delete markers as deletions.

#### 3. Re-run receipts over one backup set are protected together

**Changed.** Two receipts can name one backup set (a runner Job re-created from
its frozen inputs signs a second receipt). The older one is protected as
`SharedSegment` until every receipt naming the set is due, and then the set is
removed by one plan line naming the others in `co_point_ids`
(SHARED-SET-RETENTION). **Do:** re-approve any plan digest approved over such a
plan; it changed. Plans with no shared set are byte-identical. A shared set
with more receipts than `maxDeletionsPerRun` is never selected; raise the
ceiling to let it go.
**Scope:** found live by harness-rows-11 (it was data loss), fixed on `main` `b57753b`; live on lab-refresh-9: two receipts over one set both kept `SharedSegment`.
**Rollback:** set every `RetentionPolicy` to `mode: Report` **before** rolling
back: an older controller plans without this protection again. An older worker
refuses a plan carrying `co_point_ids` (exit 3) and deletes nothing.

#### 4. Runs that end without an exit code are named

**Changed.** A run whose pod never started may now end
`Failed/VolumeMountFailed`, `Failed/PodUnschedulable` or
`Failed/RunnerImageUnavailable` instead of `NoExitCode`, when the matching
diagnostic was still observed as the Job ended
(WARNING-DIAGNOSTICS-NOEXITCODE; [kubernetes.md](kubernetes.md) §10).
`exitCode` stays absent. **Do:** review alert rules that match `NoExitCode`.
**Scope:** controller tests on `main` `b57753b` and `c892650`; live on lab-refresh-9: two mount failures and an unschedulable pod named.
**Rollback:** an older controller writes `NoExitCode` again.

#### 5. Disaster restore upgrades the controller and runner together (PLAT-15.2)

**Changed.** A restore bound to a catalog point verifies the point's receipt
signature in the runner before any data moves, against an evidence keyring the
controller renders into the approval bundle. **Do:** upgrade the controller and
runner images **together**. A standalone `logweir restore run` of a point-bound
plan needs `--evidence-keys`. A point-bound `Restore` whose bundle an older
controller created ends `ApprovalBundleConflict`: delete it and create it again.
**Scope:** on `main` since `ac00819` ([kubernetes.md](kubernetes.md) §7d.1); live on lab-refresh-9 (a restore after the loss of every custom resource; an untrusted signer refused `CatalogPointSignerUntrusted` and `PointUntrusted`) and on the PoC.
**Rollback:** an older runner refuses `--evidence-keys` and exits 1 before any
work; a runner at this version refuses a point-bound plan from an older
controller (`PointUntrusted`). Archives and catalog records are untouched.

#### 6. Shared console values must name the controller's namespaces (PLAT-17.2)

**Changed.** A values file with `api.console.mode: shared` stops rendering until
`controller.watchNamespaces` is set, excludes the release namespace, and lists
every `roles.bindings` namespace; `trustedProxyCidrs` wider than `/16` (IPv4)
or `/48` (IPv6) is refused when `requireTrustedProxy` is on. The refused
`helm upgrade` changes nothing in the cluster. **Do:** follow
[install.md](install.md) §5e's migration list, then upgrade once.
**Scope:** chart lint and render tests; shared mode ran behind Traefik, cert-manager and Dex on docker-desktop (the PoC profile); a corporate IdP bound by group has not.
**Rollback:** reinstall the previous chart version with the previous values;
the scoping objects go and the cluster-wide binding returns.

#### 7. Approval policy: Ordinary and Governed (PLAT-19.2)

**Changed.** `Ordinary` confirmation requires `allowOrdinaryConfirmation: true`
plus an explicit namespace binding; `Governed` requires a change ticket. The
policy reference a document signs is `{name, digest}`, so any edit to a policy
is a different policy. **Do:** keys first (a `ConsoleConfirmation` key and, for
`Governed`, each approver's `GovernedApproval` key with `principal.id`), then
the binding ([install.md](install.md) §5f). A `Restore` submitted during a
policy rollout may need resubmitting once the rollout completes.
**Scope:** controller, runner and API tests; live on lab-refresh-9 (self-approval refused `403`, an unbound namespace kept `legacy-governed-v1`) and on the PoC.
**Rollback:** unbinding returns the namespace to `legacy-governed-v1`;
not-yet-admitted v2 approvals are then refused, never admitted as v1. An older
controller refuses every v2 document as `PayloadTypeMismatch`.

#### 8. Restore completion is written only from a valid scorecard

**Changed.** `Restore.status.completion` appears only once the scorecard's
verification is `Valid` (RESTORE-COMPLETION-UNWRITTEN). Its `recordsRestored`
is the count read back from the target **in the sampled window**
(`sample.records_restored`), never the total the restore wrote. **Do:**
nothing; read absence as "not yet verified", never as zero.
**Scope:** `main` `fa3384e`; seen live on the PoC install.
**Rollback:** an older controller writes no `completion` at all.

#### 9. A schedule never fires a slot due before it was created

**Changed.** A slot whose due time is before the schedule's
`metadata.creationTimestamp` never fires (SCHEDULE-FIRES-SLOT-BEFORE-CREATION).
Use *Run first backup now* for an immediate first run. **Do:** nothing;
deleting and recreating a schedule (a GitOps prune and re-apply) resets the
bound.
**Scope:** `main` `fa3384e`; live on lab-refresh-9 and on the PoC at `86a554e6`.
**Rollback:** an older controller may fire that slot once.

#### 10. The API's `trust.state` never calls a revoked-key observation green

**Changed.** `trust.state` for `result: Valid` beside
`trust.basis: RecordedBeforeRevocation`, `None`, an absent basis inside a
`trust` block, or an unknown basis word is now `untrusted`, where it read
`verified`; `Valid` beside `Unverified` is `notAttempted`
(TRUST-STATE-RBR-VERIFIED). `Current` stays `verified` and `Historical` stays
`verifiedHistorical`. **Do:** a client that must also read older servers checks
`trust.basis` as well ([api.md](api.md)).
**Scope:** `main` `178cc1c`, Tier-A review.
**Rollback:** an older API server says `verified` again for that pairing.

#### 11. One backup id is one engine run; the evidence store must enforce conditional create

**Changed.** A Backup Job lost and re-created from its frozen inputs used to run
the engine again under the same execution id and rewrite the manifest the
first run's receipt attests (RECEIPT-DUP). `logweir backup run` now claims its
execution with a create-only
`logweir/backups/<backupId>/execution.claim.json` before the engine starts. A
second run of one execution exits 1 with `status.exitReason:
ExecutionAlreadyClaimed` and writes nothing; a schedule retries it under a new
execution id only with `spec.retry`. An evidence store that does not enforce
`If-None-Match: *` makes every backup exit 4 `ExecutionClaimUnproven` before
any data is written; a destination with `writeProbe` on reports it
`notReady / ConditionalCreateUnsupported` first. FX-7 adds a set check (a run
that finds an older run's manifest or segments under `<prefix>/<backup_id>/`
stops, exit 1 `ExecutionAlreadyClaimed`; a transient read failure is exit 1, a
403 or a wrong bucket exit 4 `ExecutionClaimUnproven`) and, on a versioned
bucket, the manifest's pinned version in the receipt
(`archive.manifest_version_id`, receipt format `1.2.0`): a set written again
after the point was signed is refused by a point-bound restore
(`PointBindingMismatch`) and reported `Conflict` by the catalog. A
byte-for-byte copy of the archive (`aws s3 sync`, `mc mirror`) is checked by its
manifest digest and logs `PointPinUnchecked`. The pin is checked only where the
bucket still serves the pinned version by id; when the signing bucket's catalog
says `Conflict` and a copy's says `Available`, believe the `Conflict`
([backup-receipt.md](formats/backup-receipt.md#the-pinned-manifest-version-versioned-buckets),
[stability.md](stability.md#the-first-post-tag-addition-format-110-fx-4)).
**Do:** confirm the evidence store honours conditional create: turn on `writeProbe: CreateOnlyMarker` for one run of the destination
check, and never set `AWS_CONDITIONAL_PUT=disabled` ([support-matrix.md](support-matrix.md)). A standalone `logweir backup run` that reused a fixed
`backup_id` must pass a fresh `--backup-id-override` per run. Grant
`s3:GetObjectVersion` on the archive prefix before the upgrade. Only an older
runner still RUNNING when its Job is re-created, with nothing written yet,
escapes both checks: let such a Job finish before upgrading.
**Scope:** in-process rows, a private MinIO container and mutants; live on lab-refresh-10 (PLAT-06.1 case e, the receipt-dup rows) and FX-7's compose rows on MinIO and SeaweedFS.
**Rollback:** an older runner ignores the claims, the set check and the pin;
the claims stay in the bucket, harmless. **Before rolling the runner back to a
build without the execution claim, let in-flight `Backup`s finish:** on an
unversioned bucket a re-created Job's older runner rewrites the set's segments
under an unchanged manifest, and no check reports it. On a versioned bucket that
point is reported `Conflict`.

#### 12. A failed restore's signed scorecard: roll the controller out before the runner

**Changed.** A restore runner at this version names its signed failure at exit
2 (the scorecard of a run whose data did not reconcile is published and
verified `Valid`), and this controller writes `Restore.status.completion` only
for a run that PASSED (`exitCode 0`, `outcome: pass`, a green verdict). An
**older controller** gates completion on the verdict alone, so it would write a
completion panel over a restore that FAILED. **Do:** roll the controller out before (or with) the
runner, never the runner first, and roll the runner back before the controller;
let running `Restore`s finish before either. On the Helm path both move in one
`helm upgrade`, which is safe once nothing is running.
**Scope:** `claude/rehearsal-fix` (`6b2704d`, Tier-A review), [stability.md](stability.md), *Mixed versions*; live on lab-refresh-10 (CONSOLE-COMPLETION-ON-FAILED-RESTORE).
**Rollback:** runner first, then controller; nothing stored needs converting.

#### 13. Readiness rows are answered by the principal they name; an older runner says "upgrade"

**Changed.** A destination readiness check answers `evidenceWritable` with the
`evidenceWrite` grant and `evidenceReadable` with the `evidenceRead` grant when
they differ from the checked one
(PREFLIGHT-EVIDENCEWRITABLE-WRONG-PRINCIPAL), and a `DestinationAccess` check on
a `writeProbe: CreateOnlyMarker` destination writes its one create-only marker
under `logweir/readiness/` (DESTINATIONACCESS-IGNORES-WRITEPROBE). **Do:**
upgrade the runner image **with** the controller: an older runner refuses such
a plan at startup (`Preflight` `Failed` / `CheckContractMismatch`, saying the
runner is older than the controller), and nothing is written as the wrong
principal. Set `writeProbe: Disabled` on a destination that must never receive
the marker.
**Scope:** `claude/readiness-principal` (merged `540e3ea`), closed live at lab-refresh-10 (rows RP-L1…L15); [kubernetes.md](kubernetes.md) §21.
**Rollback:** an older controller renders the old plans again, which a newer
runner still accepts.

#### 14. A recovery point with no saved destination is checked, verified and completed

**Changed.** Three behaviours for a point written without a `BackupDestination`
(every point `v0.1.5` wrote), found by the PoC upgrade round (P3, P5, P6).
**Readiness:** a restore readiness check over such a point
(`legacySourceArchive`) reads the archive with the restore Job's own principal
(the Backup's Secret, keys `access-key-id` / `secret-access-key`) at the
approved plan's location, instead of ending `Failed/ArchiveUrlUnreadable`; no
`secretRef`, a non-`s3://` archive or an unreachable location is `notReady` and
named. The verdict is bound to the archive Secret: editing it after a green
check refuses the Create. **Evidence:** the console writes such a restore's
evidence to the archive's own bucket, and the controller reads an
inline-archive run's evidence only in the bucket of its archive handle
(`LOGWEIR_ARCHIVE_URL`): evidence elsewhere is `NotAttempted`, naming its own
evidence bucket, never read in the wrong one. **Catalog:** a `Full` or `Index`
sync reads catalog records only, so a pre-catalog point is not in a connected
catalog until its record is backfilled ([kubernetes.md](kubernetes.md) §7d,
§15.1a, §21.8). **Do:** if your legacy schedules write to a bucket other than
`LOGWEIR_ARCHIVE_URL`'s (chart `archive.url`), their runs now read
`NotAttempted`: verify them with the printed commands or move them to a
`BackupDestination` (with the bundled MinIO the handle is
`s3://kafka-backups/<release>`). Write a hand-written legacy restore plan's
`evidence:` to
the handle's bucket; run `logweir catalog sync` once per archive to list
`v0.1.5` points in a catalog; re-run any readiness check made before the
upgrade. The controller's handle needs its read credential
(`logweir-evidence-ro`, [install.md](install.md) §3).
**Scope:** preflight, restore, backup and console rows; live on the PoC upgrades to `02dc44b6` and `b748fd5f` (a `v0.1.5`-shaped inline-archive point checked, restored `Valid` 150/150, and verified once the read credential existed).
[UNVERIFIED — a point written by the v0.1.5 runner itself was not available on the upgraded install to run this path.]
**Rollback:** an older controller answers a legacy restore readiness check
`Failed/ArchiveUrlUnreadable` again and reads a legacy restore's evidence in
the handle's bucket whatever the plan names.

#### 15. Manual runs may queue; "Back up now" is rate limited (P10)

**Changed.** Nothing used to bound manual runs: on the PoC a hundred accepted
`POST …/backups` became a hundred runner pods and took the node `NotReady`. Now:
- **The controller bounds manual runs per namespace.** At most
  `runs.maxManualBackupsActivePerNamespace` (default `4`) manual `Backup`s and
  `runs.maxManualRestoresActivePerNamespace` (default `2`) admitted manual
  `Restore`s hold a runner slot at once. The rest wait with `phase: Queued`,
  `Admitted=False / ConcurrencyLimited` and `status.queue.limit`, **with nothing
  created**, and start in arrival order as slots free (within 15 s). Scheduled,
  catch-up and retry `Backup`s are neither counted nor queued;
  `concurrencyPolicy` is unchanged.
- **A queued restore keeps its approval's deadline**
  (`status.queue.authorizationExpiresAt`); one still queued when it passes is
  refused `AuthorizationExpired` "while queued behind N".
- **The console limits how fast one person can start runs:** `10` "Back up
  now" and `5` manual restores per person (`issuer#subject`), per namespace, per
  minute (`api.console.rateLimits.*`), then `429 rate_limited` with
  `Retry-After`. The window is per console process.
- **Known limit:** a subject allowed to create `Backup` objects directly can
  declare a scheduled kind and escape the pool; RBAC on `create backups`
  governs that path.
**Do:** nothing is required. An automation that starts more than the limits
must pace itself or read `429` and `Retry-After`, and one that expects
`Running` right after `201` must also accept `Queued`. Raise `runs.*` where
every namespace's nodes can carry more runner pods.
**Scope:** pool, route-table and chart rows with twelve mutants; live on the PoC upgrade to `02dc44b6` (twenty manual runs held at four pods through a controller restart; the per-person `429`).
**Rollback:** roll the **console** and the **controller** back **with the
chart** (`helm rollback`). A default install renders neither new block, so an
image-only rollback of it keeps working; with a non-default `runs.*` or
`api.console.rateLimits.*` value an older controller refuses the whole
`weirkeeper-policy` ConfigMap and fails closed, and an older console refuses its
configuration and does not start. An older controller starts every queued run
at once; an older console shows a queued run as `unknown`
(`UnrecognizedPhase`).

#### 16. A compromise revocation outlives its `TrustPolicy`; deleting one may wait

**Changed.** On the PoC a `TrustPolicy` revoked the installation signer for
`KeyCompromise`, and deleting that policy sent the namespace back to
`legacy-roster-v1`, whose roster still listed the key, so every backup it
signed verified `Valid` again (TRUSTPOLICY-DELETE-DROPS-REVOCATION). A
compromise is now a fact about the key:
- **Every namespace.** A key ANY `TrustPolicy` records as `Revoked` /
  `KeyCompromise` is revoked for compromise wherever it is listed, by evidence
  verification, the catalog view, the runner's keyring, approvals,
  `signer.rostered`, the API's `trust.state` and the keys view. The recording
  policy reads `CompromiseGuard=CompromiseRecorded`; another policy that lists
  the key `Active` reads it `Revoked`/`CompromiseInherited`. A `Superseded`
  revocation is not carried.
- **Deleting a recording policy is held** by the finalizer
  `logweir.dev/compromise-revocation` until another live policy records the same
  revocation, or nothing lists the key; until then it stays `Terminating` with
  `CompromiseGuard=True/DeletionBlocked` naming what still lists the key.
- **CEL rule G9:** a revoked key's `KeyCompromise` reason can no longer be
  edited to `Superseded` or removed.
- **The controller holds `patch` on `trustpolicies`** for that finalizer only.
**Do:** before this upgrade, run the pre-upgrade check above and confirm every
`KeyCompromise` record it prints is one you mean installation-wide. After the
upgrade, replace a policy that records a compromise by applying a successor
under a new name first ([keys.md](keys.md), *Replacing a `TrustPolicy`
safely*); the delete-and-re-create repair for a mistaken `notBefore` still works
for a policy that records no compromise. Wait until a newly revoked policy lists
the finalizer before deleting it.
**Scope:** one row per consumer (`crates/weirkeeper/tests/trust_revocation_consumers.rs`), the guard table and finalizer rows, G9, API and console rows with mutants; live on the PoC upgrade to `b748fd5f` with a minted key.
**Rollback:** an older controller never removes the finalizer, so a deletion
made meanwhile is held until this build returns; an older controller applies a
compromise only in the recording policy's own namespaces, and `v0.1.5` trusts
every key the roster lists. Before rolling back, re-create
`TrustRoster/default` without every compromise-revoked key and record each
compromise on every policy that still lists the key (rollback step 9 below).

#### 17. An `Approval` its Restore was admitted under is kept as a record (P9)

**Changed.** 900 s after an Ordinary confirmation the controller rewrote a
succeeded Restore's `Approval` to `Verified=False/AuthorizationExpired`, and
again every pass. Expiry, the policy binding and the key windows now bound only
the time to admission. Once the Restore it names is `Admitted=True` and its
approval bundle names this `Approval`'s UID, the controller adds
`Consumed=True` (reason `RestoreAdmitted`, at the admission instant) and keeps
`Verified=True` and the recorded authorization. A compromise revocation of the
recorded key still withdraws the green (`RecordedBeforeRevocation` or
`KeyRevoked`); an `Approval` deleted and re-created is
never `Consumed` ([kubernetes.md](kubernetes.md) §8). **Do:** nothing is
required. Drop any workaround that deleted `Approval`s after their restore. On
the upgrade an `Approval` withdrawn this way is re-checked at the admission
instant.
**Scope:** `approval_policy.rs` and `approval_controller.rs` with mutants; live on the PoC upgrade to `02dc44b6`.
**Rollback:** an older controller ignores `Consumed` and judges consumed
`Approval`s again (on `86a554e6`, the loop above).

#### 18. One catalog per destination: a second `RecoveryCatalog` over it is refused (P11)

**Changed.** Several `RecoveryCatalog`s whose `spec.destinationRef` names one
`BackupDestination` in one namespace were each accepted. The one created first
now catalogs the destination; every later one reports
`Ready=False/DuplicateCatalog` and `Synced=False/DuplicateCatalog`, naming the
holder, runs no sync Job and withdraws its view, so a `ProtectionPolicy` over
it reads `CatalogStale`. If the elder is deleted, the next in creation order
takes over within a minute ([kubernetes.md](kubernetes.md) §7d). **Do:** before
upgrading, list the catalogs per destination:

```bash
kubectl --context <ctx> get recoverycatalogs -A -o json \
  | jq -r '.items[] | select(.spec.destinationRef != null)
      | "\(.metadata.namespace)\t\(.spec.destinationRef.name)\t\(.metadata.creationTimestamp)\t\(.metadata.name)"' \
  | sort
```

Lines with the same namespace and destination are duplicates; the oldest
survives, not the best. If a newer duplicate has the settings you want, delete
the older one first. A `RetentionPolicy.catalogRef` or a
`ProtectionPolicy.protects.catalogRef` that names a duplicate fails closed:
point it at the survivor (a `RetentionPolicy`'s is immutable, so re-create it).
**Scope:** `recovery_catalog_controller.rs` and `catalog_controller.rs` with mutants; live on the PoC upgrade to `b748fd5f`.
**Rollback:** an older controller syncs the duplicates again.

#### 19. A failed controller evidence read is read again (P12)

**Changed.** A failed controller read of a run's evidence (an inline-archive
run, or a destination whose `evidenceRead` is `ControllerIdentity`) used to be
final. Now a transient failure (a denial, a missing credential, a timeout) is
read again three more times, 1, 5 and 15 minutes apart, recorded in
`status.evidence.observation` with its `retryAfter`; each new controller process
reads every eligible unverified run once more. A store `NotFound` is final, and
a `Backup` is never `Valid` without its `windowCovered`
([kubernetes.md](kubernetes.md) §15.1b). **Do:** a created or rotated
`logweir-evidence-ro` takes effect only when the controller restarts. A run
recorded absent through a misconfigured handle is not read again; check it with
its printed `logweir drill verify` command.
**Scope:** `backup_controller.rs`, `restore_controller.rs` and `verification.rs` with mutants; live on the PoC upgrade to `b748fd5f`.
**Rollback:** an older controller never reads a failed run again; a verdict
this build reached stays.

#### 20. A readiness replay names its own expiry, and the console asks again (P14)

**Changed.** One idempotency key names one `Preflight` for ever, so a replayed
check could be expired. The API's replay, cancel and operation projections of
a finished check now carry `expired` in `staleReasons` once `expiresAt` has
passed, with `staleBasis: ["expiry"]`: the same object, `200` ([api.md](api.md),
*A readiness key replays only while its check can still be the answer*). The
console renews its key once the check is spent. **Do:** a client that builds
its own readiness keys sends a new key once a replay reads `expired`.
**Scope:** `crates/logweir-api/tests/preflights.rs` and `ui/tests/check-intent.spec.js` with mutants; live on the PoC at `a54fb823`.
**Rollback:** `helm rollback` moves the API and the console together, and the
older pair replays a spent check again.

#### 21. A `Restore`'s `runnerResources` is applied, or the object is refused (FX-2)

**Changed.** `Restore.spec.runnerResources` (and a `RehearsalSchedule`'s
`spec.bounds.runnerResources`, withdrawn by item 73) was accepted and dropped,
so every runner pod was `BestEffort`. The block now reaches the `runner`
container exactly as written. A block the controller will not apply is refused,
never clamped: a quantity outside the schema's grammar, memory that is not
whole bytes, CPU finer than `1m`, anything above 4 CPUs or `8Gi`, a zero limit,
a memory limit below `32Mi`, or a request above its limit. Such a `Restore` ends
`Failed`/`ExecutionSpecInvalid` before its approval is read, and the product
API serves it `refused` ([kubernetes.md](kubernetes.md) §12, *The runner's
requests and limits*). On the upgrade a `Restore` with no Job yet is judged on
its next pass: a valid block can now meet a `ResourceQuota` or `LimitRange`
(`RunnerReady=False`, then `PodCreationForbidden`) or an OOM kill; a `Restore`
whose Job exists keeps it.
**Do:** before the upgrade, list every object that carries a block:

```bash
kubectl --context <ctx> get restores,rehearsalschedules -A -o json \
  | jq -r '.items[] | select(.spec.runnerResources // .spec.bounds.runnerResources)
      | "\(.kind) \(.metadata.namespace)/\(.metadata.name)"'
```

No output means nothing changes. Check each block against §12's rules and its
namespace's `ResourceQuota` and `LimitRange`. Expect an out-of-bounds `Restore`
with no Job yet to end `ExecutionSpecInvalid`; `spec` is immutable, so the
remedy is a new `Restore`.
**Scope:** `crates/weirkeeper/tests/runner_resources.rs`, `restore_controller.rs` and `status_mapping.rs` with mutants; the CRD, refusal and Job-bytes rows are the PoC upgrade's.
**Rollback:** an older controller ignores the field again; a `Restore` this
build refused stays `Failed`. A `RehearsalSchedule` this build skipped fires
again uncapped under the older controller: suspend it (`spec.suspend: true`, the
one mutable field) first if it must not run.

#### 22. A console restore asks for a replication factor it can explain, and keeps its topic subset (FX-5)

**Changed.** The restore wizard wrote `replicationFactor: 1` into every plan.
Step 4 now has a **replication factor** input. Its default is the target
connection's broker count, at most 3, from that connection's newest successful
topic discovery while the controller keeps it
(`checks.discovery.retentionSeconds`, a day by default); with none, the default
stays 1, and step 4 says why and links *Discover topics* on the target. Step 4
and the review print the factor with its basis, and say a topic the source kept
at replication factor 1, restored at 3, takes three times the storage it took there. A
factor above a fresh discovery's count is refused before anything is sent
(`ReplicationFactorExceedsBrokers`). The topic-discovery DTO gains an optional
`brokerCount` ([api.md](api.md)). Also fixed: **a resumed restore draft lost
its topic subset**, so a Restore created from one may have restored more topics
than were chosen.
What an operator sees after the console image is upgraded:
- **No discovery of the target in the last day:** the factor is still 1, with a
  warning and a link to *Discover topics*.
- **After a *Discover topics* of the target:** a restore into a multi-broker
  target asks for 2 or 3 replicas, so it stores up to three times as much.
- **A `Restore` created before the upgrade** keeps its plan bytes and its
  factor of 1.
**Do:** before restoring into a target with little free disk, check the factor
on step 4. Run the readiness check (step 5) before Create: without it a factor
the brokers cannot hold fails the run when it creates the topics (`exitCode 1`,
`operational`; [quickstart.md](quickstart.md) §7). For console Restores created
from a resumed draft before this build, compare the `source.topics` list of
each plan with the topics you meant
(`kubectl --context <ctx> -n <ns> get restore <name> -o jsonpath='{.spec.planBytes}'`).
**Scope:** `ui/tests/replication-factor.spec.js` with a negative control per behaviour, `crates/logweir-api/tests/topic_discoveries.rs`, mutants, and a Chromium journey; the PoC rows are the upgrade's.
**Rollback:** rolling the console image back restores the fixed, read-only 1
and the draft that drops the subset; a `Restore` created with a larger factor
keeps it.

#### 23. Two policy values that changed nothing are withdrawn (FX-10)

**Changed.** `checks.discovery.defaultMaxTopics` and
`checks.preflight.defaultTimeoutSeconds` never reached anything: both CRDs
default the request field (`maxTopics` 20 000, `timeoutSeconds` 120). They are
gone from `values.yaml`; the controller's policy parser accepts both keys or
neither and reads neither; the chart renders both at fixed values (20 000, or
`hardMaxTopics` if lower, and 120) so an older controller can still read the
document. **Also:** `logweir-retention` refuses a run whose
`LOGWEIR_RETENTION_MAX_DELETIONS` or `LOGWEIR_RETENTION_MAX_OBJECTS` is absent
or not a whole number of at least 1 (exit 3, nothing deleted); every controller
sets both from `spec.enforcement`.
**Do:** remove either key from your values file. A `helm upgrade` that still
carries one succeeds and prints `WITHDRAWN VALUES ARE SET AND IGNORED`; with
`--reuse-values`, upgrade once with `--reset-then-reuse-values`. To bound a
discovery, name `maxTopics` on it (`checks.discovery.hardMaxTopics` still caps
it); to give a slow cluster longer, name `timeoutSeconds` (30–600) on the
`Preflight`. An install that had set either key to another value gets a changed
policy digest once, and every retained `ready` `Preflight` reads `unknown`
(`policyChanged`): run the check again.
**Scope:** `chart_policy.rs`, `chart_lint.rs`, `scripts/check-chart.sh`, `scripts/check-chart-values.sh`, the retention worker's cap rows and the mutants in the FX-10 report; the PoC upgrade checks its policy document and digest are unchanged.
**Rollback:** `helm rollback` restores the previous chart's values; rolling back
only the controller image is also safe.

#### 24. A `newTopic` restore's scorecard names the source settings it did not reconstruct (FX-3)

**Changed.** A restore creates its target topics at the plan's replication
factor, with `retention.ms=-1` and the target broker's `cleanup.policy`, and
the scorecard labelled those deviations `intentionally_deviated` in every mode,
wrong for a `newTopic` restore. Scorecard format `1.2.0` names them in the new
`topic_parity.not_reconstructed` and in `unexpected_divergence`, and
`logweir drill verify`, `docs/verify_scorecard.py` 1.17.0 and `logweir drill
show` say so. No exit code or `outcome` changes. A 1.2.0 scorecard whose lists
contradict `not_reconstructed` is refused by both readers (`drill verify` exit
4, the script exit 1).
**Do:** nothing on the upgrade. After a `newTopic` restore, read
`not_reconstructed` and apply the source's settings once the restore is verified
([stability.md](stability.md#a-newtopic-restore-does-not-reconstruct-the-sources-topic-settings)).
Re-read any `newTopic` scorecard an earlier build signed with the current
verifier. Automation that parses `intentionally_deviated` or
`unexpected_divergence` should expect these entries in the second list.
**Scope:** unit and phase rows, the five arms in both readers with the corpus and parity gate, and a live compose row (`e2e/tests/new_topic_parity.rs`) on `cluster3`.
**Rollback:** an older runner writes 1.1.0 scorecards with the old labels
again; 1.2.0 scorecards stay valid.

#### 25. The sign-in limit counts each client behind the trusted ingress, not the ingress (FX-13)

**Changed.** `/auth/login` and `/auth/callback` allow 20 requests a minute,
counted per socket peer, so behind the ingress every user shared one budget.
The count is now kept per **client**: when the peer is a trusted proxy
(`api.console.trustedProxyService` or `trustedProxyCidrs`), the client is the
rightmost `X-Forwarded-For` hop that is not itself a trusted proxy; otherwise
the peer. `Forwarded` is never read; an IPv6 client is counted by its `/64`.
There is no budget over all clients; past 65,536 live windows a new client is
served untracked. The forwarded address chooses a counter only, never an
identity ([api.md](api.md), *Rate limits*). New audit notes: `loginRateKey`
(`forwardedClient` or `peer`) and, when the table is full,
`loginRateUntracked: tableFull`. With no trusted proxy configured nothing
changes. If kube-proxy or a load balancer replaces client addresses before the
ingress (`externalTrafficPolicy: Cluster` on a multi-node cluster, a load
balancer in instance mode), clients through one node still share its budget:
keep the address with `externalTrafficPolicy: Local`, the PROXY protocol, or an
L7 hop trusted at both the ingress and the console.
**Do:** if sign-ins are refused `429` behind the ingress, check that the
ingress is a trusted proxy (`loginRateKey: peer` on a request through it says
it is not). Keep the ingress from passing a client's own `X-Forwarded-For`
through (Traefik's `forwardedHeaders.trustedIPs: []` with `insecure: false`).
Keep `trustedProxyCidrs` no wider than the ingress's pods. Set the identity
provider's per-client quota generously, alert on `loginRateUntracked` and on
`a sign-in was refused` naming `the provider answered HTTP 429`, and keep
`kubectl port-forward` to the in-cluster administrator mode as the break-glass
path.
**Scope:** `login_rate_key` and rate-limiter unit rows and ten router rows in `crates/logweir-api/tests/entry_point.rs`, with mutants; two clients through Traefik are the PoC upgrade's.
**Rollback:** an older console keys the limit on the socket peer again and
writes neither audit note; the counters are in memory.

#### 26. A Job pod the namespace refuses is reported at once, in the admission's words, by every kind (FX-11)

**Changed.** A pod a `ResourceQuota`, a `LimitRange`, an admission webhook or a
missing ServiceAccount refuses leaves only the Job's `FailedCreate` event. Every
Job-owning controller now reads it: once the Job has had no pod for 30 seconds
and the event says why, the Job is cancelled and the object names the refusal in
the admission's words, `PodCreateRejected` for the check kinds and
`PodCreationForbidden` for the runner kinds ([kubernetes.md](kubernetes.md)
§12). On the upgrade: a `KafkaCluster` can carry `status.reason:
PodCreationForbidden` where it carried `NoExitCode`; a `RetentionPolicy`
`Enforced=False/PodCreationForbidden` where it carried `RunFailed`; a dynamic
`Backup` whose discovery pod is refused ends `PodCreationForbidden` where it
ended `DiscoveryFailed`, and a schedule does not retry it. A probe that produced
no verdict now clears `reachable` (keeping `clusterId`) and is probed again on
the ordinary cadence, so a `Restore` against a cluster whose newest probe could
not vouch for it is refused `ClusterNotReachable` until the next good probe. A
`mode: ExternalLifecycle` policy clears an earlier `lastEvaluation`. A refused
retention run counts toward `EnforcementDegraded`, so three in a row stop
scheduling. The cost is one `list` of `events` per pass of a pod-less Job.
**Do:** see which namespaces refuse Logweir pods today:

```bash
kubectl --context <ctx> get events -A --field-selector reason=FailedCreate \
  -o custom-columns=NS:.metadata.namespace,JOB:.involvedObject.name,WHY:.message
```

Before the upgrade, run `kubectl --context <ctx> get kafkaclusters -A` and look
for a `REACHABLE` value beside a reason other than `Reachable` or
`ProbeReportedUnreachable`: each is cleared and re-probed. Give each namespace a
`LimitRange` default, or room in its `ResourceQuota`, for Jobs that state no
resources. Add `PodCreationForbidden` wherever an alert or dashboard matches
`NoExitCode`, `DiscoveryFailed` or `RunFailed` on these kinds.
**Scope:** one controller row per kind with a negative control, and `check_framework.rs` pinning both grace boundaries, with mutants; the PoC rows are the upgrade's.
**Rollback:** an older controller reads no events in these kinds again; an
object this build ended keeps its `PodCreateRejected` or
`PodCreationForbidden`.

#### 27. A point-in-time restore of a `LogAppendTime` topic is refused unless its plan selects by producer time (FX-8)

**Changed.** The archive holds each record's PRODUCER timestamp, so a
point-in-time restore of a topic on `message.timestamp.type=LogAppendTime`
selected records by the producers' clocks. The runner now refuses a plan that
selects such a topic by time (it states `restore.point_in_time`, or its
`sample.window_end` cuts the archive) with exit 3 and
`refusal-reason=PointInTimeByProducerTime`, before any target topic is created.
The topic counts as `LogAppendTime` when the manifest records that override, or
when the bound, verified receipt recorded it as the effective value. A plan
that states `restore.time_basis: producerTime`, approved with it, runs, and its
scorecard (format **1.3.0**) lists the topic under
`source.time_basis.producer_time`; a topic whose type nothing recorded is listed
under `source.time_basis.not_recorded`. Full restores still run.
`verify_scorecard.py` 1.18.0 and both other readers print the label; the
controller copies it to `Restore.status.timeBasis`, served as `timeBasis`
([stability.md](stability.md#a-point-in-time-over-a-logappendtime-source-is-refused-unless-the-plan-selects-by-producer-time),
[drill-spec.md](formats/drill-spec.md#restoretime_basis-fx-8)). A
`RehearsalSchedule` (withdrawn by item 73) took the opt-in as
`spec.point.timeBasis: producerTime`.
**Do:** before the runner image rolls, find the source topics your
point-in-time restores name that are `LogAppendTime`, by override or by the
broker's default (`--all` prints every topic's effective value):

```bash
kafka-configs.sh --bootstrap-server <source> --describe --entity-type topics --all \
  | awk '/configs for topic/ {t=$5} /^ *message.timestamp.type=LogAppendTime/ {print t}'
```

For each, decide whether a restore by the producers' clocks is acceptable; if
it is, add `restore.time_basis: producerTime` to the plan and re-approve it.
**Scope:** `crates/logweir-core/src/time_basis.rs`, phase rows, the four arms in both readers with the corpus and parity gate, console rows and mutants, and live compose rows in `record_semantics.rs` and `config_coverage.rs`; its refusal and opt-in rows are the PoC upgrade's.
**Rollback:** an older runner ignores `restore.time_basis` and runs the
selections this build refuses, unlabelled, signing format 1.1.0 again; 1.3.0
scorecards stay valid.

### Verification scope: what "verified" means in this release

- **A green badge** means the signed document's signature verified under a key
  the namespace's trust accepts **and** the run's own success field
  (`exitCode == 0` for a Backup, `outcome == pass` and a zero `exitCode` for a
  Restore). A failed restore's signed scorecard is published and verified, and
  is never green and never gets the completion panel. `Historical` (signed
  before the key was retired) is a pass; `RecordedBeforeRevocation` never is
  ([kubernetes.md](kubernetes.md) §15.2–15.2a).
- **Record checks are samples.** `verificationScope` is `sampled`, `degraded` or
  `none`, never `complete`, in this release (`recordsSampled`,
  `recordsSampledMatching` and the window beside them are the exact claim);
  item 30 adds complete coverage after it.
- **The independent verifier** (`docs/verify_scorecard.py`) checks the same
  signed documents with no Logweir code ([verify-a-scorecard.md](verify-a-scorecard.md)).
- **Tested environments:**
  docker-desktop Kubernetes with a SCRAM (and private-CA TLS) Kafka and MinIO;
  the PoC profile on docker-desktop; and the GitHub Actions Compose suite.
  Nothing here was run against AWS S3, MSK, EKS, a corporate identity provider
  or a NetworkPolicy-enforcing CNI.

### Retention authority

- **A schedule's `spec.retention` only reports** in this release (withdrawn in
  v1, item 78). It prints the commands an operator would run and deletes
  nothing.
- **A `RetentionPolicy` is created in `mode: Report`** and deletes nothing.
  Only `logweir-retention-admin` (namespaced, and bound to somebody who does not
  hold `logweir-operator`) may move it to `Enforce`.
- **`Enforce` deletes only when all four gates hold**: the mode, an
  administrator's `approvedPlanSha256` equal to the current plan digest and
  younger than `planMaxAgeSeconds`, a lease written before a cluster-wide
  `Restore` check, and a worker that re-validates every key against the
  policy's prefix. The worker runs under its own delete-capable credential,
  never under `logweir/`, and writes create-only tombstones and a record
  (unsigned in this build) under `logweir/retention/`
  ([kubernetes.md](kubernetes.md) §7f).
- **`ExternalLifecycle` is a declaration**: the bucket's rule deletes, and
  Logweir verifies nothing about it.
- **A `Restore` created after a run's cluster-wide check and before its first
  delete is not held by the lease** in this build; suspend enforcement
  (`mode: Report`) around a large restore.

### Migration and rollback

**Upgrade order:** identity backup → retention grants and modes (items 1–2) →
the pre-upgrade inventories (items 16, 18, 21, 26 and 27) → CRDs (all fourteen
established) → controller **and** runner image together → console image →
approval-policy binding. Every CRD change is additive; nothing is converted and
no stored object is rewritten ([install.md](install.md), *Upgrade CRDs before
upgrading the controller*).

**Before a rollback**, in this order:

1. Set every `RetentionPolicy` to `mode: Report`, wait for `status.lease` to
   clear and any enforcement Job to finish (items 2 and 3;
   [kubernetes.md](kubernetes.md#rolling-back)).
2. Delete `Preflight` objects with `operation: SourceConnection`: an older
   controller cannot decode them and stops reconciling every `Preflight`
   ([kubernetes.md](kubernetes.md#rolling-back)).
3. If rolling back past Amendment G, delete every `Approval` whose
   `spec.subjectRef.kind` is `RehearsalSchedule` first ([kubernetes.md](kubernetes.md#rolling-back)).
4. Let destination-backed and `v2`-frozen `Backup`s finish; an older controller
   refuses them terminally rather than running them
   ([kubernetes.md](kubernetes.md#rolling-back)). Rolling the
   runner back to a build without the execution claim, let EVERY in-flight
   `Backup` finish first (item 11): on an unversioned bucket a re-created Job's
   older runner rewrites the set's segments under an unchanged manifest, and no
   check reports it.
5. Let point-bound `Restore`s that have no Job yet reach one, or recreate them
   after the rollback: an older controller ends them `ApprovalBundleConflict`
   (item 5; fail-closed, nothing is restored).
6. Unbind approval policies, or expect not-yet-admitted v2 approvals to be
   refused (item 7).
7. Roll the controller and runner back together (the runner not after the
   controller: item 12), with no `Restore` running, and leave the CRDs in place.
8. **Rolling back to a chart that did not render an object this one adopted
   deletes it.** `helm rollback` to `v0.1.5` removes the runner ServiceAccount
   and the `logweir-s3` Secret the upgrade adopted in each runner namespace;
   re-create them before the next run, or every run fails
   `serviceaccount "logweir-runner" not found`.
9. **Take every compromised key out of the roster first** (item 16): re-create
   `TrustRoster/default` without every key any `TrustPolicy` revoked for
   `KeyCompromise` (a policy's `CompromiseGuard` message says
   "TrustRoster/default still lists …" while one is there), and record each such
   revocation on every policy that still lists the key (`CompromiseInherited`).
   Do not delete a policy to "go back to the roster" before that: it is held
   until nothing lists the key.
10. **Suspend every `RehearsalSchedule` this build skips for its
    `runnerResources`** (item 21) that must not run uncapped: an older
    controller drops the block and fires its next slot.
11. **With a non-default `runs.*` or `api.console.rateLimits.*` value, roll back
    with the chart** (`helm rollback`), never the images alone (item 15): an
    older controller refuses the whole `weirkeeper-policy` ConfigMap and an
    older console its configuration.

**How this upgrade is rehearsed.** From `v0.1.5` (6 → 14 CRDs, the managed
identity adopting a hand-provisioned signer, the console arriving) and from
`sha-f49849d…`, each to the first PoC publication `86a554e6`, which crosses
items 1–13, and each rolled back; then in place to `02dc44b6` (items 14, 15 and
17), `b748fd5f` (16, 18 and 19), `a54fb823` (20), `815249cb` and `fdb48cd8` (no
item, console-only fixes). An upgrade from `sha-7b0277b…` crosses items
1–4 and 11–20. An upgrade from `fdb48cd8` crosses items 21, 22, 23, 24, 25, 26
and 27, and item 11's FX-7 additions: grant `s3:GetObjectVersion` before the
upgrade, or a pinned point whose current version differs fails closed at the
binding, and let in-flight Backups finish before rolling the runner back.

**The chart and the images move together.** This chart's controller probes run
`weirkeeper --probe`, and its console configuration can carry
`oidc.caBundleFile` and `trustedProxyService`: a controller image older than the
chart fails its liveness probe, and an older console refuses the configuration
(exit 2, `unknown field`). `helm rollback` restores the previous chart and images
together; pin all images to one build whenever you override them.

Archives, evidence and catalog records are untouched in both directions; old
signed archives keep verifying as long as their public keys stay in the trust
policy or roster ([keys.md](keys.md)).

### Limitations and open items

- **The live half of PLAT-20.2 ran on 2026-09-24 and 2026-09-25** with the PoC
  profile on docker-desktop, from published charts and images only: a clean
  install at `86a554e6`, two upgrades to it with rollbacks, and five in-place
  upgrades, after which all 356 backup receipts still passed the independent
  verifier.
  [UNVERIFIED — R2's pre-upgrade retention, mount-failure and point-bound-restore states were not set up.]
- **The large catalog was measured live at 258 real points, not 1,000**
  ([stability.md](stability.md#measured-scale-limits-plat-202)); the console
  refuses a list longer than 5,000 rows rather than showing a prefix.
  [UNVERIFIED — a 1,000-point archive was not reached live; 258 real points were measured on docker-desktop.]
- **Fixed and proven live:** P15 (`815249cb`): every readiness follower reads
  its check until the longest time a check may take (12 minutes; a discovery 7)
  and then says "did not finish", with *Run the check again*. P16 (`fdb48cd8`):
  a repaint of a running check keeps the reader's scroll and focus
  (`ui/README.md`, in the console PROD-18.1 replaced).
- **A followed check stops at the ingress's own `503` (P17, console, open).**
  When no console pod is ready the ingress answers `503` with a plain-text body,
  and the page stops following ("could not be read again") though the check
  finishes in the cluster. Run the check again, or reload once the console
  answers.
- **The demo MinIO is a rebuilt mirror.** MinIO withdrew its public images. The
  chart's demo MinIO, the e2e stack and the PoC run the same MinIO and `mc`
  releases rebuilt from the archived source,
  `docker.io/vladyslavhaina/minio-mirror` and `mc-mirror` (AGPL-3.0). Replacing
  MinIO (REPLACE-MINIO) is open.
- **A console restore's replication factor does not start from the source's**
  in this release (item 22); item 32 adds it.
- **The `v0.1.1`–`v0.1.5` image tags on Docker Hub are leftovers of failed
  runs, not releases** (`logweir:v0.1.1`–`v0.1.5`, `weirkeeper:v0.1.2`–`v0.1.5`,
  `logweir-ui:v0.1.3`–`v0.1.5`; no console image, chart or GitHub Release). Do
  not install or pin them. The owner decided on 2026-10-07 to delete them once
  v0.2.0 ships. The pipeline now only tags main CI's `sha-<commit>` images.
- **The CLI archives' run-time needs.** The Linux archives are built in the
  runner image's builder base (`rust:1.89-bookworm`): they need a glibc at
  least as new as the version each release's notes give (never above Debian
  12's 2.36), and `libssl.so.3`, `libcrypto.so.3`, `libsasl2.so.2` and
  `libz.so.1` (Debian and Ubuntu: `libssl3`, `libsasl2-2`, `zlib1g`). A
  distribution whose SASL library has another soname (`libsasl2.so.3` on RHEL
  and Fedora) builds from the checkout. The macOS archive needs Homebrew's
  `openssl@3`. `logweir --version` prints the workspace version (`0.1.0`), not
  the tag; `release.json` ties each archive to its tag and commit.
- **The product API's OpenAPI document is `1.0.0-alpha.2`, still a
  pre-release**, although the console image and the chart consume it; ship and
  upgrade the console and the API together until PROD-14.2 freezes it at
  `1.0.0` ([stability.md](stability.md)). Since `1.0.0-alpha.1` (2026-09-16,
  never published) it gained 38 operations and removed none, and its component
  schemas grew from 66 to 258.
- **No in-place runner signing-key cutover** ([keys.md](keys.md), step 2 of
  *The supported procedure*).
- **Restore admission does not hold on a retention lease** (above).
- The standing limitations of [stability.md](stability.md), *Known limitations*,
  and the `[UNVERIFIED]` marks it carries (AWS S3 create-only puts, MSK, the
  NetworkPolicy, the 1.30+ admission policy) are unchanged.

---

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
