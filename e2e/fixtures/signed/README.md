# Signed test fixtures

Everything in this directory, including `public.pem` and `signing.pem`, is a **throwaway test fixture**: the P-256 key pair here signs nothing outside this repository's own test suite and must never be used to sign a real drill scorecard. The scorecard documents and their `.sig` sidecars are regenerated on demand by `just fixtures-sign`; the **keypair is pinned and read**, not regenerated — `mint_fixture` mints one only in a tree where `signing.pem` is absent. How the key was generated, how its fingerprint is computed, and how rotation works: [`docs/keys.md`](../../../docs/keys.md).

## What these fixtures show that v0.1 cannot produce

These are hand-authored examples of the scorecard FORMAT, signed so the verifier
paths have something real to check. ONE of their fields still describes a state
the shipping code cannot emit. The other two divergences this section used to
list are closed: `just fixtures-sign` regenerates the documents and re-signs
them under the PINNED keypair, so closing a divergence costs a signature that
was already going to be re-made and never costs the `917cf9a2…` fingerprint:

- **`engine_subreport` is populated.** Every scorecard v0.1 actually produces
  has `engine_subreport: null` — `OsoCliEngine` does not override
  `DataEngine::validation_run`, so no engine report is ever retained. See
  `docs/stability.md`, "`engine_subreport` is always null in v0.1".
- **`last_phase_completed` is `7`** — CLOSED, this pair used to read `9`. No
  real drill can sign `9`: `phase8_score::run` is handed a frozen clone, so
  phase 8's own phase record — and phase 9's teardown, which happens after the
  put — are pushed onto the in-memory document AFTER the bytes were signed; a
  v0.1.0 signed scorecard therefore ends at **5, 6 or 7**, and a completed drill
  reads `7`. Reading `7` in a real artifact is not "teardown did not run" (it is
  attested in its own signed document). `emit_fixture` has emitted `7` since
  Task 3; these files were re-minted from it, so the generator and the checked-in
  bytes now agree. `crates/logweir-core/tests/fixture_regen.rs::
  fixture_last_phase_matches_generator` keeps them agreeing. The `-1..=9` DOMAIN
  is unchanged (Global Constraint 18) — what changed is the value these two
  documents pin.
- **The `evidence` block is fully zeroed** — CLOSED, this pair used to carry
  `evidence.create_only_enforced: true`. Task 20 made phase 8 zero all four
  `evidence` fields before signing, because they describe an upload that has not
  happened yet; the real post-put readback lives in the separately signed
  storage receipt. Nothing enforced it, so these two signed files falsified the
  guarantee `docs/verify_scorecard.py` printed on every successful verification.
  `Scorecard::validate_invariants` now REFUSES a `1.0.x` document whose
  `version_id`, `retain_until`, `immutable` or `create_only_enforced` is set,
  and `docs/verify_scorecard.py::check_invariants` carries the same arm in the
  same position with the same words. A hand-edit of these files that re-sets one
  of the four cannot be signed and cannot be verified by either reader.

Regenerating and re-signing the two files in this directory is release
engineering's call, not a test's. It no longer costs the keypair, though:
`mint_fixture` READS `signing.pem` when it is present and mints only when it is
absent, so `public.pem` and `signing.pem` are unchanged by a re-mint and the
`917cf9a2…` fingerprint survives it.

## The deliberately bogus fixture

`scorecard-self-attested-bogus.json` and `scorecard-self-attested-bogus.sig`
are a **validly signed** document whose **claim is false**. The signature is
genuine — it is made by the same pinned `917cf9a2…` key as everything else
here, over exactly these bytes — and the document says
`approval.self_attested: true` while its `approval.key_id` is `"a"*64`, which
is not that key. Nothing about the cryptography is wrong; the document is
lying about its own provenance.

**It exists to be refused.** `logweir drill verify` exits **4** over it (the
same class as a bad signature: it is a provenance claim the signature cannot
support) and `docs/verify_scorecard.py` exits **1**, and both print the same
`APPROVAL CLAIM NOT VERIFIED: …` line byte for byte. Before T0-1 both readers
printed `approval: SELF-ATTESTED — the approval key equals the signing key`
over it and exited 0.

It is minted by `just fixtures-sign-bogus`. **`just fixtures-sign-bogus`
regenerates only the two `-bogus` files, loads the pinned `signing.pem`, and
mints no key** — so regenerating it never touches the other five files in this
directory (ruling R-G, `plan.md:60`). Its producer,
`crates/logweir-evidence/examples/mint_bogus_fixture.rs`, uses
`SigningKey::from_pem_file` rather than `load_or_generate` on purpose: a
missing key must be a loud failure here, never a silent mint.

## Running the checks over these fixtures

`scripts/check-verifier-parity.sh` (a `just lint` arm) runs **both** verifiers
over all three scorecard documents in this directory and compares the verdicts
and the refusal text. It needs a `python3` with the
[`cryptography`](https://cryptography.io/) package — `pip install
cryptography`, or point `$LOGWEIR_PYTHON` (or `$LOGWEIR_E2E_PYTHON`, or a
repo-local `.e2e/venv`) at an interpreter that has it. **It fails rather than
skipping when the package is missing:** a skipped parity check is a documented
guarantee nothing enforces, which is exactly what these fixtures exist to
prevent. `docs/test_verify_scorecard.py` needs the same package plus `pytest`.


---

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
