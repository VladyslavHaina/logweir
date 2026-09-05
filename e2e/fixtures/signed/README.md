# Signed test fixtures

Everything in this directory, including `public.pem` and `signing.pem`, is a **throwaway test fixture**: the P-256 key pair here signs nothing outside this repository's own test suite, it is regenerated on demand by `just fixtures-sign`, and it must never be used to sign a real drill scorecard.

## What these fixtures show that v0.1 cannot produce

These are hand-authored examples of the scorecard FORMAT, signed so the verifier
paths have something real to check. Two of their fields describe states the
shipping code can no longer emit, and neither is regenerated here because doing
so would invalidate the signatures these fixtures exist to exercise:

- **`engine_subreport` is populated.** Every scorecard v0.1 actually produces
  has `engine_subreport: null` — `OsoCliEngine` does not override
  `DataEngine::validation_run`, so no engine report is ever retained. See
  `docs/stability.md`, "`engine_subreport` is always null in v0.1".
- **`evidence.create_only_enforced: true`** (in `scorecard-pass.json`). Task 20
  made phase 8 zero all four `evidence` fields before signing, because they
  describe an upload that has not happened yet; the real post-put readback lives
  in the separately signed storage receipt.

Regenerating and re-signing them is release engineering's call, not a test's.
