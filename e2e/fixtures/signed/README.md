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
- **`evidence.create_only_enforced: true`** — in `scorecard.json` and
  `scorecard-self-attested.json`, **the two files in THIS directory**. Task 20
  made phase 8 zero all four `evidence` fields before signing, because they
  describe an upload that has not happened yet; the real post-put readback lives
  in the separately signed storage receipt.

  This bullet used to say "(in `scorecard-pass.json`)". That was the file
  **outside** this directory, `e2e/fixtures/scorecard-pass.json`, and it is no
  longer true of it: that file is **unsigned**, so the signature argument above
  never applied to it, and Task 22 corrected its value to `false` rather than
  documenting a divergence that did not have to exist. See
  `e2e/fixtures/README.md`.

Regenerating and re-signing the two files in this directory is release
engineering's call, not a test's — and it is not free: `mint_fixture` generates
a NEW key pair on every run, so `public.pem` and `signing.pem` change with them.


---

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
