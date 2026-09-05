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
- **`last_phase_completed: 9`.** No real drill can sign that value.
  `phase8_score::run` is handed a frozen clone, so phase 8's own phase record —
  and phase 9's teardown, which happens after the put — are pushed onto the
  in-memory document AFTER the bytes were signed; a v0.1.0 signed scorecard
  therefore ends at **5, 6 or 7**, and a completed drill reads `7`. Reading `9`
  here is not "teardown ran"; reading `7` in a real artifact is not "teardown
  did not run" (it is attested in its own signed document). `emit_fixture` now
  emits `7`, so this divergence closes by itself the next time
  `just fixtures-sign` is run; it is listed rather than fixed here for the
  same reason as the bullet below. The unsigned `e2e/fixtures/scorecard-pass.json`
  was corrected in place, exactly as `create_only_enforced` was.
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
