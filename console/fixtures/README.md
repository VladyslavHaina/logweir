# Console test fixtures

Test data both halves of the product read: the console's Vitest rows (`console/src/`) and
the Rust rows of `logweir-api`, `weirkeeper`, `logweir-core` and `logweir`. Moved here from
`ui/tests/fixtures/` when the old console was retired (PROD-18.1 R8); where each capture came
from is recorded in that file's history (`git log --follow console/fixtures/README.md`).

- `console/` holds documents shaped as the product API answers them
  (`schemas/logweir-api-v1.openapi.json`). The mock build (`src/mock/`) reads some of them.
- The top level and `d3/` hold custom resources as the Kubernetes API returns them; where an
  object names a Secret, only its name is present.
- `plan*-fields.json` are the arguments to the console's `renderPlanBytes`, and
  `plan*.golden.yaml` what it renders. `src/pages/restores/plan.test.ts` holds the renderer to
  the goldens byte for byte; `crates/logweir/tests/console_plan.rs` deserialises each golden
  into the runner's `RestoreSpec`. `plan-clone.golden.yaml` is theme A's clone, which a runner
  from before theme A refuses; `plan-owners.golden.yaml` states the topics' declarative owners
  for the source's topic configuration; `plan-registry.golden.yaml` names the target connection's Schema
  Registry and a byte-rate limit (OD-23).
- `legacy-console.json` records what the retired console produced for the inputs the new
  console's differential rows used, so those rows still compare the two.

Some files are written by the Rust side and checked by both; regenerate them, never edit them:

```
LOGWEIR_WRITE_FIXTURES=1 cargo test --locked -p weirkeeper --test cadence \
  the_preset_catalogue_fixture_matches_the_rust_catalogue
LOGWEIR_WRITE_FIXTURES=1 cargo test --locked -p weirkeeper --test retention_policy_controller fx22_the_shared_fixture
LOGWEIR_WRITE_FIXTURES=1 cargo test --locked -p logweir-api --test retention_accounting
```

No private key material is here, and no fixture carries a credential.

Documentation is licensed [CC-BY-4.0](../../docs/LICENSE-docs).
