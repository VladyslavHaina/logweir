# Approval-policy documents the binary refuses

Each `*.values.yaml` here is one approval-policy document that
`logweir_core::approval_policy::ApprovalPolicySet::parse` refuses. A document
the binary refuses stops the whole `weirkeeper` controller at start, so the chart
must refuse the same document in `helm template`.

This directory is the single list of cases, and two gates read it:

- `scripts/check-chart.sh` (section 9) renders the chart with each file and
  requires the render to fail, naming the text on the file's `# expect:` line.
- `crates/logweir/tests/chart_lint.rs`
  (`chart_lint_every_approval_policy_refusal_is_the_binarys`) feeds each file's
  `approvalPolicy` document to `ApprovalPolicySet::parse` and requires a refusal.

When you add a refusal to either side, add its case here, and both gates must
then refuse it.
