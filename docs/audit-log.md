# Keeping the console's audit log

The console (`logweir-api`) writes one audit line per API request: who asked
(`actorId`, the issuer and subject), for what (`method`, `path`, the action),
in which workspace (`workspace`), with which roles, and whether it was allowed
(`decision`, `failureCode`). `auditId` is the `X-Request-ID` the browser was answered with,
and [api.md](api.md) describes the notes an approval adds. No cookie, token,
Secret value, plan or approval body is ever written.

**The line goes to the container's stdout and nowhere else.** Logweir stores
no copy. Kubernetes keeps a container's log only until the kubelet rotates it
(10 MiB × 5 files by default) and loses it with the pod, so an install that
needs the record ships it somewhere that keeps it.

## The line

The console logs JSON. An audit line is the one whose `target` is
`logweir_api::audit`; the record itself is the JSON string in
`fields.audit`:

```json
{"timestamp":"2026-10-11T09:14:03.120Z","level":"INFO","fields":{"message":"audit","audit":"{\"auditId\":\"…\",\"method\":\"POST\",\"path\":\"/api/v1/workspaces/team-a/backups\",\"authenticationMode\":\"oidc\",\"actorId\":\"https://idp.example.com/realms/logweir#4f1c…\",…,\"decision\":\"allow\",…}","notes":""},"target":"logweir_api::audit"}
```

The lines are written at `info`. The chart sets no `RUST_LOG` on the console,
which means `info`; an install that sets `RUST_LOG=warn` there stops writing
them.

## Shipping it to object storage

Any log shipper that reads pod logs works. This example uses
[Vector](https://vector.dev/) as a DaemonSet (its Helm chart's `customConfig`),
keeps only the audit lines, and writes them, unchanged, to an S3 bucket:

```yaml
sources:
  console:
    type: kubernetes_logs
    extra_label_selector: "app.kubernetes.io/name=logweir,app.kubernetes.io/component=api"
transforms:
  audit:
    type: filter
    inputs: [console]
    condition: |
      parsed, err = parse_json(.message)
      err == null && parsed.target == "logweir_api::audit"
sinks:
  audit_bucket:
    type: aws_s3
    inputs: [audit]
    bucket: logweir-audit
    key_prefix: "audit/%Y/%m/%d/"
    region: eu-west-1
    compression: gzip
    encoding:
      codec: text          # the line exactly as the console wrote it
```

Fluent Bit does the same with a `tail` input on the console's container log,
a JSON parser and a `grep` filter on `target`, and the `s3` output.

## Retention

- **A bucket of its own.** Not the archive bucket, which a `RetentionPolicy`'s
  delete credential reaches, and not the evidence bucket. The shipper's
  credential writes objects under `audit/` and deletes nothing.
- **A lifecycle rule sets how long the record lives.** Logweir has no default;
  take the period from your own audit policy, and keep the record at least as
  long as the backups and restores it might have to explain:

  ```sh
  aws s3api put-bucket-lifecycle-configuration --bucket logweir-audit \
    --lifecycle-configuration '{"Rules":[{"ID":"audit-retention","Status":"Enabled",
      "Filter":{"Prefix":"audit/"},"Expiration":{"Days":400}}]}'
  ```

- **If the record must not be altered**, use a bucket with S3 Object Lock and
  a default retention no longer than the lifecycle period. Object Lock needs
  versioning, so the rule then also needs `NoncurrentVersionExpiration` to
  remove what it expires.
- **It is an attribution record, not a signature.** The tamper-evident record
  of who approved a restore is the signed approval, and Kubernetes audit
  records that the `<release>-api` ServiceAccount made each call.

---

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
