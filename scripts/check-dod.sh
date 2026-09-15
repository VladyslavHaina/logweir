#!/usr/bin/env bash
# Optional release-asset check. Build/test health lives in ci-check.sh; remote
# publication and live recovery are verified by the release and integration jobs.
set -euo pipefail
cd "$(dirname "$0")/.."

for file in README.md LICENSE NOTICE TRADEMARKS.md SECURITY.md \
  CONTRIBUTING.md THIRD_PARTY_NOTICES.md docs/quickstart.md \
  docs/support-matrix.md docs/architecture.md docs/kubernetes.md \
  schemas/logweir-drill-scorecard-1.0.0.json \
  schemas/logweir-backup-receipt-1.0.0.json \
  third_party/kafka-backup-binary.digest third_party/LICENSE-MIT \
  third_party/org-root.pub.pem third_party/org-root.fingerprint \
  .github/workflows/release.yml; do
  if [ ! -s "$file" ]; then
    echo "release assets: missing or empty $file" >&2
    exit 1
  fi
done

# Verify source attribution artifacts against their recorded checksums.
found=0
for archive in third_party/kafka-backup-*.tar.gz; do
  [ -f "$archive" ] || continue
  found=1
  shasum -a 256 -c "$archive.sha256"
done
[ "$found" -eq 1 ] || { echo 'release assets: upstream source archive is missing' >&2; exit 1; }
grep -Eq '^sha256:[0-9a-f]{64}$' third_party/kafka-backup-binary.digest

# The checked-in trust anchor must actually represent its public key.
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
openssl pkey -pubin -in third_party/org-root.pub.pem -outform DER -out "$tmp/key.der"
openssl dgst -sha256 -r "$tmp/key.der" > "$tmp/digest"
read -r digest _ < "$tmp/digest"
printf 'sha256:%s\n' "$digest" > "$tmp/fingerprint"
diff -u third_party/org-root.fingerprint "$tmp/fingerprint"

echo 'release assets: required files, source checksums and public trust anchor passed'
echo 'Publication and recovery results must be read from the actual workflow runs.'
