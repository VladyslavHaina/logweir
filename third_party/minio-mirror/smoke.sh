#!/usr/bin/env bash
# third_party/minio-mirror/smoke.sh — the behaviour Logweir relies on from MinIO,
# run against ONE (minio image, mc image, platform) triple.
#
#   bash third_party/minio-mirror/smoke.sh <minio-image> <mc-image> <platform> <out-file>
#
# It starts the server image on a private Docker network, drives it with the mc
# image and with the HOST's curl (a fixed client, so the server is the only
# thing that varies between runs), and writes one `check<TAB>observed` line per
# check to <out-file>. Every observed value is compared with the value upstream
# MinIO gives; the script exits 0 only when every check matched, and 1 naming
# each one that did not. Run it against the upstream images and against the
# mirror and `diff` the two files: README.md records that both are identical.
#
# What it covers, and why each is here:
#   * --version of minio, of the mc inside the server image, and of the mc image;
#   * bucket create (`mc mb`), the chart's seed Job and the compose `minio-setup`;
#   * a user with a least-privilege policy (`mc admin user add`, `mc admin policy
#     create/attach`) that ALLOWS PutObject and DENIES GetObject, checked from
#     both sides: the user's PUT lands, the user's GET and LIST are refused, the
#     root reads the object back byte for byte;
#   * versioning, and Object Lock on a bucket created with lock enabled (default
#     GOVERNANCE retention; a locked version refuses deletion);
#   * the conditional create: a second PUT with `If-None-Match: *` on an existing
#     key is 412 — Logweir's execution claim (`PutMode::Create`);
#   * HEAD (200 / 404), list, delete.
#
# Needs: docker, curl >= 7.75 (for --aws-sigv4), jq. Every docker and curl call
# runs under a deadline. Removes its network, containers and volume on exit.
set -u

MINIO_IMAGE="${1:?usage: smoke.sh <minio-image> <mc-image> <platform> <out-file>}"
MC_IMAGE="${2:?usage: smoke.sh <minio-image> <mc-image> <platform> <out-file>}"
PLATFORM="${3:?usage: smoke.sh <minio-image> <mc-image> <platform> <out-file>}"
OUT="${4:?usage: smoke.sh <minio-image> <mc-image> <platform> <out-file>}"
ARCH="${PLATFORM#linux/}"

# A deadline for every child: `timeout` where it exists, else a perl alarm.
if command -v timeout >/dev/null 2>&1; then
  deadline() { timeout "$@"; }
else
  deadline() { perl -e 'alarm(shift); exec(@ARGV) or die "exec: $!"' "$@"; }
fi

TAG="lw-minio-smoke-$$-$(date +%s)"
NET="$TAG"
SRV="$TAG-srv"
ROOT_USER="smokeroot"
ROOT_PASS="smoke-root-password-not-a-secret"
WRITER_PASS="smoke-writer-password-not-a-secret"
W="$(mktemp -d "${TMPDIR:-/tmp}/lw-minio-smoke.XXXXXX")"

cleanup() {
  deadline 60 docker rm -f -v "$SRV" >/dev/null 2>&1
  deadline 60 docker network rm "$NET" >/dev/null 2>&1
  rm -rf "$W"
}
trap cleanup EXIT

: >"$OUT"
FAILED=""
# record <check> <observed> <expected>
record() {
  printf '%s\t%s\n' "$1" "$2" >>"$OUT"
  if [ "$2" != "$3" ]; then
    FAILED="$FAILED $1"
    printf 'MISMATCH %s: observed [%s] expected [%s]\n' "$1" "$2" "$3" >&2
  fi
}

mc() {
  deadline 120 docker run --rm --platform "$PLATFORM" --network "$NET" \
    -v "$W:/w" \
    -e "MC_HOST_root=http://$ROOT_USER:$ROOT_PASS@$SRV:9000" \
    -e "MC_HOST_writer=http://writer:$WRITER_PASS@$SRV:9000" \
    "$MC_IMAGE" "$@"
}
# s3 <user:pass> <curl args…> — the host's curl, SigV4-signed, against the published port.
s3() {
  local cred="$1"; shift
  deadline 60 curl -sS --aws-sigv4 "aws:amz:us-east-1:s3" --user "$cred" "$@"
}

deadline 60 docker network create "$NET" >/dev/null || { echo "smoke: network create failed" >&2; exit 2; }

# --- versions (the image's own binaries) -----------------------------------
v="$(deadline 120 docker run --rm --platform "$PLATFORM" --entrypoint minio "$MINIO_IMAGE" --version 2>&1)"
record version.minio "$(printf '%s\n' "$v" | sed -n 1p)" \
  "minio version RELEASE.2025-09-07T16-13-09Z (commit-id=07c3a429bfed433e49018cb0f78a52145d4bedeb)"
record version.minio.runtime "$(printf '%s\n' "$v" | sed -n 2p)" "Runtime: go1.24.6 linux/$ARCH"
v="$(deadline 120 docker run --rm --platform "$PLATFORM" --entrypoint mc "$MINIO_IMAGE" --version 2>&1)"
record version.mc-in-server-image "$(printf '%s\n' "$v" | sed -n 1p)" \
  "mc version RELEASE.2025-08-13T08-35-41Z (commit-id=7394ce0dd2a80935aded936b09fa12cbb3cb8096)"
v="$(deadline 120 docker run --rm --platform "$PLATFORM" --entrypoint curl "$MINIO_IMAGE" --version 2>&1)"
record version.curl-in-server-image "$(printf '%s\n' "$v" | sed -n 1p | cut -d' ' -f1-2)" "curl 8.11.0"
v="$(deadline 120 docker run --rm --platform "$PLATFORM" "$MC_IMAGE" --version 2>&1)"
record version.mc "$(printf '%s\n' "$v" | sed -n 1p)" \
  "mc version RELEASE.2025-08-13T08-35-41Z (commit-id=7394ce0dd2a80935aded936b09fa12cbb3cb8096)"
record version.mc.runtime "$(printf '%s\n' "$v" | sed -n 2p)" "Runtime: go1.24.6 linux/$ARCH"

# --- the server, started the way compose and the chart start it -----------
# `server /data` with no `minio` in front: docker-entrypoint.sh prepends it.
deadline 120 docker run -d --platform "$PLATFORM" --name "$SRV" --network "$NET" \
  -p 127.0.0.1::9000 \
  -e "MINIO_ROOT_USER=$ROOT_USER" -e "MINIO_ROOT_PASSWORD=$ROOT_PASS" \
  "$MINIO_IMAGE" server /data >/dev/null || { echo "smoke: server did not start" >&2; exit 2; }
PORT="$(deadline 30 docker port "$SRV" 9000/tcp | sed -n 's/.*://p' | head -n 1)"
[ -n "$PORT" ] || { echo "smoke: no published port" >&2; exit 2; }
URL="http://127.0.0.1:$PORT"
ready=no
for _ in $(seq 1 90); do
  code="$(deadline 5 curl -s -o /dev/null -w '%{http_code}' "$URL/minio/health/ready" || true)"
  if [ "$code" = 200 ]; then ready=yes; break; fi
  sleep 1
done
record server.ready "$ready" yes
if [ "$ready" != yes ]; then
  deadline 30 docker logs "$SRV" >&2
  echo "smoke: FAILED:$FAILED" >&2
  exit 1
fi
# The in-image readiness probe compose and the chart use.
deadline 60 docker exec "$SRV" mc ready local >/dev/null 2>&1
record server.mc-ready-local "exit=$?" exit=0

# --- bucket, user, least-privilege policy ----------------------------------
mc mb root/smoke >/dev/null 2>&1;                          record bucket.create "exit=$?" exit=0
mc mb --ignore-existing root/smoke >/dev/null 2>&1;        record bucket.create-ignore-existing "exit=$?" exit=0
cat >"$W/put-only.json" <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": ["s3:PutObject"], "Resource": ["arn:aws:s3:::smoke/*"] },
    { "Effect": "Deny",  "Action": ["s3:GetObject"], "Resource": ["arn:aws:s3:::smoke/*"] }
  ]
}
JSON
printf 'logweir-smoke-payload\n' >"$W/a.txt"
printf 'version-one\n' >"$W/v1.txt"
printf 'version-two\n' >"$W/v2.txt"
mc admin policy create root put-only /w/put-only.json >/dev/null 2>&1; record policy.create "exit=$?" exit=0
mc admin user add root writer "$WRITER_PASS" >/dev/null 2>&1;         record user.add "exit=$?" exit=0
mc admin policy attach root put-only --user writer >/dev/null 2>&1;   record policy.attach "exit=$?" exit=0
p="$(mc admin user info root writer --json 2>/dev/null | jq -r '.policyName // empty')"
record user.policy "$p" put-only

# The writer's side: PUT allowed; GET, LIST and another bucket refused.
mc cp /w/a.txt writer/smoke/a.txt >/dev/null 2>&1;         record writer.put "exit=$?" exit=0
out="$(mc cat writer/smoke/a.txt 2>&1)"; rc=$?
case "$out" in *"Insufficient permissions"*|*"Access Denied"*) d=denied ;; *) d="allowed(rc=$rc)" ;; esac
record writer.get "$d" denied
code="$(s3 "writer:$WRITER_PASS" -o /dev/null -w '%{http_code}' "$URL/smoke/a.txt")"
record writer.get.http "$code" 403
code="$(s3 "writer:$WRITER_PASS" -o /dev/null -w '%{http_code}' "$URL/smoke/?list-type=2")"
record writer.list.http "$code" 403
mc mb root/other >/dev/null 2>&1
code="$(s3 "writer:$WRITER_PASS" -o /dev/null -w '%{http_code}' -T "$W/a.txt" "$URL/other/a.txt")"
record writer.put-other-bucket.http "$code" 403
# The root's side: the object the writer PUT is there, byte for byte.
got="$(mc cat root/smoke/a.txt 2>/dev/null)"
[ "$got" = "logweir-smoke-payload" ] && r=match || r="differs:[$got]"
record root.get "$r" match

# --- versioning ------------------------------------------------------------
mc version enable root/smoke >/dev/null 2>&1;              record versioning.enable "exit=$?" exit=0
s="$(mc version info root/smoke --json 2>/dev/null | jq -r '.versioning.status // empty')"
record versioning.status "$s" Enabled
mc cp /w/v1.txt root/smoke/v.txt >/dev/null 2>&1
mc cp /w/v2.txt root/smoke/v.txt >/dev/null 2>&1
n="$(mc ls --versions root/smoke/v.txt --json 2>/dev/null | jq -s 'map(select(.status=="success")) | length')"
record versioning.versions "$n" 2
got="$(mc cat root/smoke/v.txt 2>/dev/null)"
record versioning.latest "$got" version-two

# --- Object Lock -----------------------------------------------------------
mc mb --with-lock root/locked >/dev/null 2>&1;             record lock.mb-with-lock "exit=$?" exit=0
s="$(mc version info root/locked --json 2>/dev/null | jq -r '.versioning.status // empty')"
record lock.bucket-versioning "$s" Enabled
mc retention set --default GOVERNANCE 1d root/locked >/dev/null 2>&1; record lock.default-set "exit=$?" exit=0
s="$(mc retention info --default root/locked --json 2>/dev/null | jq -r '[.enabled, .mode, .validity] | join(" ")')"
record lock.default-info "$s" "Enabled GOVERNANCE 1DAYS"
mc cp /w/a.txt root/locked/o.txt >/dev/null 2>&1;          record lock.put "exit=$?" exit=0
info="$(mc retention info root/locked/o.txt --json 2>/dev/null)"
record lock.object-mode "$(printf '%s' "$info" | jq -r '.mode // empty')" GOVERNANCE
vid="$(mc stat root/locked/o.txt --json 2>/dev/null | jq -r '.versionID // empty')"
[ -n "$vid" ] && r=present || r=absent
record lock.object-version-id "$r" present
out="$(mc rm --version-id "$vid" root/locked/o.txt 2>&1)"; rc=$?
case "$out" in *"WORM"*|*"retention"*|*"Object is WORM protected"*) d=refused ;; *) d="rc=$rc:$(printf '%s' "$out" | head -n 1)" ;; esac
record lock.delete-locked-version "$d" refused
code="$(s3 "$ROOT_USER:$ROOT_PASS" -o "$W/del.xml" -w '%{http_code}' -X DELETE "$URL/locked/o.txt?versionId=$vid")"
c="$(sed -n 's:.*<Code>\(.*\)</Code>.*:\1:p' "$W/del.xml" | head -n 1)"
m="$(sed -n 's:.*<Message>\(.*\)</Message>.*:\1:p' "$W/del.xml" | head -n 1)"
record lock.delete-locked-version.http "$code $c $m" "400 InvalidRequest Object is WORM protected and cannot be overwritten"
out="$(mc retention set --default GOVERNANCE 1d root/other 2>&1)"; rc=$?
[ "$rc" != 0 ] && r=refused || r=accepted
record lock.default-on-unlocked-bucket "$r" refused

# --- the conditional create: Logweir's execution claim ---------------------
code="$(s3 "$ROOT_USER:$ROOT_PASS" -o /dev/null -w '%{http_code}' -H 'If-None-Match: *' -T "$W/a.txt" "$URL/smoke/claim")"
record claim.first.http "$code" 200
code="$(s3 "$ROOT_USER:$ROOT_PASS" -o "$W/claim2.xml" -w '%{http_code}' -H 'If-None-Match: *' -T "$W/v1.txt" "$URL/smoke/claim")"
record claim.second.http "$code" 412
c="$(sed -n 's:.*<Code>\(.*\)</Code>.*:\1:p' "$W/claim2.xml" | head -n 1)"
record claim.second.code "$c" PreconditionFailed
got="$(mc cat root/smoke/claim 2>/dev/null)"
[ "$got" = "logweir-smoke-payload" ] && r=first-kept || r="overwritten:[$got]"
record claim.object-unchanged "$r" first-kept
mc put --if-not-exists /w/a.txt root/smoke/mc-claim >/dev/null 2>&1; record claim.mc-first "exit=$?" exit=0
st="$(mc --debug put --if-not-exists /w/v1.txt root/smoke/mc-claim 2>&1 | grep -o 'HTTP/1.1 [0-9][0-9][0-9] [A-Za-z ]*' | tail -n 1)"
record claim.mc-second "$st" "HTTP/1.1 412 Precondition Failed"

# --- HEAD, list, delete ----------------------------------------------------
code="$(s3 "$ROOT_USER:$ROOT_PASS" -o /dev/null -w '%{http_code}' -I "$URL/smoke/a.txt")"
record head.existing.http "$code" 200
code="$(s3 "$ROOT_USER:$ROOT_PASS" -o /dev/null -w '%{http_code}' -I "$URL/smoke/no-such-key")"
record head.missing.http "$code" 404
sz="$(mc stat root/smoke/a.txt --json 2>/dev/null | jq -r '.size // empty')"
record head.mc-stat-size "$sz" 22
keys="$(mc ls root/smoke --json 2>/dev/null | jq -r 'select(.status=="success") | .key' | sort | tr '\n' ' ' | sed 's/ $//')"
record list.keys "$keys" "a.txt claim mc-claim v.txt"
code="$(s3 "$ROOT_USER:$ROOT_PASS" -o /dev/null -w '%{http_code}' "$URL/smoke/?list-type=2&prefix=a")"
record list.http "$code" 200
mc rm root/smoke/a.txt >/dev/null 2>&1;                    record delete "exit=$?" exit=0
code="$(s3 "$ROOT_USER:$ROOT_PASS" -o /dev/null -w '%{http_code}' -I "$URL/smoke/a.txt")"
record delete.head-after.http "$code" 404
n="$(mc ls --versions root/smoke/a.txt --json 2>/dev/null | jq -s 'map(select(.status=="success" and .isDeleteMarker==true)) | length')"
record delete.marker-on-versioned-bucket "$n" 1

if [ -n "$FAILED" ]; then
  echo "smoke: FAILED:$FAILED ($MINIO_IMAGE, $MC_IMAGE, $PLATFORM)" >&2
  exit 1
fi
echo "smoke: all $(wc -l <"$OUT" | tr -d ' ') checks matched ($PLATFORM)" >&2
exit 0
