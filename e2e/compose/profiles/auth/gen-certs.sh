#!/usr/bin/env bash
# The `auth` profile's TLS material (PROD-01.5), generated ONCE per compose
# project into /certs, which docker-compose.yml binds to
# `.e2e/auth/<project>/` in the checkout (gitignored), so host-side clients —
# Logweir's librdkafka client, the engine through e2e/fixtures/engine-docker.sh
# — read the same files the broker serves.
#
# Everything here is a THROWAWAY TEST CA for one compose project on one
# machine. Nothing is committed; nothing authenticates anywhere else.
#
#   ca.pem / ca.key            the test CA (kept so a later task can mint more)
#   broker.keystore.pem        kafka-auth's PKCS#8 key + chain (PEM keystore,
#                              KIP-651); SANs localhost, kafka-auth,
#                              host.docker.internal, 127.0.0.1
#   client.pem / client.key    a client certificate (CN=logweir) for the mTLS
#                              listener, PKCS#8 key (librdkafka, the engine)
#   client.keystore.pem        the same key + chain in one file (a Java
#                              client's PEM keystore)
#   wrong-ca.pem               a CA that signed NOTHING here — the trust anchor
#                              a wrong-CA refusal row presents
#   wrong-client.pem / .key    a SELF-SIGNED client certificate (PROD-01.3) —
#                              the identity a wrong-certificate refusal row
#                              presents to the mTLS listener, which trusts
#                              only ca.pem
#
# Runs in the stack's CLI image (confluentinc/cp-kafka, which carries openssl).
set -euo pipefail
cd /certs
want="ca.pem ca.key broker.keystore.pem client.pem client.key client.keystore.pem wrong-ca.pem wrong-client.pem wrong-client.key"
missing=0
for f in $want; do [ -s "$f" ] || missing=1; done
if [ "$missing" = 0 ]; then
  echo "auth certs: present in $(pwd), reused"
  exit 0
fi
rm -f ./*.pem ./*.key ./*.csr ./*.srl ./*.ext ./*.orig
DAYS=825
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days "$DAYS" \
  -subj "/CN=logweir-e2e test CA" -keyout ca.key -out ca.pem 2>/dev/null

openssl req -newkey rsa:2048 -nodes -sha256 -subj "/CN=kafka-auth" \
  -keyout broker.orig -out broker.csr 2>/dev/null
printf '%s\n' \
  'subjectAltName=DNS:localhost,DNS:kafka-auth,DNS:host.docker.internal,IP:127.0.0.1' \
  'extendedKeyUsage=serverAuth,clientAuth' \
  'keyUsage=digitalSignature,keyEncipherment' > broker.ext
openssl x509 -req -sha256 -days "$DAYS" -in broker.csr -CA ca.pem -CAkey ca.key \
  -CAcreateserial -extfile broker.ext -out broker.pem 2>/dev/null
openssl pkcs8 -topk8 -nocrypt -in broker.orig -out broker.key
cat broker.key broker.pem ca.pem > broker.keystore.pem

openssl req -newkey rsa:2048 -nodes -sha256 -subj "/CN=logweir" \
  -keyout client.orig -out client.csr 2>/dev/null
printf '%s\n' 'extendedKeyUsage=clientAuth' 'keyUsage=digitalSignature,keyEncipherment' > client.ext
openssl x509 -req -sha256 -days "$DAYS" -in client.csr -CA ca.pem -CAkey ca.key \
  -CAcreateserial -extfile client.ext -out client.pem 2>/dev/null
openssl pkcs8 -topk8 -nocrypt -in client.orig -out client.key
cat client.key client.pem ca.pem > client.keystore.pem

openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days "$DAYS" \
  -subj "/CN=logweir-e2e WRONG CA" -keyout /dev/null -out wrong-ca.pem 2>/dev/null

# PROD-01.3: a client identity the broker must refuse (signed by nobody it
# trusts), in the same PEM shapes as client.pem / client.key.
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days "$DAYS" \
  -subj "/CN=logweir-wrong-client" -keyout wrong-client.orig -out wrong-client.pem 2>/dev/null
openssl pkcs8 -topk8 -nocrypt -in wrong-client.orig -out wrong-client.key

rm -f ./*.csr ./*.srl ./*.ext ./*.orig broker.key broker.pem
chmod 0644 ./*.pem client.key wrong-client.key
chmod 0600 ca.key
# Written as root (docker-compose.yml): give the files to whoever owns /certs,
# the host user when `just e2e-up` created it, so a host-side cleanup can
# remove them.
owner="$(stat -c '%u:%g' /certs)"
[ "$owner" = "0:0" ] || chown "$owner" $want
for f in $want; do [ -s "$f" ] || { echo "auth certs: $f was not written" >&2; exit 1; }; done
openssl verify -CAfile ca.pem client.pem
echo "auth certs: generated in $(pwd): $want"
