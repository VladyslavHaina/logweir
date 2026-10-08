#!/usr/bin/env python3
"""The owner's countersigning step, run by a PACKAGED binary (PROD-14.0).

A Governed restore waits for an approver who runs `logweir drill countersign`
on their own machine, with the CLI from the release archive for that machine
and a key that never leaves it (docs/kubernetes.md, *Approval policy*). The
release cannot hold that key, so this script proves the shipped binary can
perform the step with THROWAWAY keys instead:

1. it mints two P-256 keys, a stand-in console and a stand-in approver, in a
   temporary directory that is deleted on exit;
2. it writes a Governed authorization document v2 and the console's DSSE
   sidecar over it;
3. it runs `<logweir> drill countersign --document … --confirmation … --key …
   --out …`, the exact command an approver runs;
4. it checks the result with code that is NOT Logweir's: the console's
   signature is kept as it was, exactly one signature is added, under the
   approver's key id, and it verifies over the DSSE PAE of the UNCHANGED
   document bytes (`docs/verify_scorecard.py`'s `pae`, `key_id` and
   `verify_signature`, the independent verifier's own primitives);
5. it requires two refusals: the console's own key cannot countersign its
   confirmation, and an Ordinary document has nothing to countersign.

Nothing is published and no cluster is contacted. Every subprocess has a
timeout.

    release-countersign-check.py <path to the packaged logweir binary>
"""

import base64
import hashlib
import importlib.util
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import subprocess
import sys
import tempfile
import uuid

ROOT = Path(__file__).resolve().parent.parent
PAYLOAD_TYPE = "application/vnd.logweir.restore-authorization+json;version=2.0.0"
REQUESTER = {"issuer": "urn:logweir:local-admin", "subject": "release-requester"}


def load_verifier():
    spec = importlib.util.spec_from_file_location("verify_scorecard", ROOT / "docs/verify_scorecard.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


VS = load_verifier()
from cryptography.hazmat.primitives import hashes, serialization  # noqa: E402  (after the verifier's own import check)
from cryptography.hazmat.primitives.asymmetric import ec  # noqa: E402


def fail(message):
    print(f"FAIL: {message}", file=sys.stderr)
    sys.exit(1)


def rfc3339(instant):
    return instant.strftime("%Y-%m-%dT%H:%M:%SZ")


def document(mode):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    doc = {
        "formatVersion": "2.0.0",
        "kind": "RestoreAuthorization",
        "authorizationMode": mode,
        "subject": {
            "apiVersion": "logweir.dev/v1alpha1",
            "kind": "Restore",
            "namespace": "release-drill",
            "name": "release-countersign",
            "uid": str(uuid.uuid4()),
        },
        "planHash": "sha256:" + hashlib.sha256(b"release-countersign plan").hexdigest(),
        "requester": REQUESTER,
        "policy": {
            "name": "release-governed",
            "digest": "sha256:" + hashlib.sha256(b"release-countersign policy").hexdigest(),
        },
        "issuedAt": rfc3339(now),
        "expiresAt": rfc3339(now + timedelta(hours=1)),
        "ticket": "REL-COUNTERSIGN",
    }
    return json.dumps(doc, separators=(",", ":")).encode()


def pem(key):
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def sign(key, doc_bytes):
    signature = key.sign(VS.pae(PAYLOAD_TYPE, doc_bytes), ec.ECDSA(hashes.SHA256()))
    return {"keyid": VS.key_id(key.public_key()), "sig": base64.b64encode(signature).decode()}


def countersign(binary, work, doc_bytes, confirmation, key, name):
    paths = {part: work / f"{name}.{part}" for part in ("document", "confirmation", "key", "out")}
    paths["document"].write_bytes(doc_bytes)
    paths["confirmation"].write_text(json.dumps(confirmation))
    paths["key"].write_bytes(pem(key))
    paths["key"].chmod(0o600)
    result = subprocess.run(
        [str(binary), "drill", "countersign",
         "--document", str(paths["document"]), "--confirmation", str(paths["confirmation"]),
         "--key", str(paths["key"]), "--out", str(paths["out"])],
        capture_output=True, text=True, timeout=60,
    )
    return result, paths


def main(argv):
    if len(argv) != 2:
        print(__doc__.strip().splitlines()[-1].strip(), file=sys.stderr)
        return 2
    binary = Path(argv[1]).resolve()
    if not binary.is_file():
        fail(f"{binary} is not a file")
    with tempfile.TemporaryDirectory(prefix="logweir-release-countersign-") as tmp:
        work = Path(tmp)
        console = ec.generate_private_key(ec.SECP256R1())
        approver = ec.generate_private_key(ec.SECP256R1())

        # THE STEP ITSELF.
        governed = document("Governed")
        confirmation = {"payloadType": PAYLOAD_TYPE, "signatures": [sign(console, governed)]}
        result, paths = countersign(binary, work, governed, confirmation, approver, "governed")
        if result.returncode != 0:
            fail(f"drill countersign exited {result.returncode}: {result.stdout}{result.stderr}")
        requester = f"{REQUESTER['issuer']}#{REQUESTER['subject']}"
        if requester not in result.stdout:
            fail(f"the summary does not name the requester {requester}: {result.stdout}")
        if paths["document"].read_bytes() != governed:
            fail("countersign changed the document it signed")
        sidecar = json.loads(paths["out"].read_text())
        if sidecar.get("payloadType") != PAYLOAD_TYPE:
            fail(f"the countersigned sidecar names payloadType {sidecar.get('payloadType')!r}")
        signatures = sidecar.get("signatures")
        if not isinstance(signatures, list) or len(signatures) != 2:
            fail(f"expected the console's signature and exactly one more, got {signatures!r}")
        if signatures[0] != confirmation["signatures"][0]:
            fail("the console's signature was not kept as it was")
        message = VS.pae(PAYLOAD_TYPE, governed)
        added = signatures[1]
        if added.get("keyid") != VS.key_id(approver.public_key()):
            fail(f"the added signature claims key {added.get('keyid')}, not the approver's")
        added_sig = base64.b64decode(added["sig"])
        if not VS.verify_signature(approver.public_key(), message, added_sig):
            fail("the approver's signature does not verify over the document's DSSE PAE")
        if not VS.verify_signature(console.public_key(), message, base64.b64decode(signatures[0]["sig"])):
            fail("the console's signature no longer verifies")
        # The check above must be able to fail: the same signature under the
        # OTHER key, and over other bytes, is refused by the same primitive.
        if VS.verify_signature(console.public_key(), message, added_sig):
            fail("the verifier accepted the approver's signature under the console's key")
        if VS.verify_signature(approver.public_key(), VS.pae(PAYLOAD_TYPE, governed + b" "), added_sig):
            fail("the verifier accepted the approver's signature over different bytes")
        print(f"ok: {binary.name} drill countersign added key {added['keyid'][:16]}… over the unchanged "
              "document; both signatures verify independently")

        # REFUSAL 1: the console's own key is not a second, independent signer.
        result, paths = countersign(binary, work, governed, confirmation, console, "self")
        if result.returncode != 1 or "already signed" not in result.stdout + result.stderr:
            fail(f"a countersignature by the console's own key was not refused (exit {result.returncode}): "
                 f"{result.stdout}{result.stderr}")
        if paths["out"].exists():
            fail("a refused countersignature still wrote a sidecar")
        print("ok: the console's own key is refused (already signed)")

        # REFUSAL 2: an Ordinary document needs no approver.
        ordinary = document("Ordinary")
        result, paths = countersign(binary, work, ordinary,
                                    {"payloadType": PAYLOAD_TYPE, "signatures": [sign(console, ordinary)]},
                                    approver, "ordinary")
        if result.returncode != 1 or "Ordinary" not in result.stdout + result.stderr:
            fail(f"an Ordinary document was countersigned (exit {result.returncode}): {result.stdout}{result.stderr}")
        if paths["out"].exists():
            fail("a refused Ordinary countersignature still wrote a sidecar")
        print("ok: an Ordinary document is refused")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
