use logweir_evidence::{
    keys::SigningKey, pae::pae, sign::sign_detached, verify::verify_detached, Error,
};

/// DSSE v1 PAE: "DSSEv1" SP LEN(type) SP type SP LEN(payload) SP payload,
/// with LEN in ASCII decimal. This is the whole construction; it is ~40 lines
/// and must not be "improved".
#[test]
fn pae_matches_the_dsse_v1_vector() {
    let got = pae("http://example.com/HelloWorld", b"hello world");
    assert_eq!(
        got,
        b"DSSEv1 29 http://example.com/HelloWorld 11 hello world".to_vec()
    );
}

#[test]
fn pae_is_unambiguous_across_a_shifted_boundary() {
    // A length-prefixed encoding must not let a payload masquerade as a type.
    assert_ne!(pae("ab", b"c"), pae("a", b"bc"));
}

#[test]
fn p256_round_trips_and_a_tampered_byte_fails() {
    let sk = SigningKey::generate_p256();
    let vk = sk.verifying_key();
    let payload = br#"{"format_version":"1.0.0"}"#;
    let side = sign_detached(&sk, logweir_evidence::PAYLOAD_TYPE_SCORECARD, payload).unwrap();

    verify_detached(
        &vk,
        logweir_evidence::PAYLOAD_TYPE_SCORECARD,
        payload,
        &side,
    )
    .unwrap();

    let mut tampered = payload.to_vec();
    tampered[2] ^= 0x01;
    assert!(
        verify_detached(
            &vk,
            logweir_evidence::PAYLOAD_TYPE_SCORECARD,
            &tampered,
            &side
        )
        .is_err(),
        "a single flipped byte must fail verification"
    );
}

#[test]
fn ed25519_round_trips() {
    let sk = SigningKey::generate_ed25519();
    let vk = sk.verifying_key();
    let payload = b"{}";
    let side = sign_detached(&sk, logweir_evidence::PAYLOAD_TYPE_SCORECARD, payload).unwrap();
    verify_detached(
        &vk,
        logweir_evidence::PAYLOAD_TYPE_SCORECARD,
        payload,
        &side,
    )
    .unwrap();
}

#[test]
fn a_changed_payload_type_fails_even_with_identical_bytes() {
    let sk = SigningKey::generate_p256();
    let vk = sk.verifying_key();
    let payload = b"{}";
    let side = sign_detached(&sk, logweir_evidence::PAYLOAD_TYPE_SCORECARD, payload).unwrap();
    assert!(verify_detached(&vk, "application/json", payload, &side).is_err());
}

#[test]
fn key_id_is_stable_and_is_the_sha256_of_the_spki_der() {
    let sk = SigningKey::generate_p256();
    assert_eq!(sk.key_id(), sk.key_id());
    assert_eq!(sk.key_id().len(), 64);
}

/// verify_detached must fail closed via the keyid-match guard (verify.rs's
/// trailing "no signature by key ... in the sidecar" branch) when the
/// presented key never matches any signature's keyid — not fall through to a
/// crypto check with the wrong key.
#[test]
fn a_signature_does_not_verify_under_a_different_key() {
    let sk = SigningKey::generate_p256();
    let other_sk = SigningKey::generate_p256();
    let wrong_vk = other_sk.verifying_key();
    let payload = b"{}";
    let side = sign_detached(&sk, logweir_evidence::PAYLOAD_TYPE_SCORECARD, payload).unwrap();

    let err = verify_detached(
        &wrong_vk,
        logweir_evidence::PAYLOAD_TYPE_SCORECARD,
        payload,
        &side,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("no signature by key"),
        "a mismatched key must fail via the keyid-match guard, not a crypto check: {err}"
    );
}

/// Same guard, reached via an empty `signatures` list rather than a
/// mismatched keyid: a sidecar with no signatures at all must not verify.
#[test]
fn an_empty_signature_list_fails() {
    let sk = SigningKey::generate_p256();
    let vk = sk.verifying_key();
    let side = logweir_evidence::Sidecar {
        payload_type: logweir_evidence::PAYLOAD_TYPE_SCORECARD.to_string(),
        signatures: vec![],
    };
    assert!(verify_detached(&vk, logweir_evidence::PAYLOAD_TYPE_SCORECARD, b"{}", &side).is_err());
}

// --- Deferred finding: malformed data must not be reported as tampering ----
//
// `Error::Verify` is the crypto engine giving a definite negative answer
// (a well-formed signature that does not match, or no signature by this
// key). `Error::Malformed` is corruption of the sidecar itself — bytes that
// never reached a crypto check at all. A caller mapping these onto an
// exit-code contract needs to tell "this file is corrupt" apart from "this
// document was tampered with", so the two must never collapse to the same
// variant.

#[test]
fn base64_that_will_not_decode_is_malformed_not_verify() {
    let sk = SigningKey::generate_p256();
    let vk = sk.verifying_key();
    let payload = b"{}";
    let mut side = sign_detached(&sk, logweir_evidence::PAYLOAD_TYPE_SCORECARD, payload).unwrap();
    // '!' is outside the standard base64 alphabet.
    side.signatures[0].sig = "!!!not base64!!!".to_string();

    let err = verify_detached(
        &vk,
        logweir_evidence::PAYLOAD_TYPE_SCORECARD,
        payload,
        &side,
    )
    .unwrap_err();
    assert!(
        matches!(err, Error::Malformed(_)),
        "undecodable base64 must be Malformed, got: {err:?}"
    );
}

#[test]
fn a_p256_signature_that_is_not_valid_der_is_malformed_not_verify() {
    use base64::{engine::general_purpose::STANDARD as B64, Engine};
    let sk = SigningKey::generate_p256();
    let vk = sk.verifying_key();
    let payload = b"{}";
    let mut side = sign_detached(&sk, logweir_evidence::PAYLOAD_TYPE_SCORECARD, payload).unwrap();
    // Valid base64, but not a valid DER ECDSA signature (truncated/garbage).
    side.signatures[0].sig = B64.encode([0xDE, 0xAD, 0xBE, 0xEF]);

    let err = verify_detached(
        &vk,
        logweir_evidence::PAYLOAD_TYPE_SCORECARD,
        payload,
        &side,
    )
    .unwrap_err();
    assert!(
        matches!(err, Error::Malformed(_)),
        "invalid DER must be Malformed, not a crypto verification failure, got: {err:?}"
    );
}

#[test]
fn an_ed25519_signature_of_the_wrong_length_is_malformed_not_verify() {
    use base64::{engine::general_purpose::STANDARD as B64, Engine};
    let sk = SigningKey::generate_ed25519();
    let vk = sk.verifying_key();
    let payload = b"{}";
    let mut side = sign_detached(&sk, logweir_evidence::PAYLOAD_TYPE_SCORECARD, payload).unwrap();
    // Ed25519 signatures are exactly 64 bytes; truncate to simulate a
    // truncated file rather than a deliberately re-signed one.
    let raw = B64.decode(&side.signatures[0].sig).unwrap();
    side.signatures[0].sig = B64.encode(&raw[..raw.len() - 1]);

    let err = verify_detached(
        &vk,
        logweir_evidence::PAYLOAD_TYPE_SCORECARD,
        payload,
        &side,
    )
    .unwrap_err();
    assert!(
        matches!(err, Error::Malformed(_)),
        "a wrong-length signature must be Malformed, not a crypto verification failure, got: {err:?}"
    );
}

#[test]
fn a_genuinely_tampered_payload_is_verify_not_malformed() {
    // The structural/crypto split's other half: a well-formed signature over
    // the WRONG bytes is exactly the "tampered" fact, and must stay Verify.
    let sk = SigningKey::generate_p256();
    let vk = sk.verifying_key();
    let payload = b"{\"a\":1}";
    let side = sign_detached(&sk, logweir_evidence::PAYLOAD_TYPE_SCORECARD, payload).unwrap();

    let tampered = b"{\"a\":2}";
    let err = verify_detached(
        &vk,
        logweir_evidence::PAYLOAD_TYPE_SCORECARD,
        tampered,
        &side,
    )
    .unwrap_err();
    assert!(
        matches!(err, Error::Verify(_)),
        "a well-formed signature over tampered bytes must stay Verify, got: {err:?}"
    );
}

#[test]
fn payload_type_mismatch_is_malformed_not_verify() {
    let sk = SigningKey::generate_p256();
    let vk = sk.verifying_key();
    let payload = b"{}";
    let side = sign_detached(&sk, logweir_evidence::PAYLOAD_TYPE_SCORECARD, payload).unwrap();
    let err = verify_detached(&vk, "application/json", payload, &side).unwrap_err();
    assert!(
        matches!(err, Error::Malformed(_)),
        "a payload_type mismatch is a data-shape problem, not tamper evidence, got: {err:?}"
    );
}

#[test]
fn no_signature_by_the_presented_key_is_verify_not_malformed() {
    // The "no matching keyid" branch is a definite cryptographic fact (this
    // key never signed this document), so it stays Verify — distinct from
    // structural corruption of a signature that WAS addressed to this key.
    let sk = SigningKey::generate_p256();
    let other_sk = SigningKey::generate_p256();
    let wrong_vk = other_sk.verifying_key();
    let payload = b"{}";
    let side = sign_detached(&sk, logweir_evidence::PAYLOAD_TYPE_SCORECARD, payload).unwrap();
    let err = verify_detached(
        &wrong_vk,
        logweir_evidence::PAYLOAD_TYPE_SCORECARD,
        payload,
        &side,
    )
    .unwrap_err();
    assert!(
        matches!(err, Error::Verify(_)),
        "no signature by the presented key must stay Verify, got: {err:?}"
    );
}
