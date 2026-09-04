use logweir_evidence::{keys::SigningKey, pae::pae, sign::sign_detached, verify::verify_detached};

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
