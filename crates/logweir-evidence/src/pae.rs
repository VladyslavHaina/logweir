/// DSSE v1 Pre-Authentication Encoding.
///   PAE(type, body) = "DSSEv1" SP LEN(type) SP type SP LEN(body) SP body
/// LEN is the ASCII decimal byte count. Length-prefixing is what makes the
/// encoding injective, which is the whole security property (see the
/// `pae_is_unambiguous_across_a_shifted_boundary` test).
pub fn pae(payload_type: &str, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + payload_type.len() + 32);
    out.extend_from_slice(b"DSSEv1 ");
    out.extend_from_slice(payload_type.len().to_string().as_bytes());
    out.push(b' ');
    out.extend_from_slice(payload_type.as_bytes());
    out.push(b' ');
    out.extend_from_slice(payload.len().to_string().as_bytes());
    out.push(b' ');
    out.extend_from_slice(payload);
    out
}
