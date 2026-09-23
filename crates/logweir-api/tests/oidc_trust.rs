//! Chart gap G1: the console trusts a PRIVATE CA for its OIDC issuer, and only
//! the way it was told to.
//!
//! THE POSITIVE ROW IS A REAL TLS HANDSHAKE. A loopback HTTPS server presents a
//! certificate no public CA issued — minted in this file, at run time, with a
//! key that never leaves the process — and the console's own
//! `HyperHttpClient` reads a discovery document from it. The two negative
//! controls around it are what make it a proof: the SAME client over the
//! system roots alone refuses the same server, and the same client with the
//! bundle refuses a URL naming a host the certificate does not cover. So the
//! positive row can only pass BECAUSE the bundle was added (the "bundle
//! ignored" mutant turns it red), and adding it widened who may issue the
//! certificate and nothing else.
//!
//! No certificate or key is checked in: GitHub push protection scans for
//! key-shaped literals, and a fixture minted here cannot go stale.
//!
//! The socket is a `127.0.0.1` listener this test binds itself; nothing dials
//! off the loopback interface.

use std::io::{Read as _, Write as _};
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use logweir_api::auth::oidc::{HttpClient as _, HyperHttpClient, TlsTrust};

// ------------------------------------------------------------------ DER

fn tlv(tag: u8, content: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let len = content.len();
    if len < 0x80 {
        out.push(len as u8);
    } else if len <= 0xff {
        out.extend([0x81, len as u8]);
    } else {
        out.extend([0x82, (len >> 8) as u8, len as u8]);
    }
    out.extend_from_slice(content);
    out
}

fn seq(parts: &[Vec<u8>]) -> Vec<u8> {
    tlv(0x30, &parts.concat())
}

fn name(common_name: &str) -> Vec<u8> {
    // Name ::= SEQUENCE OF SET OF AttributeTypeAndValue { id-at-commonName, UTF8String }
    let cn = seq(&[
        tlv(0x06, &[0x55, 0x04, 0x03]),
        tlv(0x0c, common_name.as_bytes()),
    ]);
    seq(&[tlv(0x31, &cn)])
}

fn bit_string(bytes: &[u8]) -> Vec<u8> {
    let mut content = vec![0x00];
    content.extend_from_slice(bytes);
    tlv(0x03, &content)
}

const ECDSA_WITH_SHA256: [u8; 8] = [0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x02];
const EC_PUBLIC_KEY: [u8; 7] = [0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01];
const PRIME256V1: [u8; 8] = [0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07];
const SUBJECT_ALT_NAME: [u8; 3] = [0x55, 0x1d, 0x11];

/// A self-signed P-256 certificate for `127.0.0.1` and its PKCS#8 key.
///
/// Self-signed on purpose: a certificate that is its own trust anchor is the
/// smallest private PKI there is, and WebPKI verifies it exactly as it would a
/// leaf under a private root — issuer lookup in the root store, signature,
/// validity window, then the name.
struct Minted {
    cert_der: Vec<u8>,
    key_pkcs8: Vec<u8>,
}

fn mint(common_name: &str) -> Minted {
    use ring::signature::{EcdsaKeyPair, KeyPair as _, ECDSA_P256_SHA256_ASN1_SIGNING};
    let rng = ring::rand::SystemRandom::new();
    let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, &rng)
        .expect("ring generates a P-256 key");
    let key = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, pkcs8.as_ref(), &rng)
        .expect("the generated PKCS#8 document parses");
    let algorithm = seq(&[tlv(0x06, &ECDSA_WITH_SHA256)]);
    let spki = seq(&[
        seq(&[tlv(0x06, &EC_PUBLIC_KEY), tlv(0x06, &PRIME256V1)]),
        bit_string(key.public_key().as_ref()),
    ]);
    // subjectAltName: iPAddress [7] 127.0.0.1, and nothing else.
    let san = seq(&[tlv(0x87, &[127, 0, 0, 1])]);
    let extensions = tlv(
        0xa3,
        &seq(&[seq(&[tlv(0x06, &SUBJECT_ALT_NAME), tlv(0x04, &san)])]),
    );
    let tbs = seq(&[
        tlv(0xa0, &tlv(0x02, &[0x02])), // version v3
        tlv(0x02, &[0x01]),             // serialNumber
        algorithm.clone(),
        name(common_name),
        seq(&[tlv(0x17, b"200101000000Z"), tlv(0x17, b"491231235959Z")]),
        name(common_name),
        spki,
        extensions,
    ]);
    let signature = key.sign(&rng, &tbs).expect("ring signs the TBS");
    Minted {
        cert_der: seq(&[tbs, algorithm, bit_string(signature.as_ref())]),
        key_pkcs8: pkcs8.as_ref().to_vec(),
    }
}

fn pem(label: &str, der: &[u8]) -> String {
    use base64::Engine as _;
    let body = base64::engine::general_purpose::STANDARD.encode(der);
    let mut out = format!("-----BEGIN {label}-----\n");
    for chunk in body.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(chunk).unwrap());
        out.push('\n');
    }
    out.push_str(&format!("-----END {label}-----\n"));
    out
}

// --------------------------------------------------------------- server

/// A loopback HTTPS server presenting `minted`, answering every request with
/// a fixed JSON document. Returns its port. The thread lives until the test
/// process exits; every stream it accepts carries a five-second deadline.
fn serve(minted: &Minted) -> u16 {
    let _ = weirkeeper::install_default_crypto_provider();
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![rustls::pki_types::CertificateDer::from(
                minted.cert_der.clone(),
            )],
            rustls::pki_types::PrivateKeyDer::Pkcs8(minted.key_pkcs8.clone().into()),
        )
        .expect("rustls accepts the minted certificate and key");
    let config = Arc::new(config);
    let listener = TcpListener::bind("127.0.0.1:0").expect("a loopback port");
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(tcp) = stream else { continue };
            let _ = tcp.set_read_timeout(Some(Duration::from_secs(5)));
            let _ = tcp.set_write_timeout(Some(Duration::from_secs(5)));
            let Ok(connection) = rustls::ServerConnection::new(Arc::clone(&config)) else {
                continue;
            };
            let mut tls = rustls::StreamOwned::new(connection, tcp);
            let mut request = Vec::new();
            let mut buf = [0u8; 1024];
            // A client that refuses the certificate ends the handshake here,
            // and the read fails; that is the negative rows' outcome.
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                match tls.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => request.extend_from_slice(&buf[..n]),
                }
            }
            if !request.windows(4).any(|w| w == b"\r\n\r\n") {
                continue;
            }
            let body = br#"{"served":"over the private CA"}"#;
            let head = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\
                 connection: close\r\n\r\n",
                body.len()
            );
            let _ = tls.write_all(head.as_bytes());
            let _ = tls.write_all(body);
            let _ = tls.flush();
            tls.conn.send_close_notify();
            let _ = tls.flush();
        }
    });
    port
}

async fn fetch(trust: &TlsTrust, url: &str) -> Result<Vec<u8>, String> {
    let client = HyperHttpClient::new(false, trust)?;
    tokio::time::timeout(Duration::from_secs(20), client.get(url))
        .await
        .map_err(|_| "the test's own deadline expired".to_string())?
}

// ----------------------------------------------------------------- rows

#[tokio::test]
async fn a_private_ca_in_the_bundle_is_trusted_beside_the_system_roots() {
    let minted = mint("logweir-test-private-idp");
    let port = serve(&minted);
    let bundle = pem("CERTIFICATE", &minted.cert_der);
    let url = format!("https://127.0.0.1:{port}/.well-known/openid-configuration");

    // THE POSITIVE ROW: system roots kept (the default), the bundle added.
    let trust = TlsTrust::from_pem(bundle.as_bytes(), true).expect("the bundle parses");
    assert_eq!(trust.extra_root_count(), 1);
    assert!(trust.uses_system_roots());
    let body = fetch(&trust, &url)
        .await
        .expect("the provider's certificate chains to the bundle's anchor");
    assert_eq!(body, br#"{"served":"over the private CA"}"#);

    // THE NEGATIVE CONTROL THAT MAKES IT A PROOF: the same client over the
    // system roots alone refuses the same server. Were the bundle ignored,
    // the positive row above would fail exactly like this one.
    let refused = fetch(&TlsTrust::system(), &url)
        .await
        .expect_err("no system root issued a certificate minted a moment ago");
    // Refused FOR THE RIGHT REASON: the issuer, not a dead socket.
    assert!(
        refused.contains("certificate") && refused.contains("UnknownIssuer"),
        "{refused}"
    );
}

#[tokio::test]
async fn the_bundle_alone_is_trusted_when_the_system_roots_are_dropped() {
    let minted = mint("logweir-test-private-idp-only");
    let port = serve(&minted);
    let bundle = pem("CERTIFICATE", &minted.cert_der);
    let trust = TlsTrust::from_pem(bundle.as_bytes(), false).expect("the bundle parses");
    assert!(!trust.uses_system_roots());
    assert_eq!(
        trust.root_store().expect("a store").len(),
        1,
        "`systemRoots: false` is the bundle and nothing else"
    );
    let url = format!("https://127.0.0.1:{port}/jwks");
    fetch(&trust, &url)
        .await
        .expect("the bundle alone is enough for a provider it issued");
}

#[tokio::test]
async fn the_bundle_widens_who_may_issue_and_not_what_is_checked() {
    let minted = mint("logweir-test-private-idp-name");
    let port = serve(&minted);
    let bundle = pem("CERTIFICATE", &minted.cert_der);
    let trust = TlsTrust::from_pem(bundle.as_bytes(), false).expect("the bundle parses");
    // The certificate names 127.0.0.1 and nothing else. `localhost` reaches
    // the same socket and must be refused: the host name is still verified.
    let url = format!("https://localhost:{port}/.well-known/openid-configuration");
    let refused = fetch(&trust, &url)
        .await
        .expect_err("a certificate for 127.0.0.1 does not cover `localhost`");
    // The anchor was accepted; the NAME was not.
    assert!(
        refused.contains("certificate not valid for name"),
        "{refused}"
    );
}

#[test]
fn a_bundle_that_would_add_nothing_or_carries_a_key_is_refused() {
    let minted = mint("logweir-test-refusals");
    let cert = pem("CERTIFICATE", &minted.cert_der);

    let empty = TlsTrust::from_pem(b"", true).expect_err("an empty file adds no anchor");
    assert!(empty.contains("no PEM CERTIFICATE block"), "{empty}");

    let prose = TlsTrust::from_pem(b"this is not a certificate\n", true)
        .expect_err("text with no PEM block adds no anchor");
    assert!(prose.contains("no PEM CERTIFICATE block"), "{prose}");

    // A private key beside a good certificate: refused by name, never skipped.
    let with_key = format!("{cert}{}", pem("PRIVATE KEY", &minted.key_pkcs8));
    let key = TlsTrust::from_pem(with_key.as_bytes(), true)
        .expect_err("a private key is a secret in the wrong place");
    assert!(
        key.contains("PrivateKey") && key.contains("block 2"),
        "{key}"
    );

    // A CERTIFICATE block whose body is not a certificate.
    let junk = pem("CERTIFICATE", b"\x30\x03\x02\x01\x01");
    let bad = TlsTrust::from_pem(junk.as_bytes(), true)
        .expect_err("a block rustls cannot use as an anchor is refused, not skipped");
    assert!(bad.contains("not a usable trust anchor"), "{bad}");

    let broken = "-----BEGIN CERTIFICATE-----\nnot base64 at all\n-----END CERTIFICATE-----\n";
    assert!(TlsTrust::from_pem(broken.as_bytes(), true).is_err());

    // Two good certificates are two anchors.
    let other = pem("CERTIFICATE", &mint("logweir-test-second").cert_der);
    let two = TlsTrust::from_pem(format!("{cert}{other}").as_bytes(), false).unwrap();
    assert_eq!(two.extra_root_count(), 2);
    assert_eq!(two.root_store().unwrap().len(), 2);
}

#[test]
fn an_unreadable_bundle_refuses_by_path() {
    let dir = std::env::temp_dir().join(format!(
        "logweir-oidc-trust-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let missing = dir.join("absent.crt");
    let refused = TlsTrust::load(Some(&missing), true).expect_err("fail closed");
    assert!(
        refused.contains("cannot read the OIDC CA bundle") && refused.contains("absent.crt"),
        "{refused}"
    );
    let empty = dir.join("empty.crt");
    std::fs::write(&empty, b"").unwrap();
    let refused = TlsTrust::load(Some(&empty), true).expect_err("an empty file adds nothing");
    assert!(refused.contains("empty.crt"), "{refused}");

    let no_bundle = TlsTrust::load(None, true).expect("no bundle is the system roots");
    assert_eq!(no_bundle.extra_root_count(), 0);
    assert!(no_bundle.uses_system_roots());
    let _ = std::fs::remove_dir_all(&dir);
}

/// THE WIRING, end to end: a configuration file naming `oidc.caBundleFile`,
/// read by the same `preflight` `main` runs before any socket, yields the
/// trust the client then handshakes with. A preflight that dropped the path
/// (`TlsTrust::load(None, …)`) passes every row above and fails this one.
#[tokio::test]
async fn the_configured_bundle_reaches_the_client_through_the_preflight() {
    let minted = mint("logweir-test-configured-idp");
    let port = serve(&minted);
    let dir = std::env::temp_dir().join(format!(
        "logweir-oidc-wiring-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    for file in ["session.key.yaml", "cursor.key.yaml"] {
        std::fs::write(dir.join(file), format!("version: 1\nkey: \"{key}\"\n")).unwrap();
    }
    std::fs::write(dir.join("client.secret"), "a-client-secret\n").unwrap();
    std::fs::write(dir.join("ca.crt"), pem("CERTIFICATE", &minted.cert_der)).unwrap();
    let ui = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../ui");
    let text = format!(
        "mode: shared\nlisten: \"127.0.0.1:1\"\npublicBaseUrl: \"https://console.example\"\n\
         uiDirectory: {ui}\n\
         oidc:\n  issuer: https://idp.example/realms/logweir\n  clientId: logweir-console\n  \
         clientSecretFile: client.secret\n  caBundleFile: ca.crt\n\
         roles:\n  revision: r1\n  bindings:\n  - role: viewer\n    namespace: team-a\n    \
         groups: [lw-viewers]\n\
         sessionKey:\n  file: session.key.yaml\n  expectedVersion: 1\n\
         cursorKey:\n  file: cursor.key.yaml\n  expectedVersion: 1\n\
         namespaces: [team-a]\nkubernetes:\n  source: inCluster\n",
        ui = ui.display(),
    );
    let config = logweir_api::config::Config::parse(&text, &dir).expect("the file validates");
    let oidc = &config.shared().expect("shared mode").oidc;
    assert_eq!(
        oidc.ca_bundle_file.as_deref(),
        Some(dir.join("ca.crt").as_path())
    );
    assert!(
        oidc.system_roots,
        "the system roots are kept unless the file says otherwise"
    );
    let preflight = logweir_api::preflight(&config).expect("the preflight reads every file");
    let trust = preflight.shared.expect("shared material").tls_trust;
    assert_eq!(trust.extra_root_count(), 1);
    let url = format!("https://127.0.0.1:{port}/.well-known/openid-configuration");
    fetch(&trust, &url)
        .await
        .expect("the configured bundle is the anchor the client verifies with");
    let _ = std::fs::remove_dir_all(&dir);
}
