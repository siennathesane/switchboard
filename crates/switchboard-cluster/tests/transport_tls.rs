//! TLS transport tests: a self-signed certificate is generated at test
//! time; the server presents it and the client pins it as its only trust
//! anchor. Covers the rustls (aws-lc-rs) accept/connect paths of the
//! internal protocol and the client-listener TLS arm.

use std::sync::Arc;

use rcgen::{generate_simple_self_signed, CertifiedKey};

/// Mint a self-signed cert for `server_name` with SAN entries; returns
/// (cert_pem, key_pem, der).
fn self_signed(names: &[String]) -> (Vec<u8>, Vec<u8>, rustls::pki_types::CertificateDer<'static>) {
    let CertifiedKey { cert, key_pair } =
        generate_simple_self_signed(names.to_vec()).expect("cert generation");
    let cert_der = cert.der().to_owned();
    (cert.pem().into_bytes(), key_pair.serialize_pem().into_bytes(), cert_der)
}

#[tokio::test(flavor = "multi_thread")]
async fn cluster_transport_tls_roundtrip() {
    use switchboard_cluster::transport::{tls_acceptor, PeerChannel};

    let names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    let (cert_pem, key_pem, cert_der) = self_signed(&names);

    // Server side: acceptor from the generated identity.
    let acceptor = tls_acceptor(&cert_pem, &key_pem).expect("acceptor");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let mut accepted = switchboard_cluster::transport::accept(sock, Some(&acceptor))
            .await
            .expect("tls accept");
        let frame = accepted.read_frame_full().await.unwrap();
        accepted.write_frame(&frame).await.unwrap();
    });

    // Client side: trust ONLY the generated cert (product helper).
    let _ = cert_der;
    let connector = switchboard_cluster::transport::tls_connector(&cert_pem).expect("connector");
    let channel = PeerChannel::new(Some(connector), "localhost".into());

    let mut conn = channel.connect(&addr).await.expect("tls connect");
    let _ = &cert_pem;
    conn.send(b"secret-over-tls").await.unwrap();
    let back = conn.recv().await.unwrap();
    assert_eq!(back, b"secret-over-tls");
    server.await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn cluster_transport_tls_client_rejects_unknown_ca() {
    use switchboard_cluster::transport::{tls_acceptor, PeerChannel};

    // The server presents a cert the client does NOT trust.
    let names = vec!["localhost".to_string()];
    let (cert_pem, key_pem, _untrusted) = self_signed(&names);
    let acceptor = tls_acceptor(&cert_pem, &key_pem).expect("acceptor");

    // The client trusts a DIFFERENT self-signed cert.
    let (_other_pem, _other_key, _other_der) =
        self_signed(&vec!["other".to_string()]);
    let connector = switchboard_cluster::transport::tls_connector(&_other_pem).expect("connector");
    let channel = PeerChannel::new(Some(connector), "localhost".into());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let _ = switchboard_cluster::transport::accept(sock, Some(&acceptor)).await;
    });

    // The TLS handshake must FAIL: the presented cert chains to an
    // unknown root.
    let err = channel.connect(&addr).await.err().expect("handshake must fail");
    assert!(
        err.to_string().contains("certificate") || err.kind() == std::io::ErrorKind::InvalidData,
        "unexpected error: {err}"
    );
    server.await.unwrap();
}

use switchboard_cluster::transport::PeerChannel;
use switchboard_cluster::transport::PeerConn;

#[tokio::test(flavor = "multi_thread")]
async fn tls_peer_channel_send_recv_with_self_signed_trust() {
    use switchboard_cluster::transport::{tls_acceptor};

    let names = vec!["localhost".to_string()];
    let (cert_pem, key_pem, cert_der) = self_signed(&names);
    let acceptor = tls_acceptor(&cert_pem, &key_pem).unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    // Two frames exchanged back-to-back over one TLS connection.
    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let mut accepted = switchboard_cluster::transport::accept(sock, Some(&acceptor))
            .await
            .unwrap();
        let f1 = accepted.read_frame_full().await.unwrap();
        accepted.write_frame(&f1).await.unwrap();
        let f2 = accepted.read_frame_full().await.unwrap();
        accepted.write_frame(&f2).await.unwrap();
    });

    let client_config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_root_certificates({
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(cert_der)
            .unwrap();
        roots
    })
    .with_no_client_auth();
    let channel = PeerChannel::new(
        Some(tokio_rustls::TlsConnector::from(Arc::new(client_config))),
        "localhost".into(),
    );

    let mut conn: PeerConn = channel.connect(&addr).await.unwrap();
    conn.send(b"one").await.unwrap();
    assert_eq!(conn.recv().await.unwrap(), b"one");
    conn.send(b"two").await.unwrap();
    assert_eq!(conn.recv().await.unwrap(), b"two");
    server.await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_internal_frames_are_rejected() {
    // A length prefix beyond the internal frame limit: the peer closes
    // the connection instead of buffering forever.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let mut accepted = switchboard_cluster::transport::accept(sock, None)
            .await
            .expect("accept");
        // The Debug form distinguishes plain and TLS accepts.
        let dbg = format!("{accepted:?}");
        assert!(dbg.starts_with("Accepted("), "{dbg}");
        // Reading the oversized frame errors out.
        let r = accepted.read_frame_full().await;
        assert!(r.is_err(), "oversized frame must be rejected");
    });
    // Raw socket (no framing helper): the test writes the length prefix
    // itself.
    use tokio::io::AsyncWriteExt;
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    // Announce an absurd frame: 0x7FFF_FFFF bytes (LE length prefix).
    sock.write_all(&[0xFF, 0xFF, 0xFF, 0x7F]).await.unwrap();
    sock.flush().await.unwrap();
    // Keep the socket open; the server hangs up.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let _ = sock.shutdown().await;
    server.await.unwrap();
}
