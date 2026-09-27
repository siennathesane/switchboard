//! Listener integration: `listener::run` (the real production entry)
//! serving the full protocol gateway — AMQP and HTTP health through it.

mod support;

use switchboard_server::ProtocolConfig;

use support::{free_port, start_broker_node};

#[tokio::test(flavor = "multi_thread")]
async fn listener_run_serves_amqp_clients() {
    let internal = format!("127.0.0.1:{}", free_port());
    let client = format!("127.0.0.1:{}", free_port());
    let (node, _listener_handle_unused) =
        start_broker_node("lrun", 1, client.clone(), internal, vec![], true, 1, vec![]).await;

    // The production listener: full gateway on its own port (the node's
    // configured client port is already bound by the test harness).
    let gateway = format!("127.0.0.1:{}", free_port());
    let listener = tokio::spawn({
        let node = node.clone();
        let gateway = gateway.clone();
        async move {
            switchboard_server::listener::run(
                node,
                &gateway,
                Default::default(),
                None,
                ProtocolConfig::all(),
            )
            .await
        }
    });

    // Wait for the bind.
    tokio::time::sleep(std::time::Duration::from_millis(700)).await;

    // AMQP client through the listener.
    let mut c = support::connect_and_open(&gateway, "/").await.unwrap();
    c.send_method(
        1,
        &Method::QueueDeclare {
            ticket: 0,
            queue: "via-listener".into(),
            passive: false,
            durable: true,
            exclusive: false,
            auto_delete: false,
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(c.expect(1).await.unwrap(), Method::QueueDeclareOk { .. }));

    // HTTP health through the same listener.
    let mut sock = tokio::net::TcpStream::connect(&gateway).await.unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    sock.write_all(b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n").await.unwrap();
    let mut buf = Vec::new();
    sock.read_to_end(&mut buf).await.unwrap();
    assert!(String::from_utf8_lossy(&buf).contains("200 OK"));

    listener.abort();
}

use switchboard_wire::method::Method;
use std::sync::Arc;

/// Grab a free TCP port (mirrors support's helper to avoid a pub-crate
/// dependency on test-only items).
#[allow(dead_code)]
fn _keep_import_used() {
    let _ = free_port();
}

#[tokio::test(flavor = "multi_thread")]
async fn listener_run_serves_amqp_over_tls() {
    use switchboard_server::tls::TlsIdentity;

    // Mint a self-signed identity for the listener.
    let names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    let key = rcgen::generate_simple_self_signed(names.clone()).unwrap();
    let identity = TlsIdentity {
        cert_pem: key.cert.pem().into_bytes(),
        key_pem: key.key_pair.serialize_pem().into_bytes(),
    };

    let internal = format!("127.0.0.1:{}", free_port());
    let client = format!("127.0.0.1:{}", free_port());
    let gateway = format!("127.0.0.1:{}", free_port());
    let (node, _unused) = start_broker_node(
        "tlsrun", 1, client.clone(), internal, vec![], true, 1, vec![],
    )
    .await;

    let gateway_task = tokio::spawn({
        let node = node.clone();
        let gateway = gateway.clone();
        async move {
            switchboard_server::listener::run(
                node,
                &gateway,
                Default::default(),
                Some(identity),
                ProtocolConfig::all(),
            )
            .await
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(700)).await;

    // TLS client that trusts the listener's cert.
    let tls = switchboard_server::tls::acceptor(&TlsIdentity {
        cert_pem: key.cert.pem().into_bytes(),
        key_pem: key.key_pair.serialize_pem().into_bytes(),
    })
    .unwrap();
    let _ = tls; // acceptor construction covered; connect as client below

    let roots = {
        let mut r = rustls::RootCertStore::empty();
        let der = key.cert.der().to_owned();
        r.add(der).unwrap();
        r
    };
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));

    let tcp = tokio::net::TcpStream::connect(&gateway).await.unwrap();
    let server_name = rustls::pki_types::ServerName::try_from("localhost".to_string()).unwrap();
    let mut tls_stream = connector.connect(server_name, tcp).await.expect("tls handshake");

    // AMQP handshake over TLS: protocol header + read Connection.Start.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    tls_stream.write_all(&switchboard_wire::PROTOCOL_HEADER).await.unwrap();
    tls_stream.flush().await.unwrap();
    let mut buf = [0u8; 512];
    let n = tokio::io::AsyncReadExt::read(&mut tls_stream, &mut buf).await.unwrap();
    assert!(n >= 11, "expected Connection.Start over TLS");
    // Decode: frame type 1 (method), channel 0 → Connection.Start.
    let mut fr = switchboard_wire::FrameReader::new();
    fr.feed(&buf[..n]);
    let frame = fr.next_frame(0).unwrap().expect("frame");
    let m = frame.decode_method().unwrap();
    assert!(matches!(m, Method::ConnectionStart { .. }), "got {m:?}");

    gateway_task.abort();
}
