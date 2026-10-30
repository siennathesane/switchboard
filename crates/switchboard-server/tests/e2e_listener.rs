//! Listener integration: `listener::run` (the real production entry)
//! serving the full protocol gateway — AMQP and HTTP health through it.

mod support;

use switchboard_server::ProtocolConfig;

use support::free_port;
use support::start_broker_node;

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
    assert!(matches!(
        c.expect(1).await.unwrap(),
        Method::QueueDeclareOk { .. }
    ));

    // HTTP health through the same listener.
    let mut sock = tokio::net::TcpStream::connect(&gateway).await.unwrap();
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    sock.write_all(b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    let mut buf = Vec::new();
    sock.read_to_end(&mut buf).await.unwrap();
    assert!(String::from_utf8_lossy(&buf).contains("200 OK"));

    listener.abort();
}

use std::sync::Arc;
use switchboard_wire::method::Method;

/// Grab a free TCP port (mirrors support's helper to avoid a pub-crate
/// dependency on test-only items).
#[allow(dead_code)]
fn _keep_import_used() {
    let _ = free_port();
}

/// Discover a non-loopback local IP without sending any packets: a UDP
/// `connect` only installs a default destination, and `local_addr` then
/// reports the source IP the kernel would pick for it.
fn non_loopback_ip() -> Option<std::net::IpAddr> {
    for target in ["8.8.8.8:80", "1.1.1.1:53", "[2001:4860:4860::8888]:53"] {
        let Ok(sock) = std::net::UdpSocket::bind("0.0.0.0:0") else {
            continue;
        };
        if sock.connect(target).is_ok() {
            if let Ok(a) = sock.local_addr() {
                if !a.ip().is_loopback() {
                    return Some(a.ip());
                }
            }
        }
    }
    None
}

/// Alert #9 e2e: the production listener stamps `limits.peer_ip` from the
/// accepted socket, so the guest-loopback rule must fire on REAL
/// connections — a non-loopback `guest` login is refused, a loopback one
/// still works. Both halves go through `listener::run`, not a test harness.
#[tokio::test(flavor = "multi_thread")]
async fn listener_run_stamps_peer_ip_for_guest_loopback_rule() {
    let internal = format!("127.0.0.1:{}", free_port());
    let client = format!("127.0.0.1:{}", free_port());
    let (node, _unused) =
        start_broker_node("peergw", 1, client.clone(), internal, vec![], true, 1, vec![]).await;

    // All-interface bind so the same listener is reachable both via
    // loopback and via the host's non-loopback address.
    let port = free_port();
    let gateway = format!("0.0.0.0:{port}");
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
    tokio::time::sleep(std::time::Duration::from_millis(700)).await;

    // Loopback half: `guest`/`guest` through the real listener succeeds.
    let loopback = format!("127.0.0.1:{port}");
    let c = support::connect_and_open(&loopback, "/").await.expect("loopback guest login");
    drop(c);

    // Non-loopback half: same credentials from the host's LAN address are
    // refused — Start-Ok gets the socket closed with no further data
    // (§2.2.4), i.e. EOF instead of Connection.Tune.
    match non_loopback_ip() {
        Some(ip) => {
            let target = format!("{ip}:{port}");
            let mut c = support::TestClient::connect(&target).await.unwrap();
            assert!(
                matches!(c.expect(0).await.unwrap(), Method::ConnectionStart { .. }),
                "expected Connection.Start from the real listener"
            );
            let mut response = vec![0u8];
            response.extend_from_slice(b"guest");
            response.push(0);
            response.extend_from_slice(b"guest");
            c.send_method(
                0,
                &Method::ConnectionStartOk {
                    client_properties: Default::default(),
                    mechanism: "PLAIN".into(),
                    response,
                    locale: "en_US".into(),
                },
            )
            .await
            .unwrap();
            let next = c.expect(0).await;
            assert!(
                next.is_err(),
                "non-loopback guest must be refused, got {next:?}"
            );
        }
        None => {
            eprintln!("skipping non-loopback half: host has no non-loopback interface");
        }
    }

    listener.abort();
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
        "tlsrun",
        1,
        client.clone(),
        internal,
        vec![],
        true,
        1,
        vec![],
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
    let mut tls_stream = connector
        .connect(server_name, tcp)
        .await
        .expect("tls handshake");

    // AMQP handshake over TLS: protocol header + read Connection.Start.
    use tokio::io::AsyncWriteExt;
    tls_stream
        .write_all(&switchboard_wire::PROTOCOL_HEADER)
        .await
        .unwrap();
    tls_stream.flush().await.unwrap();
    let mut buf = [0u8; 512];
    let n = tokio::io::AsyncReadExt::read(&mut tls_stream, &mut buf)
        .await
        .unwrap();
    assert!(n >= 11, "expected Connection.Start over TLS");
    // Decode: frame type 1 (method), channel 0 → Connection.Start.
    let mut fr = switchboard_wire::FrameReader::new();
    fr.feed(&buf[..n]);
    let frame = fr.next_frame(0).unwrap().expect("frame");
    let m = frame.decode_method().unwrap();
    assert!(matches!(m, Method::ConnectionStart { .. }), "got {m:?}");

    gateway_task.abort();
}
