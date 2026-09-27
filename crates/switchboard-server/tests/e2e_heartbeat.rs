//! Session timing/heartbeat coverage: a client negotiating a short
//! heartbeat then going silent must be disconnected by the server after
//! two silent intervals (§4.2.7), and the writer must inject heartbeat
//! frames when the wire is quiet.

mod support;

use support::{free_port, start_broker_node};

#[tokio::test(flavor = "multi_thread")]
async fn heartbeat_timeout_disconnects_silent_client() {
    let internal = format!("127.0.0.1:{}", free_port());
    let client = format!("127.0.0.1:{}", free_port());
    let (node, _unused) = start_broker_node(
        "hb", 1, client.clone(), internal, vec![], true, 1, vec![],
    )
    .await;
    let _ = &node;

    let listener = tokio::net::TcpListener::bind(&format!("127.0.0.1:{}", free_port()))
        .await
        .unwrap();
    let laddr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else { continue };
            let node = node.clone();
            tokio::spawn(async move {
                let _ = switchboard_server::session::serve(
                    sock,
                    node,
                    switchboard_server::ConnectionLimits {
                        channel_max: 2047,
                        frame_max: 131_072,
                        heartbeat: 3,
                    },
                )
                .await;
            });
        }
    });

    // The shared-client handshake negotiates heartbeat=3 (echoes Tune).
    let mut c = support::connect_and_open(&laddr, "/").await.expect("open over hb listener");

    // Go silent: after 2 × 3s the server must close the connection.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        assert!(tokio::time::Instant::now() < deadline, "server never closed silent client");
        match c.expect(0).await {
            Ok(_) => continue, // drain any frames
            Err(_) => return,  // EOF: server closed the silent connection
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn server_sends_heartbeats_when_idle() {
    let internal = format!("127.0.0.1:{}", free_port());
    let client = format!("127.0.0.1:{}", free_port());
    let (node, _unused) = start_broker_node(
        "hbs", 1, client.clone(), internal, vec![], true, 1, vec![],
    )
    .await;
    let _ = &node;

    let listener = tokio::net::TcpListener::bind(&format!("127.0.0.1:{}", free_port()))
        .await
        .unwrap();
    let laddr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else { continue };
            let node = node.clone();
            tokio::spawn(async move {
                let _ = switchboard_server::session::serve(
                    sock,
                    node,
                    switchboard_server::ConnectionLimits {
                        channel_max: 2047,
                        frame_max: 131_072,
                        // A wide margin between the server's heartbeat
                        // send (1x) and its silence close (2x): at test
                        // tempo these are tens of milliseconds, and a
                        // busy machine can stall long enough for the
                        // close to win the race before a heartbeat was
                        // ever written.
                        heartbeat: 30,
                    },
                )
                .await;
            });
        }
    });

    let mut c = support::connect_and_open(&laddr, "/").await.expect("open over hb listener");

    // Idle: the server must inject a heartbeat frame (type 8) within ~3s.
    let mut saw_heartbeat = false;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    while !saw_heartbeat {
        assert!(tokio::time::Instant::now() < deadline, "no heartbeat frame observed");
        match c.reader.next_frame(0).unwrap() {
            Some(frame) => {
                saw_heartbeat |= frame.frame_type == switchboard_wire::FrameType::Heartbeat;
            }
            None => {
                // Pull octets from the socket into the frame reader.
                let mut buf = [0u8; 4096];
                let n = tokio::io::AsyncReadExt::read(&mut c.read_half, &mut buf)
                    .await
                    .unwrap();
                if n == 0 {
                    panic!("connection closed before heartbeat");
                }
                c.reader.feed(&buf[..n]);
            }
        }
    }
}
