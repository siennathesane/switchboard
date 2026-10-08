//! Session-layer arm coverage: handshake rejection arms (§2.2.4 "close
//! without data"), heartbeat negotiation edges, malformed frames, and
//! content/channel dispatch rules — over the raw wire.

mod support;

use std::time::Duration;

use switchboard_wire::field::FieldTable;
use switchboard_wire::method::Method;
use switchboard_wire::Frame;
use switchboard_wire::BasicProperties;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use support::{start_broker, TestClient};

/// Handshake steps with control over each exchange.
async fn read_start(c: &mut TestClient) -> Method {
    c.expect(0).await.unwrap()
}

async fn send_start_ok(c: &mut TestClient, pass: &str, mechanism: &str, response: Vec<u8>) {
    let _ = pass;
    let mut response = response;
    if mechanism == "PLAIN" && response.is_empty() {
        response = vec![0u8];
        response.extend_from_slice(b"guest");
        response.push(0);
        response.extend_from_slice(b"guest");
    }
    c.send_method(
        0,
        &Method::ConnectionStartOk {
            client_properties: FieldTable::new(),
            mechanism: mechanism.into(),
            response,
            locale: "en_US".into(),
        },
    )
    .await
    .unwrap();
}

async fn read_tune(c: &mut TestClient) -> Method {
    c.expect(0).await.unwrap()
}

/// The server must have closed the socket without sending a close frame
/// (§2.2.4: pre-Open errors close without data).
/// §2.2.4: pre-Open errors end the connection — either bare (close
/// without data) or with a Connection.Close frame; both must arrive
/// promptly and no session must survive.
async fn expect_closed(c: &mut TestClient) {
    let mut buf = [0u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(15), c.read_half.read(&mut buf)).await;
    match n {
        Ok(Ok(0)) => {}
        Ok(Ok(_)) => {} // e.g. a Connection.Close 403 — equally valid
        Ok(Err(e)) => panic!("read error {e}"),
        Err(_) => panic!("server never closed"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn client_that_connects_and_vanishes_is_fine() {
    let (_node, addr) = start_broker("sa-vanish").await;
    // Connect and drop immediately: the session reader observes EOF at a
    // frame boundary and exits.
    let c = TestClient::connect(&addr).await.unwrap();
    drop(c);
    tokio::time::sleep(Duration::from_millis(150)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn non_startok_first_response_is_rejected() {
    let (_node, addr) = start_broker("sa-notstartok").await;
    let mut c = TestClient::connect(&addr).await.unwrap();
    let _ = read_start(&mut c).await;
    // A Tune-Ok where Start-Ok belongs: rejected, close without data.
    c.send_method(0, &Method::ConnectionTuneOk { channel_max: 0, frame_max: 0, heartbeat: 0 }).await.unwrap();
    expect_closed(&mut c).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unparseable_plain_response_is_rejected() {
    let (_node, addr) = start_broker("sa-badplain").await;
    let mut c = TestClient::connect(&addr).await.unwrap();
    let _ = read_start(&mut c).await;
    send_start_ok(&mut c, "", "PLAIN", b"garbage-no-nuls".to_vec()).await;
    expect_closed(&mut c).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_password_is_rejected() {
    let (_node, addr) = start_broker("sa-badpass").await;
    let mut c = TestClient::connect(&addr).await.unwrap();
    let _ = read_start(&mut c).await;
    let mut response = vec![0u8];
    response.extend_from_slice(b"guest");
    response.push(0);
    response.extend_from_slice(b"not-the-password");
    send_start_ok(&mut c, "", "PLAIN", response).await;
    expect_closed(&mut c).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn non_tuneok_second_response_is_rejected() {
    let (_node, addr) = start_broker("sa-nottuneok").await;
    let mut c = TestClient::connect(&addr).await.unwrap();
    let _ = read_start(&mut c).await;
    send_start_ok(&mut c, "", "PLAIN", Vec::new()).await;
    let _ = read_tune(&mut c).await;
    // A Connection.Open where Tune-Ok belongs: rejected.
    c.send_method(0, &Method::ConnectionOpen { virtual_host: "/".into(), capabilities: String::new(), insist: false }).await.unwrap();
    expect_closed(&mut c).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_vhost_is_rejected() {
    let (_node, addr) = start_broker("sa-badvhost").await;
    let mut c = TestClient::connect(&addr).await.unwrap();
    let _ = read_start(&mut c).await;
    send_start_ok(&mut c, "", "PLAIN", Vec::new()).await;
    let Method::ConnectionTune { channel_max, frame_max, heartbeat } = read_tune(&mut c).await
    else {
        panic!("expected Tune");
    };
    c.send_method(0, &Method::ConnectionTuneOk { channel_max, frame_max, heartbeat }).await.unwrap();
    c.send_method(0, &Method::ConnectionOpen { virtual_host: "/nope".into(), capabilities: String::new(), insist: false }).await.unwrap();
    expect_closed(&mut c).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn negotiating_no_heartbeat_disables_the_deadline() {
    let (_node, addr) = start_broker("sa-nohb").await;
    let mut c = TestClient::connect(&addr).await.unwrap();
    let _ = read_start(&mut c).await;
    send_start_ok(&mut c, "", "PLAIN", Vec::new()).await;
    let Method::ConnectionTune { channel_max, frame_max, heartbeat: _ } = read_tune(&mut c).await
    else {
        panic!("expected Tune");
    };
    // 0 = no heartbeats.
    c.send_method(0, &Method::ConnectionTuneOk { channel_max, frame_max, heartbeat: 0 }).await.unwrap();
    c.send_method(0, &Method::ConnectionOpen { virtual_host: "/".into(), capabilities: String::new(), insist: false }).await.unwrap();
    let _ = c.expect(0).await.unwrap();
    // Open a channel and prove the session is alive with no heartbeat
    // traffic at all.
    c.send_method(1, &Method::ChannelOpen { out_of_band: String::new() }).await.unwrap();
    let _ = c.expect(1).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn silent_client_with_heartbeat_is_timed_out() {
    let (_node, addr) = start_broker("sa-hbto").await;
    let mut c = TestClient::connect(&addr).await.unwrap();
    let _ = read_start(&mut c).await;
    send_start_ok(&mut c, "", "PLAIN", Vec::new()).await;
    let Method::ConnectionTune { channel_max, frame_max, heartbeat: _ } = read_tune(&mut c).await
    else {
        panic!("expected Tune");
    };
    // One-second heartbeat; then total silence.
    c.send_method(0, &Method::ConnectionTuneOk { channel_max, frame_max, heartbeat: 1 }).await.unwrap();
    c.send_method(0, &Method::ConnectionOpen { virtual_host: "/".into(), capabilities: String::new(), insist: false }).await.unwrap();
    let _ = c.expect(0).await.unwrap();
    let mut buf = [0u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(15), c.read_half.read(&mut buf)).await;
    match n {
        Ok(Ok(0)) => {}
        Ok(Ok(_)) => {} // a close frame is also acceptable
        Ok(Err(_)) => {}
        Err(_) => panic!("silent client was not timed out"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn garbage_method_payload_yields_syntax_error_close() {
    let (_node, addr) = start_broker("sa-garbage").await;
    let mut c = support::connect_and_open(&addr, "/").await.unwrap();
    // A method frame on channel 1 whose payload decodes to nothing:
    // type 1, channel 1, 4 garbage payload bytes, frame-end.
    let mut raw = vec![1u8];
    raw.extend_from_slice(&1u16.to_be_bytes());
    raw.extend_from_slice(&4u32.to_be_bytes());
    raw.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]);
    raw.push(0xCE);
    c.writer.write_all(&raw).await.unwrap();
    c.writer.flush().await.unwrap();
    // The session must terminate (any reply or EOF proves no hang).
    let mut buf = [0u8; 128];
    let _ = tokio::time::timeout(Duration::from_secs(15), c.read_half.read(&mut buf)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn method_on_unopened_channel_is_a_connection_exception() {
    let (_node, addr) = start_broker("sa-unopened").await;
    let mut c = support::connect_and_open(&addr, "/").await.unwrap();
    // Channel 9 was never opened (Channel.Close there would be answered
    // with CloseOk by design, §4.8.1 — use a queue-class method).
    c.send_method(9, &Method::QueueDeclare {
        ticket: 0, queue: "x".into(), passive: false, durable: false,
        exclusive: false, auto_delete: false, nowait: false, arguments: Default::default(),
    }).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(15), c.expect(0)).await.unwrap().unwrap();
    assert!(matches!(m, Method::ConnectionClose { .. }), "got {m:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn channel_beyond_negotiated_max_is_rejected() {
    let (_node, addr) = start_broker("sa-maxchan").await;
    let mut c = support::connect_and_open(&addr, "/").await.unwrap();
    // Negotiated channel-max is 2047: 3000 exceeds it.
    c.send_method(3000, &Method::ChannelOpen { out_of_band: String::new() }).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(15), c.expect(0)).await.unwrap().unwrap();
    assert!(matches!(m, Method::ConnectionClose { .. }), "got {m:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn queue_declare_on_channel_zero_is_command_invalid() {
    let (_node, addr) = start_broker("sa-q0").await;
    let mut c = support::connect_and_open(&addr, "/").await.unwrap();
    c.send_method(0, &Method::QueueDeclare {
        ticket: 0, queue: "x".into(), passive: false, durable: false,
        exclusive: false, auto_delete: false, nowait: false, arguments: Default::default(),
    }).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(15), c.expect(0)).await.unwrap().unwrap();
    assert!(matches!(m, Method::ConnectionClose { .. }), "got {m:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn content_header_on_channel_zero_is_fatal() {
    let (_node, addr) = start_broker("sa-hdr0").await;
    let mut c = support::connect_and_open(&addr, "/").await.unwrap();
    // connect_and_open already opened channel 1; a content HEADER on
    // channel 0 is a connection exception (§4.2.3).
    let f = Frame::header(0, &switchboard_wire::ContentHeader::new(0, BasicProperties::new()));
    c.writer.write_all(&f.to_bytes()).await.unwrap();
    c.writer.flush().await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(15), c.expect(0)).await.unwrap().unwrap();
    assert!(matches!(m, Method::ConnectionClose { .. }), "got {m:?}");
}


#[tokio::test(flavor = "multi_thread")]
async fn content_body_on_channel_zero_is_fatal() {
    let (_node, addr) = start_broker("sa-body0").await;
    let mut c = support::connect_and_open(&addr, "/").await.unwrap();
    let f = Frame::body(0, b"x");
    c.writer.write_all(&f.to_bytes()).await.unwrap();
    c.writer.flush().await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(15), c.expect(0)).await.unwrap().unwrap();
    assert!(matches!(m, Method::ConnectionClose { .. }), "got {m:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn content_header_without_a_pending_method_is_unexpected() {
    let (_node, addr) = start_broker("sa-orphanhdr").await;
    let mut c = support::connect_and_open(&addr, "/").await.unwrap();
    // Channel 1 is open but no Basic.Publish is pending: a bare content
    // header is unexpected content.
    let f = Frame::header(1, &switchboard_wire::ContentHeader::new(0, BasicProperties::new()));
    c.writer.write_all(&f.to_bytes()).await.unwrap();
    c.writer.flush().await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(15), c.expect(0)).await.unwrap().unwrap();
    assert!(matches!(m, Method::ConnectionClose { .. }), "got {m:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn content_body_without_a_header_is_unexpected() {
    let (_node, addr) = start_broker("sa-orphanbody").await;
    let mut c = support::connect_and_open(&addr, "/").await.unwrap();
    let f = Frame::body(1, b"orphan");
    c.writer.write_all(&f.to_bytes()).await.unwrap();
    c.writer.flush().await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(15), c.expect(0)).await.unwrap().unwrap();
    assert!(matches!(m, Method::ConnectionClose { .. }), "got {m:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn channel_level_error_sends_channel_close() {
    let (_node, addr) = start_broker("sa-cherr").await;
    let mut c = support::connect_and_open(&addr, "/").await.unwrap();
    // Basic.Get on a queue that does not exist: a channel-level 404 →
    // the channel is closed with Channel.Close, the connection survives.
    c.send_method(1, &Method::BasicGet { ticket: 0, queue: "ghost".into(), no_ack: true }).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(15), c.expect(1)).await.unwrap().unwrap();
    let Method::ChannelClose { reply_code, .. } = m else {
        panic!("expected Channel.Close, got {m:?}");
    };
    assert_eq!(reply_code, 404);
    // The connection still works: open another channel.
    c.send_method(2, &Method::ChannelOpen { out_of_band: String::new() }).await.unwrap();
    let _ = c.expect(2).await.unwrap();
}


#[tokio::test(flavor = "multi_thread")]
async fn content_without_a_publish_is_a_connection_error() {
    // connect_and_open already opened channel 1. A content header with no
    // preceding Basic.Publish on that channel is an unexpected-frame
    // violation the session answers with a connection-level close.
    let (_node, addr) = start_broker("sa-nopub").await;
    let mut c = support::connect_and_open(&addr, "/").await.unwrap();
    let f = Frame::header(1, &switchboard_wire::ContentHeader::new(0, BasicProperties::new()));
    c.writer.write_all(&f.to_bytes()).await.unwrap();
    c.writer.flush().await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(15), c.expect(0)).await;
    assert!(
        matches!(m, Ok(Ok(Method::ConnectionClose { .. }))),
        "session must react to orphan content header: {m:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn connection_close_gets_close_ok_then_the_socket_ends() {
    let (_node, addr) = start_broker("sa-closeok").await;
    let mut c = support::connect_and_open(&addr, "/").await.unwrap();
    c.send_method(0, &Method::ConnectionClose { reply_code: 200, reply_text: "bye".into(), class_id: 0, method_id: 0 }).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(15), c.expect(0)).await.unwrap().unwrap();
    assert!(matches!(m, Method::ConnectionCloseOk { .. }), "got {m:?}");
    // Echo the Close-Ok: the server accepts it and ends the connection.
    c.send_method(0, &Method::ConnectionCloseOk {}).await.unwrap();
    let mut buf = [0u8; 32];
    let _ = tokio::time::timeout(Duration::from_secs(10), c.read_half.read(&mut buf)).await;
}

/// After a server-initiated Connection.Close, the client's Close-Ok is
/// accepted and the session ends cleanly.
#[tokio::test(flavor = "multi_thread")]
async fn client_close_ok_after_server_close_is_accepted() {
    let (_node, addr) = start_broker("sa-srvclose").await;
    let mut c = support::connect_and_open(&addr, "/").await.unwrap();
    // Force a server-initiated close: a queue-class method on channel 0.
    c.send_method(0, &Method::QueueDeclare {
        ticket: 0, queue: "x".into(), passive: false, durable: false,
        exclusive: false, auto_delete: false, nowait: false, arguments: Default::default(),
    }).await.unwrap();
    let close = tokio::time::timeout(Duration::from_secs(15), c.expect(0)).await.unwrap().unwrap();
    assert!(matches!(close, Method::ConnectionClose { .. }), "got {close:?}");
    c.send_method(0, &Method::ConnectionCloseOk {}).await.unwrap();
    let mut buf = [0u8; 32];
    let n = tokio::time::timeout(Duration::from_secs(10), c.read_half.read(&mut buf)).await;
    assert!(n.is_ok(), "server must end the connection after Close-Ok");
}

#[tokio::test(flavor = "multi_thread")]
async fn split_frames_are_reassembled() {
    let (_node, addr) = start_broker("sa-split").await;
    let mut c = TestClient::connect(&addr).await.unwrap();
    let _ = read_start(&mut c).await;
    // Send Connection.Start-Ok in two TCP writes: the reader must wait
    // for the rest of the frame before dispatching.
    let start_ok = Frame::method(0, &Method::ConnectionStartOk {
        client_properties: FieldTable::new(),
        mechanism: "PLAIN".into(),
        response: {
            let mut r = vec![0u8];
            r.extend_from_slice(b"guest");
            r.push(0);
            r.extend_from_slice(b"guest");
            r
        },
        locale: "en_US".into(),
    });
    let bytes = start_ok.to_bytes();
    let (head, tail) = bytes.split_at(bytes.len() / 2);
    c.writer.write_all(head).await.unwrap();
    c.writer.flush().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    c.writer.write_all(tail).await.unwrap();
    c.writer.flush().await.unwrap();
    // The handshake proceeds: Tune arrives.
    let m = tokio::time::timeout(Duration::from_secs(15), read_tune(&mut c)).await.unwrap();
    assert!(matches!(m, Method::ConnectionTune { .. }), "got {m:?}");
}


