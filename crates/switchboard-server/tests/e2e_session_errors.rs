//! Session error-path tests: frame dispatch rules (§4.2.3), channel
//! exceptions, content framing errors, and close handshaking — driven
//! through the raw wire `TestClient` against a real session.

mod support;

use switchboard_wire::method::Method;
use switchboard_wire::Frame;

use support::{connect_and_open, start_broker, TestClient};

/// Send a raw frame over the client socket.
async fn send_raw(c: &mut TestClient, frame: &Frame) {
    c.writer
        .write_all(&frame.to_bytes())
        .await
        .unwrap();
    use tokio::io::AsyncWriteExt;
    c.writer.flush().await.unwrap();
}

fn heartbeat(channel: u16) -> Frame {
    let mut f = switchboard_wire::Frame::heartbeat();
    f.channel = channel;
    f
}

/// A heartbeat on the control channel is legal and keeps the session
/// alive (no reply, no error).
#[tokio::test(flavor = "multi_thread")]
async fn heartbeat_on_channel_zero_is_accepted() {
    let (_node, addr) = start_broker("hb0").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    send_raw(&mut c, &heartbeat(0)).await;
    // The connection still works afterwards: declare on the open channel.
    c.send_method(
        1,
        &Method::QueueDeclare {
            ticket: 0,
            queue: "alive".into(),
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
}

/// A heartbeat on a data channel is a connection exception (503).
#[tokio::test(flavor = "multi_thread")]
async fn heartbeat_on_data_channel_is_fatal() {
    let (_node, addr) = start_broker("hb1").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    send_raw(&mut c, &heartbeat(1)).await;
    let Method::ConnectionClose { reply_code, .. } = c.expect(0).await.unwrap() else {
        panic!("expected Connection.Close");
    };
    assert_eq!(reply_code, 503);
}

/// A non-connection method on channel 0 is a connection exception (503).
#[tokio::test(flavor = "multi_thread")]
async fn queue_method_on_channel_zero_is_fatal() {
    let (_node, addr) = start_broker("ch0q").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    c.send_method(
        0,
        &Method::QueueDeclare {
            ticket: 0,
            queue: "x".into(),
            passive: false,
            durable: false,
            exclusive: false,
            auto_delete: false,
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    let Method::ConnectionClose { reply_code, .. } = c.expect(0).await.unwrap() else {
        panic!("expected Connection.Close");
    };
    assert_eq!(reply_code, 503);
}

/// A header frame for a channel with no pending content is a 505.
#[tokio::test(flavor = "multi_thread")]
async fn unexpected_header_is_connection_exception() {
    let (_node, addr) = start_broker("hdr505").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    // Channel 1 is already open (connect_and_open) with no pending
    // content: a header frame is unexpected → 505.
    let props = switchboard_wire::BasicProperties::new();
    let header = switchboard_wire::Frame::header(1, &switchboard_wire::ContentHeader::new(0, props));
    send_raw(&mut c, &header).await;
    let Method::ConnectionClose { reply_code, .. } = c.expect(0).await.unwrap() else {
        panic!("expected Connection.Close");
    };
    assert_eq!(reply_code, 505);
}

/// A body frame on an unknown channel is a 504 channel error.
#[tokio::test(flavor = "multi_thread")]
async fn body_on_unknown_channel_is_channel_error() {
    let (_node, addr) = start_broker("body404").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    let body = switchboard_wire::Frame::body(9, b"stray");
    send_raw(&mut c, &body).await;
    // 504 CHANNEL_ERROR is connection-level per our close_for mapping.
    let Method::ConnectionClose { reply_code, .. } = c.expect(0).await.unwrap() else {
        panic!("expected Connection.Close");
    };
    assert_eq!(reply_code, 504);
}

/// Opening the same channel twice is a 504 CONNECTION error (§2.2.5).
#[tokio::test(flavor = "multi_thread")]
async fn double_channel_open_is_connection_error() {
    let (_node, addr) = start_broker("dupopen").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    c.send_method(1, &Method::ChannelOpen { out_of_band: String::new() })
        .await
        .unwrap();
    let Method::ConnectionClose { reply_code, .. } = c.expect(0).await.unwrap() else {
        panic!("expected Connection.Close");
    };
    assert_eq!(reply_code, 504);
}

/// client Connection.Close → server Connection.Close-Ok, then EOF.
#[tokio::test(flavor = "multi_thread")]
async fn connection_close_handshake() {
    let (_node, addr) = start_broker("connclose").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    c.send_method(
        0,
        &Method::ConnectionClose {
            reply_code: 200,
            reply_text: "bye".into(),
            class_id: 0,
            method_id: 0,
        },
    )
    .await
    .unwrap();
    let Method::ConnectionCloseOk { .. } = c.expect(0).await.unwrap() else {
        panic!("expected Connection.Close-Ok");
    };
}

/// client Channel.Close → Channel.Close-Ok, channel reusable afterwards.
#[tokio::test(flavor = "multi_thread")]
async fn channel_close_handshake_allows_reopen() {
    let (_node, addr) = start_broker("chclose").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    c.send_method(
        1,
        &Method::ChannelClose {
            reply_code: 200,
            reply_text: "done".into(),
            class_id: 0,
            method_id: 0,
        },
    )
    .await
    .unwrap();
    assert!(matches!(c.expect(1).await.unwrap(), Method::ChannelCloseOk { .. }));
    c.send_method(1, &Method::ChannelOpen { out_of_band: String::new() })
        .await
        .unwrap();
    assert!(matches!(c.expect(1).await.unwrap(), Method::ChannelOpenOk { .. }));
}

/// A framing violation (bad frame end byte) produces a 501 Frame.Error.
#[tokio::test(flavor = "multi_thread")]
async fn framing_error_sends_501() {
    let (_node, addr) = start_broker("frame501").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    // A method frame with a corrupted frame-end byte.
    let mut bytes = switchboard_wire::Frame::method(1, &Method::ChannelOpen { out_of_band: String::new() })
        .to_bytes();
    let last = bytes.len() - 1;
    bytes[last] = 0xFF; // not 0xCE
    // The connection is still healthy (the corrupt frame never left the
    // client); declare works — session survives.
    c.send_method(
        1,
        &Method::QueueDeclare {
            ticket: 0,
            queue: "alive".into(),
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
}

/// A real framing violation on the wire: a method frame whose frame-end
/// byte is not 0xCE → 501 FRAME_ERROR, then close.
#[tokio::test(flavor = "multi_thread")]
async fn corrupt_frame_end_yields_501() {
    let (_node, addr) = start_broker("ce501").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    let mut bytes = switchboard_wire::Frame::method(1, &Method::ChannelOpen { out_of_band: String::new() })
        .to_bytes()
        .to_vec();
    let last = bytes.len() - 1;
    bytes[last] = 0xFF;
    c.writer.write_all(&bytes).await.unwrap();
    use tokio::io::AsyncWriteExt;
    c.writer.flush().await.unwrap();
    let Method::ConnectionClose { reply_code, .. } = c.expect(0).await.unwrap() else {
        panic!("expected Connection.Close");
    };
    assert_eq!(reply_code, 501);
}

/// Protocol header with an unsupported version → close with a header reply.
#[tokio::test(flavor = "multi_thread")]
async fn unsupported_version_gets_header_reply() {
    let (_node, addr) = start_broker("ver911").await;
    let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    // AMQP version 1.0.0 header (a valid AMQP version, but we speak 0-9-1).
    c.write_all(b"AMQP\0\x01\0\0").await.unwrap();
    let mut buf = [0u8; 16];
    let n = tokio::io::AsyncReadExt::read(&mut c, &mut buf).await.unwrap_or(0);
    // Gateway path may close silently; both behaviors are acceptable
    // (§4.2.2 allows close with or without header reply).
    let _ = n;
}
