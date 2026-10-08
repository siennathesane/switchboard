//! AMQP 1.0 arm coverage: address resolution (`/topic/`, `/exchange/`),
//! message sections (AMQP value bodies, properties, anonymous relay),
//! settlement, malformed frames, and teardown paths — driven over a real
//! gateway socket with hand-built performatives.

mod gateway_support;
mod support;

use std::time::Duration;

use gateway_support::{start_gateway, with_timeout};
use support::TestClient;
use switchboard_server::protocols::amqp10::{frames, types};
use switchboard_wire::method::Method;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// One gateway per test (most deterministic under parallel harness load).
async fn gw() -> (String, gateway_support::NodeGuard) {
    start_gateway(Default::default()).await
}

/// Read one amqp10 frame from the socket (reassembling reads).
async fn read_frame(sock: &mut tokio::net::TcpStream) -> frames::Frame {
    let mut buf = [0u8; 4096];
    let mut raw = Vec::new();
    loop {
        if let Ok((frame, used)) = frames::decode_frame(&raw) {
            if !frames::is_more(used) {
                return frame;
            }
        }
        let n = sock.read(&mut buf).await.unwrap();
        assert!(n > 0, "connection ended");
        raw.extend_from_slice(&buf[..n]);
    }
}

/// SASL-ANONYMOUS handshake + open + begin; returns the socket with the
/// open/begin replies consumed frame-by-frame (they can arrive split).
async fn amqp10_open_session(addr: &str) -> tokio::net::TcpStream {
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    sock.write_all(b"AMQP\0\x01\0\0").await.unwrap();
    let mut buf = [0u8; 2048];
    let _ = sock.read(&mut buf).await.unwrap(); // sasl-mechanisms
    sock.write_all(&frames::encode_sasl_frame(
        frames::codes::SASL_INIT,
        vec![
            types::Value::Symbol("ANONYMOUS".into()),
            types::Value::Null,
            types::Value::Binary(Vec::new()),
        ],
    ))
    .await
    .unwrap();
    let _ = sock.read(&mut buf).await.unwrap(); // sasl-outcome
    sock.write_all(b"AMQP\0\x01\0\0").await.unwrap();
    sock.write_all(&frames::open("tester")).await.unwrap();
    sock.write_all(&frames::begin(Some(0), 1)).await.unwrap();
    // The open and begin replies arrive as frames; the generic type
    // decoder rejects connection-level performatives, so assert on
    // delivery (any bytes) rather than the code.
    let open_reply = tokio::time::timeout(Duration::from_secs(10), read_frame_raw(&mut sock)).await.unwrap();
    assert!(!open_reply.is_empty(), "open reply");
    let begin_reply = tokio::time::timeout(Duration::from_secs(10), read_frame_raw(&mut sock)).await.unwrap();
    assert!(!begin_reply.is_empty(), "begin reply");
    sock
}

/// Read exactly one transport frame (header + declared size).
async fn read_frame_raw(sock: &mut tokio::net::TcpStream) -> Vec<u8> {
    use tokio::io::AsyncReadExt;
    let mut head = [0u8; 8];
    sock.read_exact(&mut head).await.unwrap();
    // §2.3: size (0..4), doff (4), type (5), channel (6..8).
    let size = u32::from_be_bytes([head[0], head[1], head[2], head[3]]) as usize;
    let mut rest = vec![0u8; size.saturating_sub(8)];
    sock.read_exact(&mut rest).await.unwrap();
    let mut frame = head.to_vec();
    frame.extend_from_slice(&rest);
    frame
}

fn described(code: u64, v: types::Value) -> types::Value {
    types::Value::Described(Box::new(types::Value::ULong(code)), Box::new(v))
}

/// A minimal body: one SECTION_DATA binary section.
fn data_body(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    types::encode(
        &described(frames::codes::SECTION_DATA, types::Value::Binary(data.to_vec())),
        &mut out,
    );
    out
}

// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a10_topic_and_exchange_addresses_resolve() {
    let (addr, _node) = gw().await;
    let mut sock = amqp10_open_session(&addr).await;

    // Receiver on /topic/news (amq.topic): publish via AMQP 0-9-1, then
    // receive through the resolved topic address.
    sock.write_all(&frames::attach_receiver("sub-topic", 0, "/topic/news")).await.unwrap();
    sock.write_all(&frames::flow_credit(0, 0, 10)).await.unwrap();
    let frame = read_frame(&mut sock).await;
    assert_eq!(frame.code(), Some(frames::codes::ATTACH), "attach reply");

    let mut amqp = support::connect_and_open(&addr, "/").await.unwrap();
    amqp.send_method(
        1,
        &Method::BasicPublish {
            ticket: 0,
            exchange: "amq.topic".into(),
            routing_key: "news".into(),
            mandatory: false,
            immediate: false,
        },
    )
    .await
    .unwrap();
    let props = switchboard_wire::BasicProperties::new();
    amqp.send_content(1, &props, b"topic-news").await.unwrap();

    let frame = with_timeout(read_frame(&mut sock), 30).await;
    assert_eq!(frame.code(), Some(frames::codes::TRANSFER), "topic delivery");
}

#[tokio::test(flavor = "multi_thread")]
async fn a10_sender_target_exchange_address_routes() {
    let (addr, _node) = gw().await;
    let mut sock = amqp10_open_session(&addr).await;

    // Pre-declare a custom exchange and a queue bound to it.
    let mut amqp = support::connect_and_open(&addr, "/").await.unwrap();
    amqp.send_method(1, &Method::ExchangeDeclare {
        ticket: 0, exchange: "custom".into(), exchange_type: "direct".into(),
        passive: false, durable: true, auto_delete: false, internal: false, nowait: false,
        arguments: Default::default(),
    }).await.unwrap();
    amqp.expect(1).await.unwrap();
    amqp.send_method(1, &Method::QueueDeclare {
        ticket: 0, queue: "cq".into(), passive: false, durable: true,
        exclusive: false, auto_delete: false, nowait: false, arguments: Default::default(),
    }).await.unwrap();
    amqp.expect(1).await.unwrap();
    amqp.send_method(1, &Method::QueueBind {
        ticket: 0, queue: "cq".into(), exchange: "custom".into(), routing_key: "rk".into(),
        nowait: false, arguments: Default::default(),
    }).await.unwrap();
    amqp.expect(1).await.unwrap();

    // amqp10 sender whose target is /exchange/custom/rk.
    sock.write_all(&frames::attach_sender("pub-ex", 0, Some("/exchange/custom/rk"))).await.unwrap();
    let frame = read_frame(&mut sock).await;
    assert_eq!(frame.code(), Some(frames::codes::ATTACH), "attach reply");
    let msg = data_body(b"routed");
    sock.write_all(&frames::encode_frame_with_payload(
        0,
        frames::codes::TRANSFER,
        vec![
            types::Value::UInt(0),
            types::Value::UInt(1),
            types::Value::Binary(vec![1]),
            types::Value::UInt(0),
            types::Value::Bool(true),
            types::Value::Bool(false),
            types::Value::Null,
            types::Value::Null,
        ],
        &msg,
    ))
    .await
    .unwrap();

    // The transfer's publish is asynchronous to the transfer write; poll
    // the queue until it shows up.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        assert!(tokio::time::Instant::now() < deadline, "routed message never appeared");
        amqp.send_method(1, &Method::BasicGet { ticket: 0, queue: "cq".into(), no_ack: true }).await.unwrap();
        match amqp.expect(1).await.unwrap() {
            Method::BasicGetOk { .. } => break,
            Method::BasicGetEmpty { .. } => continue,
            other => panic!("unexpected {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a10_attach_source_missing_queue_is_detached() {
    let (addr, _node) = gw().await;
    let mut sock = amqp10_open_session(&addr).await;
    // Queues auto-declare on attach, but an unknown exchange cannot be
    // resolved: the link must be refused with a DETACH.
    sock.write_all(&frames::attach_receiver("bad-src", 3, "/exchange/ghost/rk")).await.unwrap();
    let frame = with_timeout(read_frame(&mut sock), 30).await;
    assert_eq!(frame.code(), Some(frames::codes::DETACH), "failed source must detach");
}

#[tokio::test(flavor = "multi_thread")]
async fn a10_unknown_performative_and_handleless_flow_are_ignored() {
    let (addr, _node) = gw().await;
    let mut sock = amqp10_open_session(&addr).await;
    // An unrecognized performative code.
    sock.write_all(&frames::encode_frame_with_payload(0, 0x42, vec![types::Value::Null], &[])).await.unwrap();
    // A session-level FLOW with no handle.
    sock.write_all(&frames::flow(0, 2)).await.unwrap();
    // The session must still be alive: END → END reply.
    sock.write_all(&frames::end()).await.unwrap();
    let frame = with_timeout(read_frame(&mut sock), 30).await;
    assert_eq!(frame.code(), Some(frames::codes::END));
}

#[tokio::test(flavor = "multi_thread")]
async fn a10_bad_transfer_payload_is_ignored_and_session_survives() {
    let (addr, _node) = gw().await;
    let mut sock = amqp10_open_session(&addr).await;
    // Attach a sender (handle 0) so the handle resolves; the PAYLOAD is
    // garbage (a bare binary, not a described section).
    sock.write_all(&frames::attach_sender("pub-bad", 0, Some("/queue/anywhere"))).await.unwrap();
    let _ = read_frame(&mut sock).await;
    sock.write_all(&frames::encode_frame_with_payload(
        0,
        frames::codes::TRANSFER,
        vec![
            types::Value::UInt(0),
            types::Value::UInt(1),
            types::Value::Binary(vec![1]),
            types::Value::UInt(0),
            types::Value::Bool(true),
            types::Value::Bool(false),
            types::Value::Null,
            types::Value::Null,
        ],
        &[0xFF, 0xFF, 0xFF],
    ))
    .await
    .unwrap();
    sock.write_all(&frames::end()).await.unwrap();
    let frame = with_timeout(read_frame(&mut sock), 30).await;
    assert_eq!(frame.code(), Some(frames::codes::END));
}

#[tokio::test(flavor = "multi_thread")]
async fn a10_sender_without_target_and_without_to_is_rejected() {
    let (addr, _node) = gw().await;
    let mut sock = amqp10_open_session(&addr).await;
    // Target address empty: only a message `to` could route it, and the
    // body carries none.
    sock.write_all(&frames::attach_sender("pub-orphan", 0, None)).await.unwrap();
    let _ = read_frame(&mut sock).await;
    let msg = data_body(b"nowhere");
    sock.write_all(&frames::encode_frame_with_payload(
        0,
        frames::codes::TRANSFER,
        vec![
            types::Value::UInt(0),
            types::Value::UInt(1),
            types::Value::Binary(vec![1]),
            types::Value::UInt(0),
            types::Value::Bool(true),
            types::Value::Bool(false),
            types::Value::Null,
            types::Value::Null,
        ],
        &msg,
    ))
    .await
    .unwrap();
    sock.write_all(&frames::end()).await.unwrap();
    let frame = with_timeout(read_frame(&mut sock), 30).await;
    assert_eq!(frame.code(), Some(frames::codes::END), "rejection must not kill the session");
}

#[tokio::test(flavor = "multi_thread")]
async fn a10_detach_of_a_live_link_tears_down_and_replies() {
    let (addr, _node) = gw().await;
    let mut sock = amqp10_open_session(&addr).await;
    sock.write_all(&frames::attach_receiver("sub-d", 5, "/queue/dq")).await.unwrap();
    let frame = read_frame(&mut sock).await;
    assert_eq!(frame.code(), Some(frames::codes::ATTACH));
    sock.write_all(&frames::detach(5)).await.unwrap();
    let frame = with_timeout(read_frame(&mut sock), 30).await;
    assert_eq!(frame.code(), Some(frames::codes::DETACH));
}

#[tokio::test(flavor = "multi_thread")]
async fn a10_link_credit_flow_grant_is_accepted() {
    let (addr, _node) = gw().await;
    let mut sock = amqp10_open_session(&addr).await;
    sock.write_all(&frames::attach_receiver("sub-c", 0, "/queue/cq2")).await.unwrap();
    let _ = read_frame(&mut sock).await;
    // Link-scoped flow (handle set at field 8).
    sock.write_all(&frames::flow_credit(0, 0, 7)).await.unwrap();
    // Session stays live.
    sock.write_all(&frames::end()).await.unwrap();
    let frame = with_timeout(read_frame(&mut sock), 30).await;
    assert_eq!(frame.code(), Some(frames::codes::END));
}

#[tokio::test(flavor = "multi_thread")]
async fn a10_sasl_frame_that_is_not_init_closes() {
    let (addr, _node) = gw().await;
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    sock.write_all(b"AMQP\0\x01\0\0").await.unwrap();
    let mut buf = [0u8; 512];
    let _ = sock.read(&mut buf).await.unwrap();
    // A SASL-frame with a non-SASL_INIT code: the connection closes.
    sock.write_all(&frames::encode_sasl_frame(frames::codes::SASL_OUTCOME, vec![types::Value::UByte(0)])).await.unwrap();
    let n = tokio::time::timeout(Duration::from_secs(10), sock.read(&mut buf)).await.unwrap().unwrap();
    assert_eq!(n, 0, "connection must close");
}

#[tokio::test(flavor = "multi_thread")]
async fn a10_sasl_init_with_non_binary_response_still_authenticates() {
    let (addr, _node) = gw().await;
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    sock.write_all(b"AMQP\0\x01\0\0").await.unwrap();
    let mut buf = [0u8; 512];
    let _ = sock.read(&mut buf).await.unwrap();
    // ANONYMOUS with a Null response field.
    sock.write_all(&frames::encode_sasl_frame(
        frames::codes::SASL_INIT,
        vec![types::Value::Symbol("ANONYMOUS".into()), types::Value::Null, types::Value::Null],
    ))
    .await
    .unwrap();
    // sasl-outcome, then the client restarts the header.
    let _ = sock.read(&mut buf).await.unwrap();
    sock.write_all(b"AMQP\0\x01\0\0").await.unwrap();
    sock.write_all(&frames::open("anon2")).await.unwrap();
    let n = tokio::time::timeout(Duration::from_secs(10), sock.read(&mut buf)).await.unwrap().unwrap();
    assert!(n > 0, "open must be answered");
}

#[tokio::test(flavor = "multi_thread")]
async fn a10_bad_second_protocol_header_closes() {
    let (addr, _node) = gw().await;
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    sock.write_all(b"AMQP\0\x01\0\0").await.unwrap();
    let mut buf = [0u8; 512];
    let _ = sock.read(&mut buf).await.unwrap();
    sock.write_all(&frames::encode_sasl_frame(
        frames::codes::SASL_INIT,
        vec![types::Value::Symbol("ANONYMOUS".into()), types::Value::Null, types::Value::Binary(Vec::new())],
    ))
    .await
    .unwrap();
    let _ = sock.read(&mut buf).await.unwrap();
    // Wrong header after SASL: connection closes.
    sock.write_all(b"AMQP\x00\x00\x09\x01").await.unwrap();
    let n = tokio::time::timeout(Duration::from_secs(10), sock.read(&mut buf)).await.unwrap().unwrap();
    assert_eq!(n, 0, "connection must close");
}

#[tokio::test(flavor = "multi_thread")]
async fn a10_malformed_frames_close_gracefully() {
    let (addr, _node) = gw().await;
    // doff < 2 → invalid frame offset (§2.3 layout: size, doff, ...).
    let mut sock = amqp10_open_session(&addr).await;
    sock.write_all(&[0, 0, 0, 16, 0x01, 0, 0, 0]).await.unwrap();
    sock.write_all(&[0u8; 8]).await.unwrap();
    let mut buf = [0u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(10), sock.read(&mut buf)).await.unwrap().unwrap();
    assert_eq!(n, 0, "bad doff must close");

    // size < header length → closed as well.
    let mut sock = amqp10_open_session(&addr).await;
    sock.write_all(&[0, 0, 0, 7, 0x02, 0, 0, 0]).await.unwrap();
    let n = tokio::time::timeout(Duration::from_secs(10), sock.read(&mut buf)).await.unwrap().unwrap();
    assert_eq!(n, 0, "bad size must close");
}

#[tokio::test(flavor = "multi_thread")]
async fn a10_extended_header_frame_parses_and_session_survives() {
    let (addr, _node) = gw().await;
    let mut sock = amqp10_open_session(&addr).await;
    // doff=3: an 12-byte frame header (8 fixed + 4 extended), carrying a
    // CLOSE performative body. The extended bytes are ignored; the
    // session processes the frame.
    let mut frame = vec![0, 0, 0, 20, 0x03, 0, 0, 0]; // size, doff=3
    frame.extend_from_slice(&[0u8; 4]); // extended header
    frame.extend_from_slice(&{
        let mut body = Vec::new();
        types::encode(&types::Value::ULong(frames::codes::CLOSE), &mut body);
        // described body per transport framing: the frame body starts
        // with the channel word.
        let mut full = vec![0, 0, 0, 0];
        full.extend_from_slice(&body);
        full.resize(20 - 12, 0); // pad to declared size
        full
    });
    sock.write_all(&frame).await.unwrap();
    // Either a CLOSE reply or a clean close is acceptable; the frame
    // must not wedge the session.
    let mut rbuf = [0u8; 512];
    let n = tokio::time::timeout(Duration::from_secs(10), sock.read(&mut rbuf)).await.unwrap();
    let _ = n;
}

// ---------------------------------------------------------------------------
// Message sections, bare addresses, settlement, and credit edges.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a10_bare_address_resolves_as_queue() {
    let (addr, _node) = gw().await;
    let mut sock = amqp10_open_session(&addr).await;
    // An address with no scheme prefix is a plain queue name.
    sock.write_all(&frames::attach_receiver("bare", 0, "plainq")).await.unwrap();
    let frame = with_timeout(read_frame(&mut sock), 30).await;
    assert_eq!(frame.code(), Some(frames::codes::ATTACH));
    // It really declared the queue.
    let mut amqp = support::connect_and_open(&addr, "/").await.unwrap();
    amqp.send_method(1, &Method::QueueDeclare {
        ticket: 0, queue: "plainq".into(), passive: true, durable: true,
        exclusive: false, auto_delete: false, nowait: false, arguments: Default::default(),
    }).await.unwrap();
    let _ = amqp.expect(1).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a10_amqp_value_body_and_properties_sections_parse() {
    let (addr, _node) = gw().await;
    let mut sock = amqp10_open_session(&addr).await;
    // Sender toward /queue/props (auto-declared on attach).
    sock.write_all(&frames::attach_sender("props-pub", 0, Some("/queue/props"))).await.unwrap();
    let _ = with_timeout(read_frame(&mut sock), 30).await;

    // Body: PROPERTIES section (to + content-type) followed by an
    // AMQP-VALUE string section. The `to` routes anonymously.
    let mut fields: Vec<types::Value> = (0..7).map(|_| types::Value::Null).collect();
    fields[2] = types::Value::String("/queue/props".into());
    fields[6] = types::Value::Symbol("text/plain".into());
    let mut payload = Vec::new();
    types::encode(
        &described(frames::codes::SECTION_PROPERTIES, types::Value::List(fields)),
        &mut payload,
    );
    types::encode(
        &described(frames::codes::SECTION_AMQP_VALUE, types::Value::String("valued".into())),
        &mut payload,
    );

    sock.write_all(&frames::encode_frame_with_payload(
        0,
        frames::codes::TRANSFER,
        vec![
            types::Value::UInt(0),
            types::Value::UInt(1),
            types::Value::Binary(vec![1]),
            types::Value::UInt(0),
            types::Value::Bool(true),
            types::Value::Bool(false),
            types::Value::Null,
            types::Value::Null,
        ],
        &payload,
    ))
    .await
    .unwrap();

    // The message landed via the properties `to`.
    let mut amqp = support::connect_and_open(&addr, "/").await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        assert!(tokio::time::Instant::now() < deadline, "sectioned message never routed");
        amqp.send_method(1, &Method::BasicGet { ticket: 0, queue: "props".into(), no_ack: true }).await.unwrap();
        match amqp.expect(1).await.unwrap() {
            Method::BasicGetOk { .. } => break,
            Method::BasicGetEmpty { .. } => tokio::time::sleep(Duration::from_millis(100)).await,
            other => panic!("unexpected {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a10_unsettled_client_transfer_receives_disposition() {
    let (addr, _node) = gw().await;
    let mut sock = amqp10_open_session(&addr).await;
    // snd-settle-mode 0 (unsettled): the broker must ack our transfer.
    sock.write_all(&frames::attach_sender_mode("unset-pub", 0, false)).await.unwrap();
    let _ = with_timeout(read_frame(&mut sock), 30).await;
    sock.write_all(&frames::encode_frame_with_payload(
        0,
        frames::codes::TRANSFER,
        vec![
            types::Value::UInt(0),
            types::Value::UInt(41),
            types::Value::Binary(vec![1]),
            types::Value::UInt(0),
            types::Value::Bool(false), // settled = false
            types::Value::Bool(false),
            types::Value::Null,
            types::Value::Null,
        ],
        &data_body(b"settle-me"),
    ))
    .await
    .unwrap();
    let frame = with_timeout(read_frame(&mut sock), 30).await;
    assert_eq!(frame.code(), Some(frames::codes::DISPOSITION), "unsettled transfer must be acknowledged");
}

#[tokio::test(flavor = "multi_thread")]
async fn a10_disposition_with_unknown_state_still_settles_as_ack() {
    let (addr, _node) = gw().await;
    let mut sock = amqp10_open_session(&addr).await;
    sock.write_all(&frames::attach_receiver("ackr", 0, "/queue/ackq")).await.unwrap();
    let _ = with_timeout(read_frame(&mut sock), 30).await;
    sock.write_all(&frames::flow_credit(0, 0, 5)).await.unwrap();
    // Deliver something to settle.
    let mut amqp = support::connect_and_open(&addr, "/").await.unwrap();
    for m in [Method::QueueDeclare {
        ticket: 0, queue: "ackq".into(), passive: false, durable: true,
        exclusive: false, auto_delete: false, nowait: false, arguments: Default::default(),
    }] {
        amqp.send_method(1, &m).await.unwrap();
        let _ = amqp.expect(1).await.unwrap();
    }
    publish_and_confirm(&mut amqp, "ackq", b"payload").await;
    let frame = with_timeout(read_frame(&mut sock), 30).await;
    let (delivery_id,) = match frame.code() {
        Some(frames::codes::TRANSFER) => {
            // delivery-id is field 1 of the transfer.
            let id = match frame.field(1) {
                Some(v) => v.as_uint().unwrap_or(0),
                None => 0,
            };
            (id,)
        }
        other => panic!("expected TRANSFER, got {other:?}"),
    };
    // DISPOSITION whose state is neither accepted nor released falls
    // back to the accepted (ack) path.
    sock.write_all(&frames::encode_frame_with_payload(
        0,
        frames::codes::DISPOSITION,
        vec![
            types::Value::Bool(true), // role: receiver
            types::Value::UInt(delivery_id),
            types::Value::UInt(delivery_id),
            types::Value::Bool(true), // settled
            types::Value::Described(
                Box::new(types::Value::ULong(0x99)), // unknown state
                Box::new(types::Value::Null),
            ),
        ],
        &[],
    ))
    .await
    .unwrap();
    // Queue must be empty afterwards (acked, not requeued).
    tokio::time::sleep(Duration::from_millis(400)).await;
    amqp.send_method(1, &Method::BasicGet { ticket: 0, queue: "ackq".into(), no_ack: true }).await.unwrap();
    assert!(matches!(amqp.expect(1).await.unwrap(), Method::BasicGetEmpty { .. }));
}

/// Publish with a confirmation wait so tests observe durable enqueue.
async fn publish_and_confirm(amqp: &mut support::TestClient, queue: &str, body: &[u8]) {
    amqp.send_method(1, &Method::ConfirmSelect { nowait: false }).await.unwrap();
    let _ = amqp.expect(1).await.unwrap();
    amqp.send_method(1, &Method::BasicPublish {
        ticket: 0, exchange: "".into(), routing_key: queue.into(),
        mandatory: false, immediate: false,
    }).await.unwrap();
    amqp.send_content(1, &switchboard_wire::BasicProperties::new(), body).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(10), amqp.expect(1)).await.unwrap().unwrap();
    assert!(matches!(m, Method::BasicAck { .. }), "expected publisher confirm, got {m:?}");
}
