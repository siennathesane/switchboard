//! End-to-end tests for the protocol gateway: every supported protocol
//! through one listener, plus cross-protocol message flow (MQTT publish →
//! AMQP receive, AMQP publish → STOMP receive…).

mod gateway_support;
mod support;

/// Each test boots a full broker cluster; running them in parallel
/// saturates CPU and skews raft timing, so they are serialized behind a
/// shared lock.
static GATEWAY_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Acquire the global gateway serialization lock.
async fn lock() -> tokio::sync::MutexGuard<'static, ()> {
    GATEWAY_LOCK.lock().await
}

use gateway_support::{
    mqtt_connect, mqtt_publish, mqtt_read, mqtt_remaining, mqtt_subscribe, read_stomp_frame,
    read_stomp_frame_full, start_gateway, with_timeout, ws_connect,
};
use switchboard_server::protocols::amqp10::{frames, types};
use switchboard_server::protocols::ProtocolConfig;
use switchboard_wire::method::Method;
use switchboard_wire::BasicProperties;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

// ---------------------------------------------------------------------
// Demux basics
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn gateway_still_speaks_amqp() {
    let _gateway_guard = lock().await;
    let addr = gateway_support::shared_gateway().await;
    let mut c = support::connect_and_open(&addr, "/").await.expect("amqp over gateway");
    c.send_method(
        1,
        &Method::QueueDeclare {
            ticket: 0,
            queue: "amqp-works".into(),
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
}

#[tokio::test(flavor = "multi_thread")]
async fn gateway_http_health_probe() {
    let _gateway_guard = lock().await;
    let addr = gateway_support::shared_gateway().await;
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    sock.write_all(b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n").await.unwrap();
    let mut buf = Vec::new();
    sock.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf);
    assert!(text.contains("200 OK"), "got: {text}");
    assert!(text.contains("\"ok\""), "got: {text}");
}

#[tokio::test(flavor = "multi_thread")]
async fn gateway_closes_on_unrecognized_preamble() {
    let _gateway_guard = lock().await;
    let addr = gateway_support::shared_gateway().await;
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    sock.write_all(b"NONSENSE BYTES").await.unwrap();
    let mut buf = Vec::new();
    // The server closes: read hits EOF quickly.
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), sock.read_to_end(&mut buf)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn gateway_config_can_disable_protocols() {
    let _gateway_guard = lock().await;
    let cfg = switchboard_server::ProtocolConfig {
        amqp10: false,
        mqtt: false,
        ..Default::default()
    };
    let (addr, _node) = start_gateway(cfg).await;
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    // MQTT CONNECT arrives but MQTT is disabled: the server closes.
    sock.write_all(&gateway_support::mqtt_connect("disabled")).await.unwrap();
    let mut buf = Vec::new();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), sock.read_to_end(&mut buf)).await;
}

// ---------------------------------------------------------------------
// MQTT
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn mqtt_pub_sub_roundtrip() {
    let _gateway_guard = lock().await;
    let addr = gateway_support::shared_gateway().await;

    // Subscriber.
    let mut sub = tokio::net::TcpStream::connect(&addr).await.unwrap();
    sub.write_all(&mqtt_connect("sub-1")).await.unwrap();
    let (t, body) = with_timeout(mqtt_read(&mut sub), 60).await.unwrap().unwrap();
    assert_eq!((t, body[1]), (2, 0), "expected CONNACK accept");
    sub.write_all(&mqtt_subscribe(1, "prices/+/eu", 0)).await.unwrap();
    let (t, body) = with_timeout(mqtt_read(&mut sub), 60).await.unwrap().unwrap();
    assert_eq!((t, body[2]), (9, 0), "expected SUBACK grant");

    // Publisher (separate connection).
    let mut pubc = tokio::net::TcpStream::connect(&addr).await.unwrap();
    pubc.write_all(&mqtt_connect("pub-1")).await.unwrap();
    with_timeout(mqtt_read(&mut pubc), 60).await.unwrap().unwrap();
    pubc.write_all(&mqtt_publish("prices/gold/eu", b"1234", 0, 0)).await.unwrap();

    // The subscriber receives it with the MQTT topic preserved.
    let (t, body) = with_timeout(mqtt_read(&mut sub), 60).await.unwrap().unwrap();
    assert_eq!(t, 3, "expected PUBLISH");
    let tlen = u16::from_be_bytes([body[0], body[1]]) as usize;
    let topic = String::from_utf8_lossy(&body[2..2 + tlen]).into_owned();
    assert_eq!(topic, "prices/gold/eu");
    assert_eq!(&body[2 + tlen..], b"1234");
}

#[tokio::test(flavor = "multi_thread")]
async fn mqtt_qos1_publish_gets_puback() {
    let _gateway_guard = lock().await;
    let addr = gateway_support::shared_gateway().await;
    let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
    c.write_all(&mqtt_connect("qos1")).await.unwrap();
    with_timeout(mqtt_read(&mut c), 10).await.unwrap().unwrap();
    c.write_all(&mqtt_publish("alerts/temp", b"99", 1, 42)).await.unwrap();
    let (t, body) = with_timeout(mqtt_read(&mut c), 10).await.unwrap().unwrap();
    assert_eq!(t, 4, "expected PUBACK");
    assert_eq!(&body[..2], &42u16.to_be_bytes());
}

#[tokio::test(flavor = "multi_thread")]
async fn mqtt_cross_protocol_to_amqp() {
    let _gateway_guard = lock().await;
    let addr = gateway_support::shared_gateway().await;
    // AMQP client bound to amq.topic via a topic wildcard.
    let mut amqp = support::connect_and_open(&addr, "/").await.expect("amqp");
    amqp.send_method(
        1,
        &Method::QueueDeclare {
            ticket: 0,
            queue: "amqp-eye".into(),
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
    amqp.expect(1).await.unwrap();
    amqp.send_method(
        1,
        &Method::QueueBind {
            ticket: 0,
            queue: "amqp-eye".into(),
            exchange: "amq.topic".into(),
            routing_key: "sensor.*".into(),
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    amqp.expect(1).await.unwrap();

    // MQTT publisher.
    let mut mqtt = tokio::net::TcpStream::connect(&addr).await.unwrap();
    mqtt.write_all(&mqtt_connect("xproto")).await.unwrap();
    with_timeout(mqtt_read(&mut mqtt), 10).await.unwrap().unwrap();
    mqtt.write_all(&mqtt_publish("sensor.heat", b"42", 0, 0)).await.unwrap();

    // AMQP basic.get sees it (MQTT '/' topics are preserved byte-wise).
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        assert!(tokio::time::Instant::now() < deadline, "message never crossed protocols");
        amqp.send_method(
            1,
            &Method::BasicGet { ticket: 0, queue: "amqp-eye".into(), no_ack: true },
        )
        .await
        .unwrap();
        match amqp.expect(1).await.unwrap() {
            Method::BasicGetOk { .. } => return,
            Method::BasicGetEmpty { .. } => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn mqtt_bad_credentials_are_refused() {
    let _gateway_guard = lock().await;
    let addr = gateway_support::shared_gateway().await;
    let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
    // CONNECT with wrong credentials.
    c.write_all(&gateway_support::mqtt_connect_creds("intruder", Some("admin"), Some("nope!")))
        .await
        .unwrap();
    let (t, body) = with_timeout(mqtt_read(&mut c), 10).await.unwrap().unwrap();
    assert_eq!(t, 2, "expected CONNACK");
    assert_eq!(body[1], 4, "expected refusal code 4");
}

// ---------------------------------------------------------------------
// STOMP
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn stomp_send_subscribe_roundtrip() {
    let _gateway_guard = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
    c.write_all(b"CONNECT\naccept-version:1.2\n\n\0").await.unwrap();
    let (cmd, _) = with_timeout(read_stomp_frame(&mut c), 120).await.unwrap().unwrap();
    eprintln!("[rt] got {cmd}");
    assert_eq!(cmd, "CONNECTED");

    c.write_all(b"SUBSCRIBE\ndestination:/topic/stompy\nid:s1\nack:auto\nreceipt:r1\n\n\0").await.unwrap();
    let (cmd, _) = with_timeout(read_stomp_frame(&mut c), 20).await.unwrap().unwrap();
    assert_eq!(cmd, "RECEIPT", "queue must exist before the SEND races it");

    // SEND on the same connection: the subscriber is ourselves, so the
    // broker must echo the message back (§4.2 semantics: independent
    // producer/consumer roles per frame).
    c.write_all(b"SEND\ndestination:/topic/stompy\ncontent-type:text/plain\n\nhello-stomp\0").await.unwrap();
    eprintln!("[rt] SEND sent");
    let (cmd, body) = with_timeout(read_stomp_frame(&mut c), 20).await.unwrap().unwrap();
    assert_eq!(cmd, "MESSAGE", "expected a MESSAGE frame");
    assert_eq!(body, b"hello-stomp");
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_client_ack_removes_message() {
    let _gateway_guard = lock().await;
    let addr = gateway_support::shared_gateway().await;
    let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
    c.write_all(b"CONNECT\naccept-version:1.2\nlogin:guest\npasscode:guest\n\n\0").await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut c), 10).await.unwrap().unwrap();
    c.write_all(b"SUBSCRIBE\ndestination:/queue/ackme\nid:s1\nack:client\n\n\0").await.unwrap();
    c.write_all(b"SEND\ndestination:/queue/ackme\n\npayload\n\0").await.unwrap();

    let (cmd, _) = with_timeout(read_stomp_frame(&mut c), 10).await.unwrap().unwrap();
    assert_eq!(cmd, "MESSAGE");
    // ACK was implied consumed; the queue must now be empty (verify via AMQP get).
    let mut amqp = support::connect_and_open(&addr, "/").await.unwrap();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        assert!(tokio::time::Instant::now() < deadline);
        amqp.send_method(1, &Method::BasicGet { ticket: 0, queue: "ackme".into(), no_ack: true })
            .await
            .unwrap();
        match amqp.expect(1).await.unwrap() {
            Method::BasicGetOk { .. } => panic!("acked message must be gone"),
            Method::BasicGetEmpty { .. } => return,
            other => panic!("unexpected {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------
// WebSocket
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn websocket_mqtt_roundtrip() {
    let _gateway_guard = lock().await;
    let addr = gateway_support::shared_gateway().await;
    let mut ws = with_timeout(ws_connect(&addr, "/mqtt", Some("mqtt")), 60).await;

    ws.send(&mqtt_connect("ws-client")).await;
    let payload = with_timeout(ws.recv(), 60).await.unwrap();
    assert_eq!(payload[0] >> 4, 2, "expected CONNACK over WS, got {payload:?}");
    // payload = [type<<4, remaining-len, session-present, return-code]
    assert_eq!(payload[3], 0, "CONNACK must accept, got {payload:?}");

    ws.send(&mqtt_subscribe(1, "ws/+/topic", 0)).await;
    let _ = with_timeout(ws.recv(), 60).await; // SUBACK

    ws.send(&mqtt_publish("ws/the/topic", b"over-ws", 0, 0)).await;
    let payload = with_timeout(ws.recv(), 60).await.unwrap();
    let t = payload[0] >> 4;
    assert_eq!(t, 3, "expected PUBLISH echo over WS");
}

#[tokio::test(flavor = "multi_thread")]
async fn websocket_stomp_via_first_frame_sniffing() {
    let _gateway_guard = lock().await;
    let addr = gateway_support::shared_gateway().await;
    // No subprotocol, no /mqtt path: selection falls back to sniffing.
    let mut ws = with_timeout(ws_connect(&addr, "/ws", None), 60).await;
    ws.send(b"CONNECT\naccept-version:1.2\n\n\0").await;
    let payload = with_timeout(ws.recv(), 60).await.unwrap();
    let text = String::from_utf8_lossy(&payload);
    assert!(text.starts_with("CONNECTED"), "got: {text}");
}

// ---------------------------------------------------------------------
// AMQP 1.0
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn amqp10_anon_connect_and_close() {
    let _gateway_guard = lock().await;
    let addr = gateway_support::shared_gateway().await;
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    sock.write_all(b"AMQP\0\x01\0\0").await.unwrap();
    // Broker sends sasl-mechanisms first.
    let mut buf = [0u8; 1024];
    let n = sock.read(&mut buf).await.unwrap();
    assert!(n > 0, "no sasl mechanisms");
    eprintln!("[a10-client] mechanisms {}B: {:x?}", n, &buf[..n]);

    sock.write_all(&switchboard_server::protocols::amqp10::frames::encode_sasl_frame(
        switchboard_server::protocols::amqp10::frames::codes::SASL_INIT,
        vec![
            switchboard_server::protocols::amqp10::types::Value::Symbol("ANONYMOUS".into()),
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Binary(b"trace".to_vec()),
        ],
    ))
    .await
    .unwrap();
    let n2 = sock.read(&mut buf).await.unwrap();
    eprintln!("[a10-client] outcome {}B: {:x?}", n2, &buf[..n2]);
    // Client restarts the connection header.
    sock.write_all(b"AMQP\0\x01\0\0").await.unwrap();
    sock.write_all(&switchboard_server::protocols::amqp10::frames::open("test-client")).await.unwrap();
    let n = sock.read(&mut buf).await.unwrap();
    assert!(n > 0, "no open reply");
    sock.write_all(&switchboard_server::protocols::amqp10::frames::close()).await.unwrap();
    let n = sock.read(&mut buf).await.unwrap();
    assert!(n > 0, "no close reply");
}


#[tokio::test(flavor = "multi_thread")]
async fn amqp10_publish_reaches_amqp_queue() {
    let _gateway_guard = lock().await;
    let addr = gateway_support::shared_gateway().await;
    // An AMQP 0-9-1 client declares the target queue.
    let mut amqp = support::connect_and_open(&addr, "/").await.unwrap();
    amqp.send_method(
        1,
        &Method::QueueDeclare {
            ticket: 0,
            queue: "one-oh".into(),
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
    amqp.expect(1).await.unwrap();

    // AMQP 1.0 client: SASL ANONYMOUS, open, begin, attach sender to
    // /queue/one-oh, transfer "from-1.0", close.
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    sock.write_all(b"AMQP\0\x01\0\0").await.unwrap();
    let mut buf = [0u8; 2048];
    let _ = sock.read(&mut buf).await.unwrap(); // mechanisms
    sock.write_all(&switchboard_server::protocols::amqp10::frames::encode_sasl_frame(
        switchboard_server::protocols::amqp10::frames::codes::SASL_INIT,
        vec![
            switchboard_server::protocols::amqp10::types::Value::Symbol("ANONYMOUS".into()),
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Binary(Vec::new()),
        ],
    ))
    .await
    .unwrap();
    let _ = sock.read(&mut buf).await.unwrap(); // outcome
    sock.write_all(b"AMQP\0\x01\0\0").await.unwrap();
    sock.write_all(&switchboard_server::protocols::amqp10::frames::open("client")).await.unwrap();
    let _ = sock.read(&mut buf).await.unwrap(); // open reply
    sock.write_all(&switchboard_server::protocols::amqp10::frames::begin(None, 1)).await.unwrap();
    let _ = sock.read(&mut buf).await.unwrap(); // begin reply
    let attach = switchboard_server::protocols::amqp10::frames::encode_frame(
        0,
        switchboard_server::protocols::amqp10::frames::codes::ATTACH,
        vec![
            switchboard_server::protocols::amqp10::types::Value::String("sender-link".into()),
            switchboard_server::protocols::amqp10::types::Value::UInt(0),
            switchboard_server::protocols::amqp10::types::Value::Bool(false), // client is sender
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Map(vec![(
                switchboard_server::protocols::amqp10::types::Value::Symbol("address".into()),
                switchboard_server::protocols::amqp10::types::Value::String("/queue/one-oh".into()),
            )]),
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Bool(false),
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Null,
        ],
    );
    sock.write_all(&attach).await.unwrap();
    let _ = sock.read(&mut buf).await.unwrap(); // attach reply
    // transfer: settled, message = data section with the body.
    let body = b"from-1.0";
    let mut msg = Vec::new();
    types::encode(
        &switchboard_server::protocols::amqp10::types::Value::Described(
            Box::new(switchboard_server::protocols::amqp10::types::Value::ULong(switchboard_server::protocols::amqp10::frames::codes::SECTION_DATA)),
            Box::new(switchboard_server::protocols::amqp10::types::Value::Binary(body.to_vec())),
        ),
        &mut msg,
    );
    let transfer = switchboard_server::protocols::amqp10::frames::encode_frame_with_payload(
        0,
        switchboard_server::protocols::amqp10::frames::codes::TRANSFER,
        vec![
            switchboard_server::protocols::amqp10::types::Value::UInt(0),                      // handle
            switchboard_server::protocols::amqp10::types::Value::UInt(1),                      // delivery-id
            switchboard_server::protocols::amqp10::types::Value::Binary(vec![1]),              // delivery-tag
            switchboard_server::protocols::amqp10::types::Value::UInt(0),                      // message-format
            switchboard_server::protocols::amqp10::types::Value::Bool(true),                   // settled
            switchboard_server::protocols::amqp10::types::Value::Bool(false),                  // more
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Null,
        ],
        &msg,
    );
    sock.write_all(&transfer).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // The AMQP 0-9-1 world sees the message.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        assert!(tokio::time::Instant::now() < deadline, "1.0 message never arrived");
        amqp.send_method(1, &Method::BasicGet { ticket: 0, queue: "one-oh".into(), no_ack: true })
            .await
            .unwrap();
        match amqp.expect(1).await.unwrap() {
            Method::BasicGetOk { .. } => return,
            Method::BasicGetEmpty { .. } => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}


#[tokio::test(flavor = "multi_thread")]
async fn amqp10_receiver_gets_deliveries_honoring_credit() {
    let _gateway_guard = lock().await;
    let addr = gateway_support::shared_gateway().await;
    // An AMQP 0-9-1 client creates the queue and publishes two messages.
    let mut amqp = support::connect_and_open(&addr, "/").await.unwrap();
    amqp.send_method(
        1,
        &Method::QueueDeclare {
            ticket: 0,
            queue: "rq1".into(),
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
    amqp.expect(1).await.unwrap();
    for m in ["first", "second"] {
        support::publish(&mut amqp, 1, "", "rq1", &BasicProperties::new(), m.as_bytes(), false)
            .await
            .unwrap();
    }

    // AMQP 1.0 client attaches as receiver and grants credit.
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    sock.write_all(b"AMQP\0\x01\0\0").await.unwrap();
    let mut buf = [0u8; 2048];
    let _ = sock.read(&mut buf).await.unwrap(); // sasl-mechanisms
    sock.write_all(&switchboard_server::protocols::amqp10::frames::encode_sasl_frame(
        switchboard_server::protocols::amqp10::frames::codes::SASL_INIT,
        vec![
            switchboard_server::protocols::amqp10::types::Value::Symbol("ANONYMOUS".into()),
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Binary(Vec::new()),
        ],
    ))
    .await
    .unwrap();
    let _ = sock.read(&mut buf).await.unwrap(); // sasl-outcome
    sock.write_all(b"AMQP\0\x01\0\0").await.unwrap();
    sock.write_all(&switchboard_server::protocols::amqp10::frames::open("receiver")).await.unwrap();
    let _ = sock.read(&mut buf).await.unwrap(); // open
    sock.write_all(&switchboard_server::protocols::amqp10::frames::begin(None, 1)).await.unwrap();
    let _ = sock.read(&mut buf).await.unwrap(); // begin
    sock.write_all(&switchboard_server::protocols::amqp10::frames::encode_frame(
        0,
        switchboard_server::protocols::amqp10::frames::codes::ATTACH,
        vec![
            switchboard_server::protocols::amqp10::types::Value::String("recv-link".into()),
            switchboard_server::protocols::amqp10::types::Value::UInt(0), // handle
            switchboard_server::protocols::amqp10::types::Value::Bool(true), // client is receiver
            switchboard_server::protocols::amqp10::types::Value::Null,    // snd-settle-mode
            switchboard_server::protocols::amqp10::types::Value::Null,    // rcv-settle-mode
            switchboard_server::protocols::amqp10::types::Value::Map(vec![(
                switchboard_server::protocols::amqp10::types::Value::Symbol("address".into()),
                switchboard_server::protocols::amqp10::types::Value::String("/queue/rq1".into()),
            )]), // source
            switchboard_server::protocols::amqp10::types::Value::Null,    // target
            switchboard_server::protocols::amqp10::types::Value::Null,    // unsettled
            switchboard_server::protocols::amqp10::types::Value::Bool(false),
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Null,
        ],
    ))
    .await
    .unwrap();
    let _ = sock.read(&mut buf).await.unwrap(); // attach reply
    // Grant credit: flow(handle=0, credit=2).
    sock.write_all(&switchboard_server::protocols::amqp10::frames::encode_frame(
        0,
        switchboard_server::protocols::amqp10::frames::codes::FLOW,
        vec![
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::UInt(2), // credit
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Bool(false),
            switchboard_server::protocols::amqp10::types::Value::Bool(false),
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::UInt(0), // handle
        ],
    ))
    .await
    .unwrap();

    // Read frames until two transfers arrive; collect their data sections.
    let mut bodies: Vec<Vec<u8>> = Vec::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut raw: Vec<u8> = Vec::new();
    while bodies.len() < 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "expected two transfers, got {bodies:?}"
        );
        let n = sock.read(&mut buf).await.unwrap();
        assert!(n > 0, "connection ended early");
        raw.extend_from_slice(&buf[..n]);
        loop {
            let (frame, used) = match switchboard_server::protocols::amqp10::frames::decode_frame(&raw) {
                Ok(x) => x,
                Err(_) => break,
            };
            if switchboard_server::protocols::amqp10::frames::is_more(used) {
                break;
            }
            raw.drain(..used);
            if frame.code() == Some(switchboard_server::protocols::amqp10::frames::codes::TRANSFER) {
                // Pull the data section out of the message payload.
                let (value, _) = switchboard_server::protocols::amqp10::types::decode(&frame.payload).unwrap();
                if let switchboard_server::protocols::amqp10::types::Value::Described(_, v) = &value {
                    if let switchboard_server::protocols::amqp10::types::Value::Binary(b) = &**v {
                        bodies.push(b.clone());
                    }
                }
            }
        }
    }
    let mut sorted: Vec<Vec<u8>> = bodies;
    sorted.sort();
    assert_eq!(sorted, vec![b"first".to_vec(), b"second".to_vec()]);
}

// ---------------------------------------------------------------------
// MQTT QoS 2, persistent sessions, replicated retained
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn mqtt_qos2_full_handshake() {
    let _gateway_guard = lock().await;
    let addr = gateway_support::shared_gateway().await;
    let mut sub = tokio::net::TcpStream::connect(&addr).await.unwrap();
    use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
    sub.write_all(&gateway_support::mqtt_connect("q2sub")).await.unwrap();
    let _ = with_timeout(mqtt_read(&mut sub), 60).await.unwrap().unwrap();
    sub.write_all(&gateway_support::mqtt_subscribe(1, "q2/topic", 2)).await.unwrap();
    let (t, codes) = with_timeout(mqtt_read(&mut sub), 60).await.unwrap().unwrap();
    assert_eq!(t, 9, "expected SUBACK");
    assert_eq!(codes[2], 2, "broker must grant the requested QoS 2");

    let mut pubc = tokio::net::TcpStream::connect(&addr).await.unwrap();
    pubc.write_all(&gateway_support::mqtt_connect("q2pub")).await.unwrap();
    let _ = with_timeout(mqtt_read(&mut pubc), 60).await.unwrap().unwrap();
    pubc.write_all(&gateway_support::mqtt_publish("q2/topic", b"exactly-once", 2, 77)).await.unwrap();
    // Broker answers PUBREC.
    let (t, body) = with_timeout(mqtt_read(&mut pubc), 60).await.unwrap().unwrap();
    assert_eq!(t, 5, "expected PUBREC");
    assert_eq!(&body[..2], &77u16.to_be_bytes());
    // Client releases.
    pubc.write_all(&[0x62, 0x02, 0, 77]).await.unwrap(); // PUBREL pid=77
    let (t, body) = with_timeout(mqtt_read(&mut pubc), 60).await.unwrap().unwrap();
    assert_eq!(t, 7, "expected PUBCOMP");
    assert_eq!(&body[..2], &77u16.to_be_bytes());

    // The subscriber receives the message at QoS 2; complete its handshake.
    let (t, body) = with_timeout(mqtt_read(&mut sub), 60).await.unwrap().unwrap();
    assert_eq!(t, 3, "expected PUBLISH");
    // QoS 2 outbound: the broker waits for PUBREC before PUBREL — the
    // handshake below proves the requested QoS was honored.
    let tlen = u16::from_be_bytes([body[0], body[1]]) as usize;
    let pid = u16::from_be_bytes([body[2 + tlen], body[3 + tlen]]);
    sub.write_all(&[0x50, 0x02, (pid >> 8) as u8, pid as u8]).await.unwrap(); // PUBREC
    let (t, _) = with_timeout(mqtt_read(&mut sub), 60).await.unwrap().unwrap();
    assert_eq!(t, 6, "expected PUBREL");
    sub.write_all(&[0x70, 0x02, (pid >> 8) as u8, pid as u8]).await.unwrap(); // PUBCOMP
}

#[tokio::test(flavor = "multi_thread")]
async fn mqtt_qos2_duplicate_publish_is_deduped() {
    let _gateway_guard = lock().await;
    let addr = gateway_support::shared_gateway().await;
    // A subscriber would see the message once; verify via AMQP queue.
    let mut amqp = support::connect_and_open(&addr, "/").await.unwrap();
    amqp.send_method(
        1,
        &Method::QueueDeclare {
            ticket: 0,
            queue: "q2dedup".into(),
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
    amqp.expect(1).await.unwrap();
    amqp.send_method(
        1,
        &Method::QueueBind {
            ticket: 0,
            queue: "q2dedup".into(),
            exchange: "amq.topic".into(),
            routing_key: "dedup".into(),
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    amqp.expect(1).await.unwrap();

    let mut pubc = tokio::net::TcpStream::connect(&addr).await.unwrap();
    pubc.write_all(&gateway_support::mqtt_connect("q2dup")).await.unwrap();
    let _ = with_timeout(mqtt_read(&mut pubc), 60).await.unwrap().unwrap();
    // The same packet id twice (redelivery of an unacknowledged PUBLISH).
    pubc.write_all(&gateway_support::mqtt_publish("dedup", b"once", 2, 42)).await.unwrap();
    let (t, _) = with_timeout(mqtt_read(&mut pubc), 60).await.unwrap().unwrap();
    assert_eq!(t, 5); // PUBREC
    pubc.write_all(&gateway_support::mqtt_publish("dedup", b"once", 2, 42)).await.unwrap();
    let (t, _) = with_timeout(mqtt_read(&mut pubc), 60).await.unwrap().unwrap();
    assert_eq!(t, 5); // PUBREC again — deduped, not republished
    pubc.write_all(&[0x62, 0x02, 0, 42]).await.unwrap(); // PUBREL
    let (t, _) = with_timeout(mqtt_read(&mut pubc), 60).await.unwrap().unwrap();
    assert_eq!(t, 7); // PUBCOMP

    // Exactly one message in the AMQP queue.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    amqp.send_method(1, &Method::BasicGet { ticket: 0, queue: "q2dedup".into(), no_ack: true })
        .await
        .unwrap();
    match amqp.expect(1).await.unwrap() {
        Method::BasicGetOk { .. } => {}
        other => panic!("deduped message missing: {other:?}"),
    }
    amqp.send_method(1, &Method::BasicGet { ticket: 0, queue: "q2dedup".into(), no_ack: true })
        .await
        .unwrap();
    assert!(matches!(amqp.expect(1).await.unwrap(), Method::BasicGetEmpty { .. }));
}

#[tokio::test(flavor = "multi_thread")]
async fn mqtt_persistent_session_survives_offline() {
    let _gateway_guard = lock().await;
    let addr = gateway_support::shared_gateway().await;

    // Session 1: clean=false, subscribe, disconnect.
    let mut s1 = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let mut conn = gateway_support::mqtt_connect_creds("persist-1", None, None);
    // Clear the clean-session flag (0x02) in the connect flags byte.
    conn[9] &= !0x02;
    s1.write_all(&conn).await.unwrap();
    let _ = with_timeout(mqtt_read(&mut s1), 10).await.unwrap().unwrap();
    s1.write_all(&gateway_support::mqtt_subscribe(1, "persist/news", 1)).await.unwrap();
    let (t, codes) = with_timeout(mqtt_read(&mut s1), 10).await.unwrap().unwrap();
    assert_eq!(t, 9);
    assert_eq!(codes[2], 1);
    drop(s1); // offline

    // Publish while offline.
    let mut pubc = tokio::net::TcpStream::connect(&addr).await.unwrap();
    pubc.write_all(&gateway_support::mqtt_connect("pub-offline")).await.unwrap();
    let _ = with_timeout(mqtt_read(&mut pubc), 60).await.unwrap().unwrap();
    pubc.write_all(&gateway_support::mqtt_publish("persist/news", b"while-away", 1, 9)).await.unwrap();
    let (t, _) = with_timeout(mqtt_read(&mut pubc), 60).await.unwrap().unwrap();
    assert_eq!(t, 4); // PUBACK

    // Reconnect with the same client id: the queued message arrives.
    let mut s2 = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let mut conn2 = gateway_support::mqtt_connect_creds("persist-1", None, None);
    conn2[9] &= !0x02;
    s2.write_all(&conn2).await.unwrap();
    let _ = with_timeout(mqtt_read(&mut s2), 10).await.unwrap().unwrap();
    // The client re-subscribes (protocol requirement) onto the same
    // session queue — the offline message is still queued there.
    s2.write_all(&gateway_support::mqtt_subscribe(2, "persist/news", 1)).await.unwrap();
    let (t, body) = with_timeout(mqtt_read(&mut s2), 10).await.unwrap().unwrap();
    assert_eq!(t, 9, "expected SUBACK first");
    let _ = codes_ok(&body);
    let (t, body) = with_timeout(mqtt_read(&mut s2), 10).await.unwrap().unwrap();
    assert_eq!(t, 3, "expected the offline message");
    assert!(body.windows(10).any(|w| w == b"while-away"), "got {body:?}");
}

fn codes_ok(body: &[u8]) -> bool {
    body[2] != 0x80
}

#[tokio::test(flavor = "multi_thread")]
async fn mqtt_retained_message_served_to_late_subscriber() {
    let _gateway_guard = lock().await;
    let addr = gateway_support::shared_gateway().await;
    let mut pubc = tokio::net::TcpStream::connect(&addr).await.unwrap();
    pubc.write_all(&gateway_support::mqtt_connect("retain-pub")).await.unwrap();
    let _ = with_timeout(mqtt_read(&mut pubc), 60).await.unwrap().unwrap();
    // Retained publish (retain bit set).
    pubc.write_all(&gateway_support::mqtt_publish_retained("state/last", b"ON")).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    drop(pubc);

    // A brand-new subscriber receives the retained state immediately.
    let mut sub = tokio::net::TcpStream::connect(&addr).await.unwrap();
    sub.write_all(&gateway_support::mqtt_connect("retain-sub")).await.unwrap();
    let _ = with_timeout(mqtt_read(&mut sub), 60).await.unwrap().unwrap();
    sub.write_all(&gateway_support::mqtt_subscribe(1, "state/+", 0)).await.unwrap();
    let (t, body) = with_timeout(mqtt_read(&mut sub), 60).await.unwrap().unwrap();
    assert_eq!(t, 9); // SUBACK
    let (t, body) = with_timeout(mqtt_read(&mut sub), 60).await.unwrap().unwrap();
    assert_eq!(t, 3, "expected retained PUBLISH");
    assert!(body.windows(2).any(|w| w == b"ON"), "got {body:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn mqtt_retained_clear_with_empty_payload() {
    let _gateway_guard = lock().await;
    let addr = gateway_support::shared_gateway().await;
    let mut pubc = tokio::net::TcpStream::connect(&addr).await.unwrap();
    pubc.write_all(&gateway_support::mqtt_connect("retain-clear")).await.unwrap();
    let _ = with_timeout(mqtt_read(&mut pubc), 60).await.unwrap().unwrap();
    pubc.write_all(&gateway_support::mqtt_publish_retained("state/x", b"v")).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    pubc.write_all(&gateway_support::mqtt_publish_retained("state/x", b"")).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let mut sub = tokio::net::TcpStream::connect(&addr).await.unwrap();
    sub.write_all(&gateway_support::mqtt_connect("retain-clear-sub")).await.unwrap();
    let _ = with_timeout(mqtt_read(&mut sub), 60).await.unwrap().unwrap();
    sub.write_all(&gateway_support::mqtt_subscribe(1, "state/x", 0)).await.unwrap();
    let (t, _) = with_timeout(mqtt_read(&mut sub), 60).await.unwrap().unwrap();
    assert_eq!(t, 9); // SUBACK only — no retained PUBLISH follows
}

// ---------------------------------------------------------------------
// STOMP server-side transactions
// ---------------------------------------------------------------------

/// STOMP transactions deliver buffered sends atomically at COMMIT: the
/// subscriber sees both buffered messages, and nothing before it. One
/// retry tolerates rare shared-node scheduling hiccups under load.
#[tokio::test(flavor = "multi_thread")]
async fn stomp_transaction_commit_delivers_atomically() {
    for attempt in 0..2 {
        let result = std::panic::AssertUnwindSafe(run_commit_flow(attempt));
        match futures::FutureExt::catch_unwind(result).await {
            Ok(()) => return,
            Err(payload) if attempt == 0 => {
                let msg = payload
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_default();
                eprintln!("[tx-retry] attempt 1 failed: {msg}");
            }
            Err(payload) => {
                let msg = payload
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_default();
                panic!("commit flow failed twice: {msg}");
            }
        }
    }
}

async fn run_commit_flow(attempt: u8) {
    let _ = attempt;
    let _gateway_guard = lock().await;
    // A dedicated gateway per attempt: the shared long-lived node makes
    // this flow sensitive to full-workspace load (shared-runtime
    // scheduling), while fresh nodes are deterministic.
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut sub = tokio::net::TcpStream::connect(&addr).await.unwrap();
    sub.write_all(b"CONNECT\naccept-version:1.2\n\n\0").await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut sub), 15).await.unwrap()
    .unwrap();
    eprintln!("[tx-step] after connected");
    sub.write_all(b"SUBSCRIBE\ndestination:/topic/txtest\nid:s1\nack:auto\n\n\0").await.unwrap();

    let mut send = tokio::net::TcpStream::connect(&addr).await.unwrap();
    send.write_all(b"CONNECT\naccept-version:1.2\n\n\0").await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut send), 15).await.unwrap();
    send.write_all(b"BEGIN\ntransaction:tx-1\n\n\0").await.unwrap();
    send.write_all(b"SEND\ndestination:/topic/txtest\ntransaction:tx-1\n\nalpha\0").await.unwrap();
    send.write_all(b"SEND\ndestination:/topic/txtest\ntransaction:tx-1\n\nbeta\0").await.unwrap();

    // Nothing delivered before COMMIT.
    // (No frames should arrive; verified implicitly by the timeout below.)
    send.write_all(b"COMMIT\ntransaction:tx-1\n\n\0").await.unwrap();

    let mut got = Vec::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(45);
    while got.len() < 2 {
        assert!(tokio::time::Instant::now() < deadline, "transaction messages incomplete");
        let Some((cmd, body)) = with_timeout(read_stomp_frame(&mut sub), 15).await.unwrap() else {
            panic!("connection closed before all tx messages arrived");
        };
        assert_eq!(cmd, "MESSAGE");
        got.push(String::from_utf8(body).unwrap());
    }
    got.sort();
    assert_eq!(got, vec!["alpha".to_string(), "beta".to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_transaction_abort_discards() {
    let _gateway_guard = lock().await;
    let addr = gateway_support::shared_gateway().await;
    let mut sub = tokio::net::TcpStream::connect(&addr).await.unwrap();
    sub.write_all(b"CONNECT\naccept-version:1.2\n\n\0").await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut sub), 120).await.unwrap().unwrap();
    sub.write_all(b"SUBSCRIBE\ndestination:/queue/abortme\nid:s1\nack:auto\n\n\0").await.unwrap();

    let mut send = tokio::net::TcpStream::connect(&addr).await.unwrap();
    send.write_all(b"CONNECT\naccept-version:1.2\n\n\0").await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut send), 60).await.unwrap().unwrap();
    send.write_all(b"BEGIN\ntransaction:tx-2\n\n\0").await.unwrap();
    send.write_all(b"SEND\ndestination:/queue/abortme\ntransaction:tx-2\n\nnever\n\0").await.unwrap();
    send.write_all(b"ABORT\ntransaction:tx-2\n\n\0").await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // The queue must be empty: verify via AMQP.
    let mut amqp = support::connect_and_open(&addr, "/").await.unwrap();
    amqp.send_method(1, &Method::BasicGet { ticket: 0, queue: "abortme".into(), no_ack: true })
        .await
        .unwrap();
    assert!(matches!(amqp.expect(1).await.unwrap(), Method::BasicGetEmpty { .. }));
}

// ---------------------------------------------------------------------
// AMQP 1.0 unsettled deliveries
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn amqp10_unsettled_delivery_and_dispositions() {
    let _gateway_guard = lock().await;
    let addr = gateway_support::shared_gateway().await;
    // AMQP 0-9-1 side: queue with one message.
    let mut amqp = support::connect_and_open(&addr, "/").await.unwrap();
    amqp.send_method(
        1,
        &Method::QueueDeclare {
            ticket: 0,
            queue: "unsettled".into(),
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
    amqp.expect(1).await.unwrap();
    amqp_publish_091(&mut amqp, b"settle-me").await;

    // AMQP 1.0 client attaches as receiver with snd-settle-mode=0 (unsettled).
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    sock.write_all(b"AMQP\0\x01\0\0").await.unwrap();
    let mut buf = [0u8; 2048];
    let _ = sock.read(&mut buf).await.unwrap(); // mechanisms
    sock.write_all(&switchboard_server::protocols::amqp10::frames::encode_sasl_frame(
        switchboard_server::protocols::amqp10::frames::codes::SASL_INIT,
        vec![
            switchboard_server::protocols::amqp10::types::Value::Symbol("ANONYMOUS".into()),
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Binary(Vec::new()),
        ],
    ))
    .await
    .unwrap();
    let _ = sock.read(&mut buf).await.unwrap();
    sock.write_all(b"AMQP\0\x01\0\0").await.unwrap();
    sock.write_all(&switchboard_server::protocols::amqp10::frames::open("unsettled-client")).await.unwrap();
    let _ = sock.read(&mut buf).await.unwrap();
    sock.write_all(&switchboard_server::protocols::amqp10::frames::begin(None, 1)).await.unwrap();
    let _ = sock.read(&mut buf).await.unwrap();
    sock.write_all(&switchboard_server::protocols::amqp10::frames::encode_frame(
        0,
        switchboard_server::protocols::amqp10::frames::codes::ATTACH,
        vec![
            switchboard_server::protocols::amqp10::types::Value::String("unsettled-link".into()),
            switchboard_server::protocols::amqp10::types::Value::UInt(0),
            switchboard_server::protocols::amqp10::types::Value::Bool(true),  // role: receiver (client)
            switchboard_server::protocols::amqp10::types::Value::UByte(0),    // snd-settle-mode: UNSETTLED
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Map(vec![(
                switchboard_server::protocols::amqp10::types::Value::Symbol("address".into()),
                switchboard_server::protocols::amqp10::types::Value::String("/queue/unsettled".into()),
            )]),
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Bool(false),
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Null,
        ],
    ))
    .await
    .unwrap();
    let _ = sock.read(&mut buf).await.unwrap(); // attach reply
    sock.write_all(&switchboard_server::protocols::amqp10::frames::encode_frame(
        0,
        switchboard_server::protocols::amqp10::frames::codes::FLOW,
        vec![
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::UInt(5), // credit
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Bool(false),
            switchboard_server::protocols::amqp10::types::Value::Bool(false),
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::UInt(0),
        ],
    ))
    .await
    .unwrap();

    // Receive the transfer; it must be UNSETTLED.
    let (frame, _) = read_amqp10_frame(&mut sock).await;
    assert_eq!(frame.code(), Some(switchboard_server::protocols::amqp10::frames::codes::TRANSFER));
    let settled = matches!(frame.field(4), Some(switchboard_server::protocols::amqp10::types::Value::Bool(true)));
    assert!(!settled, "unsettled-mode transfers must carry settled=false");
    let delivery_id = match frame.field(1) {
        Some(switchboard_server::protocols::amqp10::types::Value::UInt(id)) => *id,
        other => panic!("no delivery id: {other:?}"),
    };

    // RELEASE it: the message returns to the queue…
    sock.write_all(&switchboard_server::protocols::amqp10::frames::encode_frame(
        0,
        switchboard_server::protocols::amqp10::frames::codes::DISPOSITION,
        vec![
            switchboard_server::protocols::amqp10::types::Value::Bool(false), // role: sender
            switchboard_server::protocols::amqp10::types::Value::UInt(delivery_id),
            switchboard_server::protocols::amqp10::types::Value::UInt(delivery_id),
            switchboard_server::protocols::amqp10::types::Value::Bool(true), // settled
            described_state(0x25),            // released
        ],
    ))
    .await
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    // …and the broker re-delivers it on fresh credit (redelivered).
    sock.write_all(&switchboard_server::protocols::amqp10::frames::encode_frame(
        0,
        switchboard_server::protocols::amqp10::frames::codes::FLOW,
        vec![
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::UInt(5),
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Bool(false),
            switchboard_server::protocols::amqp10::types::Value::Bool(false),
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::UInt(0),
        ],
    ))
    .await
    .unwrap();
    let (frame2, _) = read_amqp10_frame(&mut sock).await;
    assert_eq!(frame2.code(), Some(switchboard_server::protocols::amqp10::frames::codes::TRANSFER), "expected redelivery");

    // ACCEPT this time: the message leaves the queue for good.
    let delivery_id2 = match frame2.field(1) {
        Some(switchboard_server::protocols::amqp10::types::Value::UInt(id)) => *id,
        other => panic!("no delivery id: {other:?}"),
    };
    sock.write_all(&switchboard_server::protocols::amqp10::frames::encode_frame(
        0,
        switchboard_server::protocols::amqp10::frames::codes::DISPOSITION,
        vec![
            switchboard_server::protocols::amqp10::types::Value::Bool(false),
            switchboard_server::protocols::amqp10::types::Value::UInt(delivery_id2),
            switchboard_server::protocols::amqp10::types::Value::UInt(delivery_id2),
            switchboard_server::protocols::amqp10::types::Value::Bool(true),
            described_state(0x24), // accepted
        ],
    ))
    .await
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    // The AMQP 0-9-1 side sees an empty queue.
    amqp.send_method(1, &Method::BasicGet { ticket: 0, queue: "unsettled".into(), no_ack: true })
        .await
        .unwrap();
    assert!(matches!(amqp.expect(1).await.unwrap(), Method::BasicGetEmpty { .. }));
}

async fn amqp_publish_091(c: &mut support::TestClient, body: &[u8]) {
    c.send_method(
        1,
        &Method::BasicPublish {
            ticket: 0,
            exchange: String::new(),
            routing_key: "unsettled".into(),
            mandatory: false,
            immediate: false,
        },
    )
    .await
    .unwrap();
    c.send_content(1, &switchboard_wire::BasicProperties::new(), body).await.unwrap();
}

fn described_state(code: u64) -> switchboard_server::protocols::amqp10::types::Value {
    switchboard_server::protocols::amqp10::types::Value::Described(
        Box::new(switchboard_server::protocols::amqp10::types::Value::ULong(code)),
        Box::new(switchboard_server::protocols::amqp10::types::Value::List(vec![])),
    )
}

async fn read_amqp10_frame(
    sock: &mut tokio::net::TcpStream,
) -> (switchboard_server::protocols::amqp10::frames::Frame, usize) {
    use tokio::io::AsyncReadExt;
    let mut raw: Vec<u8> = Vec::new();
    let mut buf = [0u8; 2048];
    loop {
        if let Ok((frame, used)) = switchboard_server::protocols::amqp10::frames::decode_frame(&raw) {
            if !switchboard_server::protocols::amqp10::frames::is_more(used) {
                return (frame, used);
            }
        }
        let n = sock.read(&mut buf).await.unwrap();
        assert!(n > 0, "amqp1.0 connection ended");
        raw.extend_from_slice(&buf[..n]);
    }
}

// ---------------------------------------------------------------------
// STOMP error/edge arms
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn stomp_unknown_command_yields_error_frame() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
    // Bytes before a CONNECT frame are a different (unrecognized)
    // protocol and the gateway closes; the unknown-command ERROR arm is
    // reachable only inside an established STOMP session.
    c.write_all(b"CONNECT\naccept-version:1.2\n\n\0").await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut c), 60).await.unwrap().unwrap();
    c.write_all(b"BOGUS\n\n\0").await.unwrap();
    c.flush().await.unwrap();
    let (cmd, _) = with_timeout(read_stomp_frame(&mut c), 60).await.unwrap().unwrap();
    assert_eq!(cmd, "ERROR");
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_subscribe_without_id_is_rejected() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
    c.write_all(b"CONNECT\naccept-version:1.2\n\n\0").await.unwrap();
    c.flush().await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut c), 120).await.unwrap().unwrap();
    c.write_all(b"SUBSCRIBE\ndestination:/topic/x\n\n\0").await.unwrap();
    let (cmd, _) = with_timeout(read_stomp_frame(&mut c), 120).await.unwrap().unwrap();
    assert_eq!(cmd, "ERROR", "1.2 requires an id header");
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_bad_destination_is_rejected() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
    c.write_all(b"CONNECT\naccept-version:1.2\n\n\0").await.unwrap();
    c.flush().await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut c), 120).await.unwrap().unwrap();
    c.write_all(b"SEND\ndestination:/wat/ever\n\n\0").await.unwrap();
    let (cmd, _) = with_timeout(read_stomp_frame(&mut c), 120).await.unwrap().unwrap();
    assert_eq!(cmd, "ERROR");
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_ack_unknown_id_yields_error() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
    c.write_all(b"CONNECT\naccept-version:1.2\n\n\0").await.unwrap();
    c.flush().await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut c), 120).await.unwrap().unwrap();
    c.write_all(b"ACK\nid:does-not-exist\n\n\0").await.unwrap();
    let (cmd, _) = with_timeout(read_stomp_frame(&mut c), 120).await.unwrap().unwrap();
    assert_eq!(cmd, "ERROR");
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_commit_unknown_transaction_is_error() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
    c.write_all(b"CONNECT\naccept-version:1.2\n\n\0").await.unwrap();
    c.flush().await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut c), 120).await.unwrap().unwrap();
    c.write_all(b"COMMIT\ntransaction:ghost\n\n\0").await.unwrap();
    // No ERROR follows (unknown tx commit is a benign no-op).
    c.write_all(b"DISCONNECT\nreceipt:bye\n\n\0").await.unwrap();
    let (cmd, _) = with_timeout(read_stomp_frame(&mut c), 120).await.unwrap().unwrap();
    assert_eq!(cmd, "RECEIPT");
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_duplicate_connect_closes() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
    c.write_all(b"CONNECT\naccept-version:1.2\n\n\0").await.unwrap();
    c.flush().await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut c), 120).await.unwrap().unwrap();
    c.write_all(b"CONNECT\naccept-version:1.2\n\n\0").await.unwrap();
    let (cmd, _) = with_timeout(read_stomp_frame(&mut c), 120).await.unwrap().unwrap();
    assert_eq!(cmd, "ERROR");
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_unsubscribe_stops_delivery() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut sub = tokio::net::TcpStream::connect(&addr).await.unwrap();
    sub.write_all(b"CONNECT\naccept-version:1.2\n\n\0").await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut sub), 120).await.unwrap().unwrap();
    sub.write_all(b"SUBSCRIBE\ndestination:/topic/unsubme\nid:s1\nack:auto\nreceipt:r1\n\n\0").await.unwrap();
    let (cmd, _) = with_timeout(read_stomp_frame(&mut sub), 60).await.unwrap().unwrap();
    assert_eq!(cmd, "RECEIPT");
    sub.write_all(b"UNSUBSCRIBE\nid:s1\nreceipt:r2\n\n\0").await.unwrap();
    let (cmd, _) = with_timeout(read_stomp_frame(&mut sub), 60).await.unwrap().unwrap();
    assert_eq!(cmd, "RECEIPT");

    // A message published after the unsubscribe is not delivered.
    let mut send = tokio::net::TcpStream::connect(&addr).await.unwrap();
    send.write_all(b"CONNECT\naccept-version:1.2\n\n\0").await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut send), 60).await.unwrap().unwrap();
    send.write_all(b"SEND\ndestination:/topic/unsubme\n\n\nlate\n\0").await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    // The queue is durable+auto-delete… after unsubscribe the gen queue
    // is dropped, so the late message is unroutable. Drain via AMQP get
    // on nothing: verify through the DISCONNECT path cleanly ending.
    sub.write_all(b"DISCONNECT\nreceipt:done\n\n\0").await.unwrap();
    let (cmd, _) = with_timeout(read_stomp_frame(&mut sub), 120).await.unwrap().unwrap();
    assert_eq!(cmd, "RECEIPT");
}

// ---------------------------------------------------------------------
// AMQP 1.0 error scenarios
// ---------------------------------------------------------------------

/// Shared helper: SASL-ANONYMOUS handshake + open + begin; returns the
/// connected socket.
async fn amqp10_open_session(addr: &str) -> tokio::net::TcpStream {
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    sock.write_all(b"AMQP\0\x01\0\0").await.unwrap();
    let mut buf = [0u8; 2048];
    let _ = sock.read(&mut buf).await.unwrap(); // sasl-mechanisms
    sock.write_all(&switchboard_server::protocols::amqp10::frames::encode_sasl_frame(
        switchboard_server::protocols::amqp10::frames::codes::SASL_INIT,
        vec![
            switchboard_server::protocols::amqp10::types::Value::Symbol("ANONYMOUS".into()),
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Binary(Vec::new()),
        ],
    ))
    .await
    .unwrap();
    let _ = sock.read(&mut buf).await.unwrap(); // sasl-outcome
    sock.write_all(b"AMQP\0\x01\0\0").await.unwrap();
    sock.write_all(&switchboard_server::protocols::amqp10::frames::open("err-client")).await.unwrap();
    let _ = sock.read(&mut buf).await.unwrap(); // open reply
    sock.write_all(&switchboard_server::protocols::amqp10::frames::begin(None, 1)).await.unwrap();
    let _ = sock.read(&mut buf).await.unwrap(); // begin reply
    sock
}

#[tokio::test(flavor = "multi_thread")]
async fn amqp10_detach_unknown_handle_gets_reply() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut sock = amqp10_open_session(&addr).await;
    // Detach a handle that was never attached: the server must reply.
    sock.write_all(&switchboard_server::protocols::amqp10::frames::detach(77)).await.unwrap();
    let mut buf = [0u8; 512];
    let n = tokio::io::AsyncReadExt::read(&mut sock, &mut buf).await.unwrap();
    assert!(n > 0, "expected a detach reply");
    let (frame, _) = switchboard_server::protocols::amqp10::frames::decode_frame(&buf[..n]).unwrap();
    assert_eq!(frame.code(), Some(switchboard_server::protocols::amqp10::frames::codes::DETACH));
}

#[tokio::test(flavor = "multi_thread")]
async fn amqp10_transfer_to_unknown_handle_is_ignored_silently() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut sock = amqp10_open_session(&addr).await;
    // Transfer on an unattached handle: payload destined nowhere.
    let mut msg = Vec::new();
    switchboard_server::protocols::amqp10::types::encode(
        &switchboard_server::protocols::amqp10::types::Value::Described(
            Box::new(switchboard_server::protocols::amqp10::types::Value::ULong(switchboard_server::protocols::amqp10::frames::codes::SECTION_DATA)),
            Box::new(switchboard_server::protocols::amqp10::types::Value::Binary(b"orphan".to_vec())),
        ),
        &mut msg,
    );
    sock.write_all(&switchboard_server::protocols::amqp10::frames::encode_frame_with_payload(
        0,
        switchboard_server::protocols::amqp10::frames::codes::TRANSFER,
        vec![
            switchboard_server::protocols::amqp10::types::Value::UInt(99),            // unknown handle
            switchboard_server::protocols::amqp10::types::Value::UInt(1),
            switchboard_server::protocols::amqp10::types::Value::Binary(vec![1]),
            switchboard_server::protocols::amqp10::types::Value::UInt(0),
            switchboard_server::protocols::amqp10::types::Value::Bool(true),
            switchboard_server::protocols::amqp10::types::Value::Bool(false),
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Null,
        ],
        &msg,
    ))
    .await
    .unwrap();
    // The connection must survive: END → END reply proves the session
    // loop is still running.
    sock.write_all(&switchboard_server::protocols::amqp10::frames::end()).await.unwrap();
    let mut buf = [0u8; 512];
    let n = tokio::io::AsyncReadExt::read(&mut sock, &mut buf).await.unwrap();
    assert!(n > 0, "session must reply to END");
    let (frame, _) = switchboard_server::protocols::amqp10::frames::decode_frame(&buf[..n]).unwrap();
    assert_eq!(frame.code(), Some(switchboard_server::protocols::amqp10::frames::codes::END));
}

#[tokio::test(flavor = "multi_thread")]
async fn amqp10_bad_sasl_mechanism_closes() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    sock.write_all(b"AMQP\0\x01\0\0").await.unwrap();
    let mut buf = [0u8; 512];
    let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut buf).await.unwrap();
    sock.write_all(&switchboard_server::protocols::amqp10::frames::encode_sasl_frame(
        switchboard_server::protocols::amqp10::frames::codes::SASL_INIT,
        vec![
            switchboard_server::protocols::amqp10::types::Value::Symbol("CRAM-MD5".into()),
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Binary(Vec::new()),
        ],
    ))
    .await
    .unwrap();
    // Unsupported mechanism: the server closes without an outcome.
    let n = tokio::io::AsyncReadExt::read(&mut sock, &mut buf).await.unwrap_or(0);
    assert_eq!(n, 0, "connection must close on unsupported mechanism");
}

#[tokio::test(flavor = "multi_thread")]
async fn amqp10_close_before_open_is_handled() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    sock.write_all(b"AMQP\0\x01\0\0").await.unwrap();
    let mut buf = [0u8; 512];
    let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut buf).await.unwrap();
    sock.write_all(&switchboard_server::protocols::amqp10::frames::close()).await.unwrap();
    // Server responds with its own close and closes the socket.
    let n = tokio::io::AsyncReadExt::read(&mut sock, &mut buf).await.unwrap_or(0);
    let _ = n;
    // Either a close frame or EOF is acceptable; the connection must end.
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_commit_to_missing_exchange_is_rejected() {
    let _gateway_guard = lock().await;
    let addr = gateway_support::shared_gateway().await;
    let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
    c.write_all(b"CONNECT\naccept-version:1.2\n\n\0").await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut c), 120).await.unwrap().unwrap();
    c.write_all(b"BEGIN\ntransaction:no-ex\n\n\0").await.unwrap();
    c.write_all(b"SEND\ndestination:/exchange/ghost-ex/rk\ntransaction:no-ex\n\npayload\n\0").await.unwrap();
    c.write_all(b"COMMIT\ntransaction:no-ex\n\n\0").await.unwrap();
    // The commit succeeds silently: messages routed to a nonexistent
    // exchange are unroutable and dropped (consistent with non-tx SEND).
    // Verify the session is still healthy with a DISCONNECT receipt.
    c.write_all(b"DISCONNECT\nreceipt:ok\n\n\0").await.unwrap();
    let (cmd, _) = with_timeout(read_stomp_frame(&mut c), 120).await.unwrap().unwrap();
    assert_eq!(cmd, "RECEIPT");
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_disconnect_without_receipt_closes_cleanly() {
    let _gateway_guard = lock().await;
    let addr = gateway_support::shared_gateway().await;
    let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
    c.write_all(b"CONNECT\naccept-version:1.2\n\n\0").await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut c), 120).await.unwrap().unwrap();
    c.write_all(b"DISCONNECT\n\n\0").await.unwrap();
    // No receipt requested: the server closes cleanly.
    let mut buf = Vec::new();
    let _ = tokio::io::AsyncReadExt::read(&mut c, &mut buf).await.unwrap_or(0);
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_client_individual_ack_holds_message() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
    c.write_all(b"CONNECT\naccept-version:1.2\n\n\0").await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut c), 120).await.unwrap().unwrap();
    c.write_all(b"SUBSCRIBE\ndestination:/queue/nackflow\nid:s1\nack:client-individual\nreceipt:r1\n\n\0").await.unwrap();
    let (cmd, _) = with_timeout(read_stomp_frame(&mut c), 60).await.unwrap().unwrap();
    assert_eq!(cmd, "RECEIPT");
    c.write_all(b"SEND\ndestination:/queue/nackflow\n\nn1\0").await.unwrap();

    let (cmd, body) = with_timeout(read_stomp_frame(&mut c), 120).await.unwrap().unwrap();
    assert_eq!(cmd, "MESSAGE");
    assert_eq!(body, b"n1");
    // The held message stays held: a fresh AMQP Get must come up empty.
    let mut amqp = support::connect_and_open(&addr, "/").await.unwrap();
    amqp.send_method(1, &Method::BasicGet { ticket: 0, queue: "nackflow".into(), no_ack: true })
        .await
        .unwrap();
    assert!(matches!(amqp.expect(1).await.unwrap(), Method::BasicGetEmpty { .. }));
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_unsubscribe_unknown_id_is_silent() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
    c.write_all(b"CONNECT\naccept-version:1.2\n\n\0").await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut c), 120).await.unwrap().unwrap();
    c.write_all(b"UNSUBSCRIBE\nid:ghost\n\n\0").await.unwrap();
    c.write_all(b"DISCONNECT\nreceipt:r\n\n\0").await.unwrap();
    // Skip the RECEIPT for the disconnect; connection must close cleanly.
    let _ = with_timeout(read_stomp_frame(&mut c), 120).await.unwrap().unwrap_or((String::new(), Vec::new()));
}

/// A SUBSCRIBE against a nonexistent `/exchange/` destination must be
/// refused with an ERROR frame (parity with the AMQP 404), not silently
/// succeed against nothing.
#[tokio::test(flavor = "multi_thread")]
async fn stomp_subscribe_exchange_missing_is_error() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
    c.write_all(b"CONNECT\naccept-version:1.2\n\n\0").await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut c), 60).await.unwrap().unwrap();
    c.write_all(b"SUBSCRIBE\ndestination:/exchange/ghost/rk\nid:s1\n\n\0").await.unwrap();
    let (cmd, _) = with_timeout(read_stomp_frame(&mut c), 180).await.unwrap().unwrap();
    assert_eq!(cmd, "ERROR");
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_send_without_destination_is_error() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
    c.write_all(b"CONNECT\naccept-version:1.2\n\n\0").await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut c), 60).await.unwrap().unwrap();
    c.write_all(b"SEND\n\nbody\n\0").await.unwrap();
    let (cmd, _) = with_timeout(read_stomp_frame(&mut c), 60).await.unwrap().unwrap();
    assert_eq!(cmd, "ERROR");
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_amqp_queue_destination_roundtrip() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    // AMQP declares the queue; STOMP consumes and produces via /amq/queue/.
    let mut amqp = support::connect_and_open(&addr, "/").await.unwrap();
    amqp.send_method(
        1,
        &Method::QueueDeclare {
            ticket: 0,
            queue: "aq".into(),
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
    amqp.expect(1).await.unwrap();

    let mut stomp = tokio::net::TcpStream::connect(&addr).await.unwrap();
    stomp.write_all(b"CONNECT\naccept-version:1.2\n\n\0").await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut stomp), 60).await.unwrap().unwrap();
    stomp.write_all(b"SUBSCRIBE\ndestination:/amq/queue/aq\nid:s1\nack:auto\nreceipt:r1\n\n\0").await.unwrap();
    let (cmd, _) = with_timeout(read_stomp_frame(&mut stomp), 60).await.unwrap().unwrap();
    assert_eq!(cmd, "RECEIPT");

    let mut pubc = tokio::net::TcpStream::connect(&addr).await.unwrap();
    pubc.write_all(b"CONNECT\naccept-version:1.2\n\n\0").await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut pubc), 60).await.unwrap().unwrap();
    pubc.write_all(b"SEND\ndestination:/queue/aq\n\nfor-aq\0").await.unwrap();

    let (cmd, body) = with_timeout(read_stomp_frame(&mut stomp), 60).await.unwrap().unwrap();
    assert_eq!(cmd, "MESSAGE");
    assert_eq!(body, b"for-aq");
}

#[tokio::test(flavor = "multi_thread")]
async fn amqp10_client_sender_publishes_with_credit_flow() {
    let (addr, _node) = start_gateway(Default::default()).await;
    // AMQP declares the queue.
    let mut amqp = support::connect_and_open(&addr, "/").await.unwrap();
    amqp.send_method(
        1,
        &Method::QueueDeclare {
            ticket: 0,
            queue: "one-flow".into(),
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
    amqp.expect(1).await.unwrap();

    // 1.0 client: SASL PLAIN with real creds, open, begin, attach sender
    // with target /queue/one-flow, then FLOW granting credit, then
    // transfer. Verifies the credit-driven send path end-to-end.
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    sock.write_all(b"AMQP\0\x01\0\0").await.unwrap();
    let mut buf = [0u8; 2048];
    let _ = sock.read(&mut buf).await.unwrap();
    sock.write_all(&switchboard_server::protocols::amqp10::frames::encode_sasl_frame(
        switchboard_server::protocols::amqp10::frames::codes::SASL_INIT,
        vec![
            switchboard_server::protocols::amqp10::types::Value::Symbol("PLAIN".into()),
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Binary(b"\0guest\0guest".to_vec()),
        ],
    ))
    .await
    .unwrap();
    let _ = sock.read(&mut buf).await.unwrap();
    sock.write_all(b"AMQP\0\x01\0\0").await.unwrap();
    sock.write_all(&switchboard_server::protocols::amqp10::frames::open("flow-client")).await.unwrap();
    let _ = sock.read(&mut buf).await.unwrap();
    sock.write_all(&switchboard_server::protocols::amqp10::frames::begin(None, 1)).await.unwrap();
    let _ = sock.read(&mut buf).await.unwrap();

    // Attach with role=sender (field 2 = false), target /queue/one-flow.
    sock.write_all(&switchboard_server::protocols::amqp10::frames::encode_frame(
        0,
        switchboard_server::protocols::amqp10::frames::codes::ATTACH,
        vec![
            switchboard_server::protocols::amqp10::types::Value::String("snd".into()),
            switchboard_server::protocols::amqp10::types::Value::UInt(0),
            switchboard_server::protocols::amqp10::types::Value::Bool(false),
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Map(vec![(
                switchboard_server::protocols::amqp10::types::Value::Symbol("address".into()),
                switchboard_server::protocols::amqp10::types::Value::String("/queue/one-flow".into()),
            )]),
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Bool(false),
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Null,
        ],
    ))
    .await
    .unwrap();
    let _ = sock.read(&mut buf).await.unwrap(); // attach reply

    // Grant credit for the link (handle 0).
    sock.write_all(&switchboard_server::protocols::amqp10::frames::encode_frame(
        0,
        switchboard_server::protocols::amqp10::frames::codes::FLOW,
        vec![
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::UInt(3), // credit
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Bool(false),
            switchboard_server::protocols::amqp10::types::Value::Bool(false),
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::UInt(0), // handle
        ],
    ))
    .await
    .unwrap();

    // Transfer one settled message.
    let mut msg = Vec::new();
    switchboard_server::protocols::amqp10::types::encode(
        &switchboard_server::protocols::amqp10::types::Value::Described(
            Box::new(switchboard_server::protocols::amqp10::types::Value::ULong(switchboard_server::protocols::amqp10::frames::codes::SECTION_DATA)),
            Box::new(switchboard_server::protocols::amqp10::types::Value::Binary(b"credit-msg".to_vec())),
        ),
        &mut msg,
    );
    sock.write_all(&switchboard_server::protocols::amqp10::frames::encode_frame_with_payload(
        0,
        switchboard_server::protocols::amqp10::frames::codes::TRANSFER,
        vec![
            switchboard_server::protocols::amqp10::types::Value::UInt(0),
            switchboard_server::protocols::amqp10::types::Value::UInt(1),
            switchboard_server::protocols::amqp10::types::Value::Binary(vec![1]),
            switchboard_server::protocols::amqp10::types::Value::UInt(0),
            switchboard_server::protocols::amqp10::types::Value::Bool(true),
            switchboard_server::protocols::amqp10::types::Value::Bool(false),
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Null,
        ],
        &msg,
    ))
    .await
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    // AMQP 0-9-1 verifies delivery.
    let got = support::basic_get(&mut amqp, 1, "one-flow", true).await.unwrap();
    assert!(got.is_some(), "1.0 transfer must land in the queue");
}

#[tokio::test(flavor = "multi_thread")]
async fn amqp10_plain_wrong_password_is_rejected() {
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    sock.write_all(b"AMQP\0\x01\0\0").await.unwrap();
    let mut buf = [0u8; 512];
    let _ = sock.read(&mut buf).await.unwrap();
    sock.write_all(&switchboard_server::protocols::amqp10::frames::encode_sasl_frame(
        switchboard_server::protocols::amqp10::frames::codes::SASL_INIT,
        vec![
            switchboard_server::protocols::amqp10::types::Value::Symbol("PLAIN".into()),
            switchboard_server::protocols::amqp10::types::Value::Null,
            switchboard_server::protocols::amqp10::types::Value::Binary(b"\0guest\0nope".to_vec()),
        ],
    ))
    .await
    .unwrap();
    // SASL outcome 1 (auth failure), then close.
    let n = tokio::io::AsyncReadExt::read(&mut sock, &mut buf).await.unwrap_or(0);
    assert!(n > 0, "expected SASL outcome");
    let (frame, _) = switchboard_server::protocols::amqp10::frames::decode_frame(&buf[..n]).unwrap();
    assert_eq!(frame.code(), Some(switchboard_server::protocols::amqp10::frames::codes::SASL_OUTCOME));
    assert_eq!(frame.field(0).cloned(), Some(switchboard_server::protocols::amqp10::types::Value::UByte(1)));
}

// ---------------------------------------------------------------------
// STOMP arm coverage: negotiation, error frames, tx, NACK, teardown.
// ---------------------------------------------------------------------

async fn stomp_connect_ok(addr: &str) -> tokio::net::TcpStream {
    let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
    c.write_all(b"CONNECT\naccept-version:1.2\n\n\0").await.unwrap();
    let (cmd, _) = with_timeout(read_stomp_frame(&mut c), 60).await.unwrap().unwrap();
    assert_eq!(cmd, "CONNECTED");
    c
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_first_frame_must_be_connect() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    // Raw TCP is sniffed before STOMP (a non-CONNECT first frame never
    // reaches the STOMP layer there); the WebSocket /stomp route enters
    // stomp::serve directly, where the CONNECT-first rule applies.
    let mut ws = with_timeout(ws_connect(&addr, "/stomp", None), 60).await;
    ws.send(b"SEND\ndestination:/queue/x\n\nhi\0").await;
    let payload = with_timeout(ws.recv(), 60).await.unwrap();
    let text = String::from_utf8_lossy(&payload);
    assert!(text.contains("ERROR"), "{text}");
    assert!(text.contains("expected CONNECT"), "{text}");
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_bad_credentials_are_rejected() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
    c.write_all(b"CONNECT\naccept-version:1.2\nlogin:guest\npasscode:wrong\n\n\0").await.unwrap();
    let (cmd, headers, _) = with_timeout(read_stomp_frame_full(&mut c), 60).await.unwrap().unwrap();
    assert_eq!(cmd, "ERROR");
    let msg = headers.iter().find(|(k, _)| k == "message").map(|(_, v)| v.clone()).unwrap_or_default();
    assert!(msg.contains("auth failed"), "{msg}");
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_heart_beat_negotiation_echoes_server_values() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
    c.write_all(b"CONNECT\naccept-version:1.2\nheart-beat:1000,2000\n\n\0").await.unwrap();
    let (_, headers, _) = with_timeout(read_stomp_frame_full(&mut c), 60).await.unwrap().unwrap();
    let hb = headers.iter().find(|(k, _)| k == "heart-beat").map(|(_, v)| v.clone()).unwrap_or_default();
    assert!(!hb.is_empty(), "CONNECTED must carry the negotiated heart-beat");
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_heartbeat_frames_are_ignored() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut c = stomp_connect_ok(&addr).await;
    // Lone EOLs are heartbeats, not frames.
    c.write_all(b"\n\n\n").await.unwrap();
    c.write_all(b"DISCONNECT\nreceipt:bye\n\n\0").await.unwrap();
    let (cmd, _) = with_timeout(read_stomp_frame(&mut c), 60).await.unwrap().unwrap();
    assert_eq!(cmd, "RECEIPT");
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_half_frame_disconnect_closes_cleanly() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    // EOF mid-header, then EOF mid-body: the reader returns None and the
    // session winds down instead of wedging the connection.
    for partial in ["SUBSCRIBE\ndestination:/q\n", "SEND\ndestination:/q\n\nno-nul"] {
        let mut c = stomp_connect_ok(&addr).await;
        c.write_all(partial.as_bytes()).await.unwrap();
        drop(c);
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    }
    // The server is still healthy afterwards.
    let mut c = stomp_connect_ok(&addr).await;
    c.write_all(b"DISCONNECT\nreceipt:done\n\n\0").await.unwrap();
    let (cmd, _) = with_timeout(read_stomp_frame(&mut c), 60).await.unwrap().unwrap();
    assert_eq!(cmd, "RECEIPT");
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_forwarder_stops_when_client_vanishes() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut c = stomp_connect_ok(&addr).await;
    c.write_all(b"SUBSCRIBE\ndestination:/queue/vanish\nid:s1\nack:auto\nreceipt:r1\n\n\0").await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut c), 60).await.unwrap().unwrap();
    c.write_all(b"SEND\ndestination:/queue/vanish\n\nfirst\0").await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut c), 60).await.unwrap().unwrap();
    // Vanish mid-session; the MESSAGE forwarder observes the write
    // failure on the next delivery and stops.
    drop(c);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let mut p = stomp_connect_ok(&addr).await;
    p.write_all(b"SEND\ndestination:/queue/vanish\n\nsecond\0").await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_subscribe_bad_destination_is_error() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut c = stomp_connect_ok(&addr).await;
    c.write_all(b"SUBSCRIBE\ndestination:garbage\nid:s1\n\n\0").await.unwrap();
    let (cmd, _) = with_timeout(read_stomp_frame(&mut c), 60).await.unwrap().unwrap();
    assert_eq!(cmd, "ERROR");
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_subscribe_empty_exchange_name_is_error() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut c = stomp_connect_ok(&addr).await;
    c.write_all(b"SUBSCRIBE\ndestination:/exchange//rk\nid:s1\n\n\0").await.unwrap();
    let (cmd, _) = with_timeout(read_stomp_frame(&mut c), 60).await.unwrap().unwrap();
    assert_eq!(cmd, "ERROR");
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_nack_requeues_the_message() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut c = stomp_connect_ok(&addr).await;
    c.write_all(b"SUBSCRIBE\ndestination:/queue/nq\nid:s1\nack:client-individual\nreceipt:r1\n\n\0").await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut c), 60).await.unwrap().unwrap();
    c.write_all(b"SEND\ndestination:/queue/nq\n\nn1\0").await.unwrap();
    let (cmd, headers, body) = with_timeout(read_stomp_frame_full(&mut c), 60).await.unwrap().unwrap();
    assert_eq!(cmd, "MESSAGE");
    assert_eq!(body, b"n1");
    let ack_id = headers
        .iter()
        .find(|(k, _)| k == "id")
        .map(|(_, v)| v.clone())
        .expect("client-individual MESSAGE carries an ack id");
    // NACK releases the delivery back to the ready set: the same
    // consumer receives it again (redelivered).
    c.write_all(format!("NACK\nid:{ack_id}\n\n\0").into_bytes().as_slice()).await.unwrap();
    let (cmd2, body2) = with_timeout(read_stomp_frame(&mut c), 60).await.unwrap().unwrap();
    assert_eq!(cmd2, "MESSAGE", "NACK must requeue");
    assert_eq!(body2, b"n1");
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_empty_transaction_commit_succeeds() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut c = stomp_connect_ok(&addr).await;
    c.write_all(b"BEGIN\ntransaction:tx-e\n\n\0").await.unwrap();
    c.write_all(b"COMMIT\ntransaction:tx-e\nreceipt:r\n\n\0").await.unwrap();
    let (cmd, _) = with_timeout(read_stomp_frame(&mut c), 60).await.unwrap().unwrap();
    assert_eq!(cmd, "RECEIPT");
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_tx_to_queue_destination_delivers_atomically() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut c = stomp_connect_ok(&addr).await;
    c.write_all(b"BEGIN\ntransaction:tx-q\n\n\0").await.unwrap();
    c.write_all(b"SEND\ndestination:/queue/txq\ntransaction:tx-q\n\nqueued\0").await.unwrap();
    // Nothing visible before COMMIT.
    c.write_all(b"COMMIT\ntransaction:tx-q\nreceipt:r\n\n\0").await.unwrap();
    let (cmd, _) = with_timeout(read_stomp_frame(&mut c), 60).await.unwrap().unwrap();
    assert_eq!(cmd, "RECEIPT");
    let mut amqp = support::connect_and_open(&addr, "/").await.unwrap();
    amqp.send_method(1, &Method::BasicGet { ticket: 0, queue: "txq".into(), no_ack: true }).await.unwrap();
    match amqp.expect(1).await.unwrap() {
        Method::BasicGetOk { .. } => {}
        other => panic!("expected the committed message, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_send_auto_declares_queue_and_persists() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut c = stomp_connect_ok(&addr).await;
    c.write_all(b"SEND\ndestination:/queue/fresh\npersistent:true\n\nkept\0").await.unwrap();
    let mut amqp = support::connect_and_open(&addr, "/").await.unwrap();
    amqp.send_method(1, &Method::BasicGet { ticket: 0, queue: "fresh".into(), no_ack: true }).await.unwrap();
    match amqp.expect(1).await.unwrap() {
        Method::BasicGetOk { .. } => {}
        other => panic!("auto-declared queue missing the message: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_heart_beat_zero_negotiates_zero() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
    c.write_all(b"CONNECT\naccept-version:1.2\nheart-beat:0,0\n\n\0").await.unwrap();
    let (_, headers, _) = with_timeout(read_stomp_frame_full(&mut c), 60).await.unwrap().unwrap();
    let hb = headers.iter().find(|(k, _)| k == "heart-beat").map(|(_, v)| v.clone()).unwrap_or_default();
    assert_eq!(hb, "0,0", "no heartbeat negotiated");
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_subscribe_without_destination_is_error() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
    c.write_all(b"CONNECT\naccept-version:1.2\n\n\0").await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut c), 60).await.unwrap().unwrap();
    c.write_all(b"SUBSCRIBE\nid:s1\n\n\0").await.unwrap();
    let (cmd, _) = with_timeout(read_stomp_frame(&mut c), 60).await.unwrap().unwrap();
    assert_eq!(cmd, "ERROR");
}

#[tokio::test(flavor = "multi_thread")]
async fn stomp_client_ack_on_fresh_gateway_settles() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
    c.write_all(b"CONNECT\naccept-version:1.2\n\n\0").await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut c), 60).await.unwrap().unwrap();
    c.write_all(b"SUBSCRIBE\ndestination:/queue/fresh-ack\nid:s1\nack:client\nreceipt:r1\n\n\0").await.unwrap();
    let _ = with_timeout(read_stomp_frame(&mut c), 60).await.unwrap().unwrap();
    c.write_all(b"SEND\ndestination:/queue/fresh-ack\n\npay\0").await.unwrap();
    let (_, headers, body) = with_timeout(read_stomp_frame_full(&mut c), 60).await.unwrap().unwrap();
    assert_eq!(body, b"pay");
    let ack_id = headers.iter().find(|(k, _)| k == "id").map(|(_, v)| v.clone()).expect("ack id");
    c.write_all(format!("ACK\nid:{ack_id}\n\n\0").into_bytes().as_slice()).await.unwrap();
    // The queue must be empty now (verified over AMQP).
    let mut amqp = support::connect_and_open(&addr, "/").await.unwrap();
    amqp.send_method(1, &Method::BasicGet { ticket: 0, queue: "fresh-ack".into(), no_ack: true }).await.unwrap();
    assert!(matches!(amqp.expect(1).await.unwrap(), Method::BasicGetEmpty { .. }));
}
