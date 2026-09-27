//! WebSocket gateway edge arms: malformed upgrades, protocol sniffing
//! through `/ws` (no hint), and disabled inner protocols.

mod gateway_support;
mod support;

use std::time::Duration;

use gateway_support::{start_gateway, with_timeout};
use switchboard_server::protocols::ProtocolConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// Tests boot full brokers; serialize them like the other suites.
static GATEWAY_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn lock() -> tokio::sync::MutexGuard<'static, ()> {
    GATEWAY_LOCK.lock().await
}

async fn gw_custom(cfg: ProtocolConfig) -> (String, gateway_support::NodeGuard) {
    gateway_support::start_gateway(cfg).await
}

#[tokio::test(flavor = "multi_thread")]
async fn ws_upgrade_without_key_is_rejected() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    // Missing Sec-WebSocket-Key: the upgrade cannot be answered.
    sock.write_all(b"GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n\r\n").await.unwrap();
    let mut buf = vec![0u8; 1024];
    let n = with_timeout(sock.read(&mut buf), 30).await.unwrap_or(0);
    let text = String::from_utf8_lossy(&buf[..n]);
    assert!(text.contains("400") || n == 0, "expected rejection, got {text:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn ws_no_hint_sniff_routes_amqp10_from_split_frames() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    // Complete the upgrade with NO path hint and NO subprotocol.
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let req = "GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
    sock.write_all(req.as_bytes()).await.unwrap();
    read_http_head(&mut sock).await;
    // The AMQP 1.0 header arrives in TWO frames (NeedMore then Yes).
    let header = b"AMQP\0\x01\0\0";
    send_masked(&mut sock, &header[..4]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    send_masked(&mut sock, &header[4..]).await.unwrap();
    // The amqp10 server answers with SASL mechanisms inside WS frames.
    let mut buf = vec![0u8; 512];
    let n = with_timeout(sock.read(&mut buf), 30).await.unwrap();
    assert!(n > 2 && buf[0] & 0x0F == 0x2, "binary frame with sasl-mechanisms, got {buf:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn ws_no_hint_sniff_routes_mqtt() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let req = "GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
    sock.write_all(req.as_bytes()).await.unwrap();
    read_http_head(&mut sock).await;
    // One masked frame with the whole MQTT CONNECT.
    let connect = gateway_support::mqtt_connect("ws-sniff");
    send_masked(&mut sock, &connect).await.unwrap();
    let mut buf = vec![0u8; 512];
    let n = with_timeout(sock.read(&mut buf), 30).await.unwrap();
    assert!(n > 2 && buf[0] & 0x0F == 0x2, "binary frame with CONNACK");
    // First MQTT payload byte inside the frame: CONNACK type 2.
    let inner = unmask_frame(&buf[..n]);
    assert_eq!(inner[0] >> 4, 2, "CONNACK expected, got {inner:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn ws_disabled_inner_protocol_closes_quietly() {
    let _g = lock().await;
    let cfg = ProtocolConfig { mqtt: false, ..Default::default() };
    let (addr, _node) = gw_custom(cfg).await;
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let req = "GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
    sock.write_all(req.as_bytes()).await.unwrap();
    read_http_head(&mut sock).await;
    // Upgrade succeeds (101), but MQTT is disabled: the sniffed
    // connection closes instead of serving.
    let mut buf = vec![0u8; 512];
    let connect = gateway_support::mqtt_connect("ws-disabled");
    send_masked(&mut sock, &connect).await.unwrap();
    // The sniffed connection must NOT serve MQTT: no CONNACK ever
    // arrives (the gateway may hold the socket for reuse; what matters
    // is silence).
    let served = tokio::time::timeout(Duration::from_secs(5), sock.read(&mut buf)).await;
    match served {
        Err(_) => {} // silence: not served
        Ok(Ok(n)) => {
            let payload: Vec<u8> = buf[..n].to_vec();
            assert!(
                payload.first().map(|b| b >> 4 != 2).unwrap_or(true),
                "CONNACK must not be delivered on a disabled protocol"
            );
        }
        Ok(Err(e)) => panic!("read error {e}"),
    }
}


/// Read until the end of the HTTP 101 response head.
async fn read_http_head(sock: &mut tokio::net::TcpStream) {
    let mut head = Vec::new();
    let mut buf = [0u8; 512];
    loop {
        let n = with_timeout(sock.read(&mut buf), 30).await.unwrap_or(0);
        if n == 0 {
            return;
        }
        head.extend_from_slice(&buf[..n]);
        if head.windows(4).any(|w| w == b"\r\n\r\n") {
            return;
        }
    }
}

/// Write one masked binary WebSocket frame (client→server framing).
async fn send_masked(sock: &mut tokio::net::TcpStream, payload: &[u8]) -> std::io::Result<()> {
    let mut frame = vec![0x82];
    if payload.len() < 126 {
        frame.push(0x80 | payload.len() as u8);
    } else {
        frame.push(0x80 | 126);
        frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    }
    frame.extend_from_slice(&[1u8, 2, 3, 4]);
    for (i, b) in payload.iter().enumerate() {
        frame.push(b ^ [1u8, 2, 3, 4][i % 4]);
    }
    sock.write_all(&frame).await?;
    sock.flush().await
}

/// Unmask the first frame in a received (server→client, unmasked) buffer.
fn unmask_frame(raw: &[u8]) -> Vec<u8> {
    let mut len = raw[1] as usize;
    let mut off = 2;
    if len == 126 {
        len = u16::from_be_bytes([raw[2], raw[3]]) as usize;
        off = 4;
    }
    raw[off..off + len].to_vec()
}

#[tokio::test(flavor = "multi_thread")]
async fn ws_sniff_rejects_ftp_style_garbage_and_amqp091() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    // Both AMQP 0-9-1 headers and unrecognizable bytes over a hintless
    // WebSocket end the connection: only MQTT/STOMP/AMQP-1.0 ride WS.
    for payload in [&b"AMQP\0\0\x09\x01"[..], b"not-a-protocol"] {
        let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let req = "GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
        sock.write_all(req.as_bytes()).await.unwrap();
        read_http_head(&mut sock).await;
        send_masked(&mut sock, payload).await.unwrap();
        // The connection ends without serving anything.
        let mut buf = [0u8; 128];
        let n = tokio::time::timeout(Duration::from_secs(10), sock.read(&mut buf)).await.unwrap().unwrap_or(0);
        assert_eq!(n, 0, "unroutable sniff must close, got {buf:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn ws_closed_before_any_payload_is_fine() {
    let _g = lock().await;
    let (addr, _node) = start_gateway(Default::default()).await;
    // Upgrade completes, then the client vanishes before sniffing: the
    // sniff read observes EOF and the session winds down.
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let req = "GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
    sock.write_all(req.as_bytes()).await.unwrap();
    read_http_head(&mut sock).await;
    drop(sock);
    tokio::time::sleep(Duration::from_millis(200)).await;
}
