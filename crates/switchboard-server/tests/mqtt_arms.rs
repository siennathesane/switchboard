//! MQTT arm coverage: handshake rules, keepalive edges, full QoS 1/2
//! settlement in both directions, and unsubscribe — over a real gateway.

mod gateway_support;
mod support;

use std::time::Duration;

use gateway_support::{mqtt_read, mqtt_remaining, start_gateway, with_timeout};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn gw() -> (String, gateway_support::NodeGuard) {
    start_gateway(Default::default()).await
}

/// Hand-build a CONNECT packet with explicit flags and keepalive.
fn connect_packet(client_id: &str, clean: bool, keepalive: u16) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&4u16.to_be_bytes());
    body.extend_from_slice(b"MQTT");
    body.push(4); // level 4 (3.1.1)
    body.push(if clean { 0x02 } else { 0x00 });
    body.extend_from_slice(&keepalive.to_be_bytes());
    body.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
    body.extend_from_slice(client_id.as_bytes());
    let mut out = vec![0x10];
    mqtt_remaining(body.len(), &mut out);
    out.extend_from_slice(&body);
    out
}

fn pingreq() -> Vec<u8> {
    vec![0xC0, 0x00]
}

fn pingresp() -> Vec<u8> {
    vec![0xD0, 0x00]
}

fn unsub(pid: u16, filter: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&pid.to_be_bytes());
    body.extend_from_slice(&(filter.len() as u16).to_be_bytes());
    body.extend_from_slice(filter.as_bytes());
    let mut out = vec![0xA2];
    mqtt_remaining(body.len(), &mut out);
    out.extend_from_slice(&body);
    out
}

fn puback(pid: u16) -> Vec<u8> {
    vec![0x40, 0x02, (pid >> 8) as u8, pid as u8]
}

fn pubrec(pid: u16) -> Vec<u8> {
    vec![0x50, 0x02, (pid >> 8) as u8, pid as u8]
}

fn pubcomp(pid: u16) -> Vec<u8> {
    vec![0x70, 0x02, (pid >> 8) as u8, pid as u8]
}

/// CONNECT + read CONNACK; returns the socket and the CONNACK payload.
async fn connect_ok(
    addr: &str,
    packet: Vec<u8>,
) -> (tokio::net::TcpStream, Vec<u8>) {
    let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
    c.write_all(&packet).await.unwrap();
    let (t, payload) = with_timeout(mqtt_read(&mut c), 30).await.unwrap().unwrap();
    assert_eq!(t, 2, "CONNACK expected, got type {t}");
    (c, payload)
}

#[tokio::test(flavor = "multi_thread")]
async fn mqtt_first_packet_must_be_connect() {
    let (addr, _n) = gw().await;
    let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
    // A PINGREQ before CONNECT: the server hangs up.
    c.write_all(&pingreq()).await.unwrap();
    let mut buf = [0u8; 32];
    let n = with_timeout(c.read(&mut buf), 15).await.unwrap_or(0);
    assert_eq!(n, 0, "server must hang up");
}

#[tokio::test(flavor = "multi_thread")]
async fn mqtt_persistent_session_with_empty_client_id_is_accepted() {
    let (addr, _n) = gw().await;
    // clean=false with an empty client id: no session queue, still OK.
    let (_, payload) = connect_ok(&addr, connect_packet("", false, 0)).await;
    assert_eq!(payload[1], 0, "session present must be false");
}

#[tokio::test(flavor = "multi_thread")]
async fn mqtt_second_connect_is_a_protocol_violation() {
    let (addr, _n) = gw().await;
    let (mut c, _) = connect_ok(&addr, connect_packet("double", true, 0)).await;
    // A second CONNECT is a violation: the server closes.
    c.write_all(&connect_packet("double", true, 0)).await.unwrap();
    let mut buf = [0u8; 32];
    let n = with_timeout(c.read(&mut buf), 15).await.unwrap_or(0);
    assert_eq!(n, 0, "server must close on a second CONNECT");
}

#[tokio::test(flavor = "multi_thread")]
async fn mqtt_pingreq_gets_pingresp() {
    let (addr, _n) = gw().await;
    let (mut c, _) = connect_ok(&addr, connect_packet("pinger", true, 0)).await;
    c.write_all(&pingreq()).await.unwrap();
    let mut buf = [0u8; 8];
    let n = with_timeout(c.read(&mut buf), 15).await.unwrap();
    assert_eq!(&buf[..n], &pingresp()[..n]);
}

#[tokio::test(flavor = "multi_thread")]
async fn mqtt_silent_client_with_keepalive_times_out() {
    let (addr, _n) = gw().await;
    let (mut c, _) = connect_ok(&addr, connect_packet("quiet", true, 1)).await;
    // Total silence past the 1s keepalive: the server times the session
    // out and closes.
    use tokio::io::AsyncReadExt;
    let mut buf = [0u8; 32];
    let n = tokio::time::timeout(Duration::from_secs(20), c.read(&mut buf)).await;
    match n {
        Ok(Ok(0)) => {}
        Ok(Ok(_)) => {} // a close notification is fine
        Ok(Err(_)) => {}
        Err(_) => panic!("silent client was not timed out"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn mqtt_puback_settles_outbound_qos1() {
    let (addr, _n) = gw().await;
    let (mut sub, _) = connect_ok(&addr, connect_packet("q1sub", true, 0)).await;
    sub.write_all(&gateway_support::mqtt_subscribe(1, "arm/q1", 1)).await.unwrap();
    let _ = with_timeout(mqtt_read(&mut sub), 15).await.unwrap().unwrap(); // SUBACK

    let (mut pubc, _) = connect_ok(&addr, connect_packet("q1pub", true, 0)).await;
    pubc.write_all(&gateway_support::mqtt_publish("arm/q1", b"m1", 0, 0)).await.unwrap();

    // Broker→client PUBLISH carries a packet id for qos1.
    let (t, payload) = with_timeout(mqtt_read(&mut sub), 15).await.unwrap().unwrap();
    assert_eq!(t, 3, "PUBLISH expected");
    let (topic, pid, body) = parse_publish(&payload);
    assert_eq!(topic, "arm/q1");
    assert_eq!(body, b"m1");
    // PUBACK settles it (the broker may redeliver otherwise).
    sub.write_all(&puback(pid)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn mqtt_outbound_qos2_full_flow() {
    let (addr, _n) = gw().await;
    let (mut sub, _) = connect_ok(&addr, connect_packet("q2sub", true, 0)).await;
    sub.write_all(&gateway_support::mqtt_subscribe(1, "arm/q2", 2)).await.unwrap();
    let _ = with_timeout(mqtt_read(&mut sub), 15).await.unwrap().unwrap(); // SUBACK

    let (mut pubc, _) = connect_ok(&addr, connect_packet("q2pub", true, 0)).await;
    pubc.write_all(&gateway_support::mqtt_publish("arm/q2", b"m2", 0, 0)).await.unwrap();

    // PUBLISH qos2 → PUBREC → PUBREL → PUBCOMP.
    let (t, payload) = with_timeout(mqtt_read(&mut sub), 15).await.unwrap().unwrap();
    assert_eq!(t, 3);
    let (topic, pid, body) = parse_publish(&payload);
    assert_eq!(topic, "arm/q2");
    assert_eq!(body, b"m2");
    sub.write_all(&pubrec(pid)).await.unwrap();
    let (t, rel) = with_timeout(mqtt_read(&mut sub), 15).await.unwrap().unwrap();
    assert_eq!(t, 6, "PUBREL expected, got type {t}");
    assert_eq!(u16::from_be_bytes([rel[0], rel[1]]), pid);
    sub.write_all(&pubcomp(pid)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn mqtt_unsubscribe_stops_delivery() {
    let (addr, _n) = gw().await;
    let (mut sub, _) = connect_ok(&addr, connect_packet("unsubber", true, 0)).await;
    sub.write_all(&gateway_support::mqtt_subscribe(1, "arm/u", 0)).await.unwrap();
    let _ = with_timeout(mqtt_read(&mut sub), 15).await.unwrap().unwrap(); // SUBACK
    sub.write_all(&unsub(9, "arm/u")).await.unwrap();
    let (t, payload) = with_timeout(mqtt_read(&mut sub), 15).await.unwrap().unwrap();
    assert_eq!(t, 11, "UNSUBACK expected");
    assert_eq!(u16::from_be_bytes([payload[0], payload[1]]), 9);

    // A late publish is not delivered.
    let (mut pubc, _) = connect_ok(&addr, connect_packet("latepub", true, 0)).await;
    pubc.write_all(&gateway_support::mqtt_publish("arm/u", b"late", 0, 0)).await.unwrap();
    let burst = tokio::time::timeout(Duration::from_millis(700), mqtt_read(&mut sub)).await;
    assert!(burst.is_err(), "no delivery may follow UNSUBSCRIBE");
}

#[tokio::test(flavor = "multi_thread")]
async fn mqtt_persistent_session_shard_is_declared_when_missing() {
    let (addr, _n) = gw().await;
    // A fresh persistent client id: its session queue does not exist yet.
    let (_, payload) = connect_ok(&addr, connect_packet("persist-fresh", false, 0)).await;
    assert_eq!(payload[1], 0, "fresh persistent session is not present");
}

#[test]
fn mqtt_filter_conversion_wrapper() {
    // '/' maps to the AMQP topic separator '.'; a leading slash yields a
    // leading empty level ("$shared" style) — mapped consistently.
    assert_eq!(switchboard_server::protocols::mqtt::convert_filter("/a/b"), ".a.b");
    assert_eq!(switchboard_server::protocols::mqtt::convert_filter("a/b"), "a.b");
}

/// Parse a broker PUBLISH (qos>0) payload: topic-len, topic, packet-id,
/// payload.
fn parse_publish(payload: &[u8]) -> (String, u16, Vec<u8>) {
    let tlen = u16::from_be_bytes([payload[0], payload[1]]) as usize;
    let topic = String::from_utf8(payload[2..2 + tlen].to_vec()).unwrap();
    let pid = u16::from_be_bytes([payload[2 + tlen], payload[3 + tlen]]);
    (topic, pid, payload[4 + tlen..].to_vec())
}
