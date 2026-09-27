//! Snoops broker frames around a confirm-mode publish to a multi-queue
//! topic exchange (the Go suite's exact flow).

mod support;

use switchboard_wire::field::FieldTable;
use switchboard_wire::method::Method;

#[tokio::test(flavor = "multi_thread")]
async fn snoop_confirm_frames() {
    let (_node, addr) = support::start_broker("snoop").await;
    // Like the Go client: negotiate a 10-second heartbeat.
    let mut c = support::TestClient::connect(&addr).await.unwrap();
    {
        let start = c.expect(0).await.unwrap();
        let Method::ConnectionStart { mechanisms: _, .. } = &start else {
            panic!("expected Start");
        };
        c.send_method(0, &Method::ConnectionStartOk {
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
        }).await.unwrap();
        let Method::ConnectionTune { channel_max, frame_max, heartbeat: _ } = c.expect(0).await.unwrap() else {
            panic!("expected Tune");
        };
        c.send_method(0, &Method::ConnectionTuneOk { channel_max, frame_max, heartbeat: 10 }).await.unwrap();
        c.send_method(0, &Method::ConnectionOpen { virtual_host: "/".into(), capabilities: String::new(), insist: false }).await.unwrap();
        let _ = c.expect(0).await.unwrap();
        c.send_method(1, &Method::ChannelOpen { out_of_band: String::new() }).await.unwrap();
        let _ = c.expect(1).await.unwrap();
    }

    c.send_method(1, &Method::ConfirmSelect { nowait: false }).await.unwrap();
    println!("[snoop] after ConfirmSelect: {:?}", c.expect(1).await.unwrap().name());

    // Mirror the Go suite: confirmed publish -> get -> ack -> declares
    // -> binds -> confirmed publish.
    c.send_method(1, &Method::QueueDeclare {
        ticket: 0, queue: "sn-first".into(), passive: false, durable: true,
        exclusive: false, auto_delete: false, nowait: false, arguments: Default::default(),
    }).await.unwrap();
    let _ = c.expect(1).await.unwrap();
    c.send_method(1, &Method::BasicPublish {
        ticket: 0, exchange: "".into(), routing_key: "sn-first".into(),
        mandatory: false, immediate: false,
    }).await.unwrap();
    c.send_content(1, &switchboard_wire::BasicProperties::new(), b"one").await.unwrap();
    println!("[snoop] after pub1: {:?}", c.expect(1).await.unwrap());
    c.send_method(1, &Method::BasicGet { ticket: 0, queue: "sn-first".into(), no_ack: false }).await.unwrap();
    let got = c.expect(1).await.unwrap();
    println!("[snoop] get: {}", got.name());
    if let Method::BasicGetOk { delivery_tag, .. } = got {
        c.send_method(1, &Method::BasicAck { delivery_tag, multiple: false }).await.unwrap();
        println!("[snoop] acked {delivery_tag}");
    }

    for q in ["sn-a", "sn-b"] {
        c.send_method(1, &Method::QueueDeclare {
            ticket: 0, queue: q.into(), passive: false, durable: true,
            exclusive: false, auto_delete: false, nowait: false, arguments: Default::default(),
        }).await.unwrap();
        let _ = c.expect(1).await.unwrap();
        c.send_method(1, &Method::QueueBind {
            ticket: 0, queue: q.into(), exchange: "amq.topic".into(),
            routing_key: "a.*".into(), nowait: false, arguments: FieldTable::new(),
        }).await.unwrap();
        let _ = c.expect(1).await.unwrap();
    }

    // Mirror the Go nack probe: confirm-publish, get, ack, then raw-dump
    // everything the broker sends.
    c.send_method(1, &Method::QueueDeclare {
        ticket: 0, queue: "sn-first".into(), passive: false, durable: true,
        exclusive: false, auto_delete: false, nowait: false, arguments: Default::default(),
    }).await.unwrap();
    let _ = c.expect(1).await.unwrap();
    c.send_method(1, &Method::BasicPublish {
        ticket: 0, exchange: "".into(), routing_key: "sn-first".into(),
        mandatory: false, immediate: false,
    }).await.unwrap();
    c.send_content(1, &switchboard_wire::BasicProperties::new(), b"one").await.unwrap();
    println!("[snoop] after pub1: {:?}", c.expect(1).await.unwrap());
    c.send_method(1, &Method::BasicGet { ticket: 0, queue: "sn-first".into(), no_ack: false }).await.unwrap();
    let got = c.expect(1).await.unwrap();
    println!("[snoop] get: {}", got.name());
    if let Method::BasicGetOk { delivery_tag, .. } = got {
        c.send_method(1, &Method::BasicAck { delivery_tag, multiple: false }).await.unwrap();
        println!("[snoop] acked {delivery_tag}");
    }

    // Raw dump: hex-decode every method frame header the broker sends.
    use tokio::io::AsyncReadExt;
    let mut buf = [0u8; 4096];
    let mut acc: Vec<u8> = Vec::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    let mut frames = 0usize;
    while tokio::time::Instant::now() < deadline && frames < 12 {
        let n = tokio::time::timeout(std::time::Duration::from_millis(500), c.read_half.read(&mut buf)).await;
        match n {
            Ok(Ok(0)) => break,
            Err(_) => continue, // silence until the deadline
            Ok(Err(_)) => break,
            Ok(Ok(n)) => acc.extend_from_slice(&buf[..n]),
        }
        // Parse frames: 7-byte header (type, ch2, size4) + payload + end.
        while acc.len() >= 8 {
            let size = u32::from_be_bytes([acc[3], acc[4], acc[5], acc[6]]) as usize;
            let total = 7 + size + 1;
            if acc.len() < total {
                break;
            }
            let ftype = acc[0];
            let channel = u16::from_be_bytes([acc[1], acc[2]]);
            let payload = &acc[7..7 + size];
            if ftype == 1 && payload.len() >= 4 {
                let class = u16::from_be_bytes([payload[0], payload[1]]);
                let method = u16::from_be_bytes([payload[2], payload[3]]);
                let rest = &payload[4..];
                let tag = if rest.len() >= 8 {
                    u64::from_be_bytes(rest[..8].try_into().unwrap())
                } else {
                    0
                };
                println!("[snoop] frame ch={channel} class={class} method={method} tag={tag} payload={payload:02x?}");
            } else {
                println!("[snoop] frame type={ftype} ch={channel} size={size}");
            }
            acc.drain(..total);
            frames += 1;
        }
    }
}
