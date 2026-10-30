//! End-to-end coverage for the admin HTTP API (`/api`) and the Prometheus
//! `/metrics` endpoint, through the real protocol gateway on the client
//! port:
//!
//! * auth failure — missing/invalid basic credentials are refused (401),
//! * permissions CRUD round-trip — set through the API, observed through
//!   the API, ENFORCED against a real AMQP connection (a restricted user
//!   publishes/reads only what their regexes allow), and cleared again,
//! * `/metrics` — the Prometheus text content type plus representative
//!   metric lines fed by live traffic.

mod gateway_support;
mod support;

use switchboard_wire::BasicProperties;
use switchboard_wire::method::Method;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

/// Each test boots a full broker; running them in parallel saturates CPU
/// and skews raft timing, so they are serialized behind a shared lock
/// (same discipline as the gateway suite).
static GATEWAY_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn lock() -> tokio::sync::MutexGuard<'static, ()> {
    GATEWAY_LOCK.lock().await
}

/// Light test-grade hashing params BEFORE any user is created or
/// verified in this process (the gateway support module does not set
/// them; see support::init_tracing for the rationale).
fn light_hashing() {
    if std::env::var("SWITCHBOARD_ARGON2_M_KIB").is_err() {
        unsafe {
            std::env::set_var("SWITCHBOARD_ARGON2_M_KIB", "2048");
        }
        unsafe {
            std::env::set_var("SWITCHBOARD_ARGON2_T", "1");
        }
    }
}

// ---------------------------------------------------------------------
// Raw HTTP helpers (no HTTP client dependency anywhere in this crate)
// ---------------------------------------------------------------------

fn basic_auth_header(user: &str, pass: &str) -> String {
    use base64::Engine;
    format!(
        "Authorization: Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"))
    )
}

/// Send one HTTP request to the gateway and return
/// `(status_code, full_response_text)`.
async fn http_request(
    method: &str,
    path: &str,
    extra_headers: &[String],
    body: Option<&str>,
) -> (u16, String) {
    let addr = gateway_support::shared_gateway().await;
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    for h in extra_headers {
        req.push_str(h);
        req.push_str("\r\n");
    }
    if let Some(b) = body {
        req.push_str(&format!("Content-Length: {}\r\n", b.len()));
    }
    req.push_str("\r\n");
    if let Some(b) = body {
        req.push_str(b);
    }
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    sock.write_all(req.as_bytes()).await.unwrap();
    sock.flush().await.unwrap();
    let mut buf = Vec::new();
    // The server answers `Connection: close` and shuts down; read to EOF.
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        sock.read_to_end(&mut buf),
    )
    .await;
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    (status, text)
}

/// GET with credentials, retried briefly — the gateway is shared across
/// tests and a meta write from another test may still be converging.
async fn api_get(path: &str, auth: Option<&str>) -> (u16, String) {
    for attempt in 0..40 {
        let headers: Vec<String> = auth.map(|a| vec![a.to_string()]).unwrap_or_default();
        let (code, text) = http_request("GET", path, &headers, None).await;
        if code == 200 || code == 403 || attempt == 39 {
            return (code, text);
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    unreachable!()
}

/// PUT with credentials and a JSON body.
async fn api_put(path: &str, auth: &str, body: &str) -> (u16, String) {
    http_request("PUT", path, &[auth.to_string()], Some(body)).await
}

async fn api_delete(path: &str, auth: &str) -> (u16, String) {
    http_request("DELETE", path, &[auth.to_string()], None).await
}

fn body_of(response: &str) -> &str {
    response.split("\r\n\r\n").nth(1).unwrap_or("")
}

fn header_of(response: &str, name: &str) -> Option<String> {
    let head = response.split("\r\n\r\n").next().unwrap_or("");
    for line in head.split("\r\n").skip(1) {
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        if k.eq_ignore_ascii_case(name) {
            return Some(v.trim().to_string());
        }
    }
    None
}

// ---------------------------------------------------------------------
// AMQP helpers (restricted-user handshake + scoped operations)
// ---------------------------------------------------------------------

/// Full handshake with explicit credentials (support's helper is
/// hardcoded to guest/guest). Retries while the node warms up.
async fn connect_and_open_creds(
    addr: &str,
    vhost: &str,
    user: &str,
    pass: &str,
) -> Result<support::TestClient, String> {
    let mut last = String::new();
    for _ in 0..25 {
        match connect_once(addr, vhost, user, pass).await {
            Ok(c) => return Ok(c),
            Err(e) => {
                last = e;
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    }
    Err(last)
}

async fn connect_once(
    addr: &str,
    vhost: &str,
    user: &str,
    pass: &str,
) -> Result<support::TestClient, String> {
    let mut c = support::TestClient::connect(addr)
        .await
        .map_err(|e| e.to_string())?;
    let Method::ConnectionStart { mechanisms, .. } = c.expect(0).await? else {
        return Err("expected Connection.Start".into());
    };
    assert!(String::from_utf8_lossy(&mechanisms).contains("PLAIN"));
    let mut response = vec![0u8];
    response.extend_from_slice(user.as_bytes());
    response.push(0);
    response.extend_from_slice(pass.as_bytes());
    c.send_method(
        0,
        &Method::ConnectionStartOk {
            client_properties: switchboard_wire::field::FieldTable::new(),
            mechanism: "PLAIN".into(),
            response,
            locale: "en_US".into(),
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    let Method::ConnectionTune {
        channel_max,
        frame_max,
        heartbeat,
    } = c.expect(0).await?
    else {
        return Err("expected Connection.Tune".into());
    };
    c.send_method(
        0,
        &Method::ConnectionTuneOk {
            channel_max,
            frame_max,
            heartbeat,
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    c.send_method(
        0,
        &Method::ConnectionOpen {
            virtual_host: vhost.into(),
            capabilities: String::new(),
            insist: false,
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    let Method::ConnectionOpenOk { .. } = c.expect(0).await? else {
        // Authentication/vhost refused: the server closes without OpenOk.
        return Err("handshake refused (no Connection.OpenOk)".into());
    };
    // §4.2.3: a method on an unopened channel is a connection exception —
    // the scoped-operation tests below use several channels (each denial
    // closes its own channel), so open them all up front.
    for ch in [1u16, 2, 3, 4] {
        c.send_method(
            ch,
            &Method::ChannelOpen {
                out_of_band: String::new(),
            },
        )
        .await
        .map_err(|e| e.to_string())?;
        let Method::ChannelOpenOk { .. } = c.expect(ch).await? else {
            return Err(format!("expected Channel.OpenOk on channel {ch}"));
        };
    }
    Ok(c)
}

/// QueueDeclare on `channel`; returns whatever the server answered.
async fn declare(c: &mut support::TestClient, channel: u16, queue: &str) -> Method {
    declare_ctx(c, channel, queue, queue).await
}

async fn declare_ctx(
    c: &mut support::TestClient,
    channel: u16,
    queue: &str,
    context: &str,
) -> Method {
    c.send_method(
        channel,
        &Method::QueueDeclare {
            ticket: 0,
            queue: queue.into(),
            passive: false,
            durable: true,
            exclusive: false,
            auto_delete: false,
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap_or_else(|e| panic!("{context}: send failed: {e}"));
    c.expect(channel)
        .await
        .unwrap_or_else(|e| panic!("{context}: expect failed: {e}"))
}

/// Expect a channel-level ACCESS_REFUSED close (the authz denial shape).
async fn expect_access_refused(c: &mut support::TestClient, channel: u16, context: &str) {
    let got = c
        .expect(channel)
        .await
        .unwrap_or_else(|e| panic!("{context}: {e}"));
    let Method::ChannelClose {
        reply_code,
        reply_text,
        ..
    } = got
    else {
        panic!("{context}: expected Channel.Close, got {got:?}");
    };
    assert_eq!(
        reply_code, 403,
        "{context}: expected 403, got {reply_code} ({reply_text})"
    );
    let _ = c.send_method(channel, &Method::ChannelCloseOk {}).await;
}

// ---------------------------------------------------------------------
// 1) Auth failure
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn admin_api_refuses_missing_or_bad_credentials() {
    let _guard = lock().await;
    light_hashing();
    support::init_tracing();

    // No Authorization header at all.
    let (code, text) = http_request("GET", "/api/users", &[], None).await;
    assert_eq!(code, 401, "got: {text}");
    let challenge = header_of(&text, "WWW-Authenticate").unwrap_or_default();
    assert!(
        challenge.to_ascii_lowercase().starts_with("basic"),
        "expected a Basic challenge, got: {challenge}"
    );

    // Wrong password for a real user (guest exists with password guest).
    let bad = basic_auth_header("guest", "wrong-password");
    let (code, text) = http_request("GET", "/api/users", &[bad], None).await;
    assert_eq!(code, 401, "got: {text}");

    // Unknown user.
    let unknown = basic_auth_header("nobody-here", "pw");
    let (code, text) = http_request("GET", "/api/queues", &[unknown], None).await;
    assert_eq!(code, 401, "got: {text}");

    // Malformed Authorization header.
    let (code, text) = http_request(
        "GET",
        "/api/users",
        &["Authorization: Basic !!!notb64!!!".into()],
        None,
    )
    .await;
    assert_eq!(code, 401, "got: {text}");
}

// ---------------------------------------------------------------------
// 2) Permissions CRUD round-trip (+ listings, vhost CRUD)
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn permissions_crud_round_trip() {
    let _guard = lock().await;
    light_hashing();
    support::init_tracing();
    let admin = basic_auth_header("guest", "guest");
    let user = "svc-crud";
    let perms = r#"{"configure":"^svc-crud-.*","write":"^svc-crud-.*|^$","read":"^svc-crud-.*"}"#;

    // Create the user.
    let (code, text) = api_put(
        &format!("/api/users/{user}"),
        &admin,
        r#"{"password":"s3cret"}"#,
    )
    .await;
    assert_eq!(code, 200, "PUT user failed: {text}");

    // A user with no permissions entry yet may not be listed as restricted;
    // but they must exist and authenticate (403 comes from the authz rule
    // only once an entry exists — until then they are unrestricted).
    let (code, text) = api_get("/api/users", Some(&admin)).await;
    assert_eq!(code, 200, "GET users failed: {text}");
    assert!(
        body_of(&text).contains(user),
        "user missing from listing: {}",
        body_of(&text)
    );

    // Set permissions on the default vhost (%2F == "/").
    let (code, text) = api_put("/api/permissions/%2F/svc-crud", &admin, perms).await;
    assert_eq!(code, 200, "PUT permissions failed: {text}");

    // Read it back through the API.
    let (code, text) = api_get("/api/permissions/%2F/svc-crud", Some(&admin)).await;
    assert_eq!(code, 200, "GET permission failed: {text}");
    let body = body_of(&text);
    assert!(
        body.contains(r#""configure":"^svc-crud-.*""#),
        "configure regex missing: {body}"
    );
    assert!(
        body.contains(r#""write":"^svc-crud-.*|^$""#),
        "write regex missing: {body}"
    );
    assert!(
        body.contains(r#""read":"^svc-crud-.*""#),
        "read regex missing: {body}"
    );

    // The aggregate listing carries it too.
    let (code, text) = api_get("/api/permissions", Some(&admin)).await;
    assert_eq!(code, 200);
    assert!(
        body_of(&text).contains(user),
        "aggregate listing lacks {user}: {}",
        body_of(&text)
    );

    // GET for a user/vhost pair with no entry is a 404.
    let (code, _) = api_get("/api/permissions/%2F/svc-nobody", Some(&admin)).await;
    assert_eq!(code, 404);

    // A RESTRICTED user may not use the admin API — listings included.
    let restricted = basic_auth_header(user, "s3cret");
    let (code, text) = api_get("/api/users", Some(&restricted)).await;
    assert_eq!(code, 403, "restricted user got in: {text}");
    let (code, text) = api_get("/api/queues", Some(&restricted)).await;
    assert_eq!(code, 403, "restricted user listed queues: {text}");
    let (code, text) = api_put("/api/users/other-u", &restricted, r#"{"password":"x"}"#).await;
    assert_eq!(code, 403, "restricted user created a user: {text}");

    // A restricted user still authenticates over AMQP — their connection
    // is scoped, not rejected (covered in depth by the next test).

    // Vhost CRUD: create, list, mandatory exchanges installed, no delete.
    let (code, text) = api_put("/api/vhosts/e2e-crud-vh", &admin, "").await;
    assert_eq!(code, 200, "PUT vhost failed: {text}");
    let (code, text) = api_get("/api/vhosts", Some(&admin)).await;
    assert_eq!(code, 200);
    assert!(
        body_of(&text).contains("e2e-crud-vh"),
        "vhost missing: {}",
        body_of(&text)
    );
    let (code, text) = api_get("/api/exchanges", Some(&admin)).await;
    assert_eq!(code, 200);
    assert!(
        body_of(&text).contains(r#""vhost":"e2e-crud-vh""#),
        "mandatory exchanges not installed on new vhost: {}",
        body_of(&text)
    );
    let (code, _) = api_delete("/api/vhosts/e2e-crud-vh", &admin).await;
    assert_eq!(code, 405, "no DeleteVhost command exists; expected 405");

    // Queue listing carries the default vhost's declared queues.
    let addr = gateway_support::shared_gateway().await;
    let mut c = support::connect_and_open(&addr, "/").await.unwrap();
    let got = declare(&mut c, 1, "crud-listing-q").await;
    assert!(
        matches!(got, Method::QueueDeclareOk { .. }),
        "declare failed: {got:?}"
    );
    let (code, text) = api_get("/api/queues", Some(&admin)).await;
    assert_eq!(code, 200);
    assert!(
        body_of(&text).contains(r#""name":"crud-listing-q""#)
            && body_of(&text).contains(r#""vhost":"/""#),
        "queue listing lacks crud-listing-q: {}",
        body_of(&text)
    );

    // Delete the permissions entry (clear), observe the 404.
    let (code, text) = api_delete("/api/permissions/%2F/svc-crud", &admin).await;
    assert_eq!(code, 200, "DELETE permission failed: {text}");
    let (code, _) = api_get("/api/permissions/%2F/svc-crud", Some(&admin)).await;
    assert_eq!(code, 404, "permission survived the clear");

    // Cleanup: remove the user (cascades any remaining permissions).
    let (code, text) = api_delete(&format!("/api/users/{user}"), &admin).await;
    assert_eq!(code, 200, "DELETE user failed: {text}");
    let (code, _) = api_get(&format!("/api/users/{user}"), Some(&admin)).await;
    assert_eq!(code, 404, "user survived deletion");
}

// ---------------------------------------------------------------------
// 3) Restricted user scoped through a REAL connection
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn restricted_user_can_only_publish_and_read_within_their_regexes() {
    let _guard = lock().await;
    light_hashing();
    support::init_tracing();
    let admin = basic_auth_header("guest", "guest");

    // Admin-side setup: user "svc-app" scoped to ^svc-app-* resources, plus
    // an out-of-scope queue created by the ADMIN (the user must not read it).
    let (code, text) = api_put("/api/users/svc-app", &admin, r#"{"password":"app-pw"}"#).await;
    assert_eq!(code, 200, "create svc-app failed: {text}");
    let (code, text) = api_put(
        "/api/permissions/%2F/svc-app",
        &admin,
        r#"{"configure":"^svc-app-.*","write":"^svc-app-.*|^$","read":"^svc-app-.*"}"#,
    )
    .await;
    assert_eq!(code, 200, "set permissions failed: {text}");
    let addr = gateway_support::shared_gateway().await;
    let mut admin_c = support::connect_and_open(&addr, "/").await.unwrap();
    let got = declare_ctx(&mut admin_c, 1, "off-limits-q", "admin declare forbidden-q").await;
    assert!(matches!(got, Method::QueueDeclareOk { .. }));

    // Real connection as the restricted user (plain auth, non-guest).
    let mut c = connect_and_open_creds(&addr, "/", "svc-app", "app-pw")
        .await
        .expect("restricted user connects and opens");

    // Configure: allowed queue passes.
    let got = declare_ctx(&mut c, 1, "svc-app-inbox", "restricted declare in-scope").await;
    assert!(
        matches!(got, Method::QueueDeclareOk { .. }),
        "in-scope declare failed: {got:?}"
    );

    // Configure: anything outside the regex is refused (403 channel close).
    let got = declare(&mut c, 2, "other-people-q").await;
    assert!(
        matches!(
            got,
            Method::ChannelClose {
                reply_code: 403,
                ..
            }
        ),
        "out-of-scope declare allowed: {got:?}"
    );
    let _ = c.send_method(2, &Method::ChannelCloseOk {}).await;

    // Write: publishing to the default exchange (write regex matches "").
    support::publish(
        &mut c,
        1,
        "",
        "svc-app-inbox",
        &BasicProperties::default(),
        b"scoped-payload",
        false,
    )
    .await
    .unwrap();

    // Read: get from the in-scope queue delivers that message.
    let got = support::basic_get(&mut c, 1, "svc-app-inbox", true)
        .await
        .unwrap();
    let (tag, body) = got.expect("in-scope get must see the published message");
    assert_eq!(body, b"scoped-payload");
    let _ = tag;

    // Read: the admin-created queue is refused (403), despite existing.
    // (support::basic_get answers the server's Channel.Close itself.)
    let got = support::basic_get(&mut c, 3, "off-limits-q", true).await;
    match got {
        Err(text) => assert!(text.contains("403"), "expected 403, got: {text}"),
        Ok(Some(_)) => panic!("restricted user read a forbidden queue"),
        Ok(None) => panic!("read on forbidden queue answered GetEmpty instead of 403"),
    }

    // Write: a topic exchange outside the regex is refused (403). The
    // routing key check only runs when the exchange check passes, so the
    // exchange name itself is the resource that fails here.
    c.send_method(
        4,
        &Method::BasicPublish {
            ticket: 0,
            exchange: "amq.topic".into(),
            routing_key: "svc-app.anything".into(),
            mandatory: false,
            immediate: false,
        },
    )
    .await
    .unwrap();
    c.send_content(4, &BasicProperties::default(), b"denied")
        .await
        .unwrap();
    expect_access_refused(&mut c, 4, "out-of-scope topic publish").await;

    // The forbidden queue really did not receive anything through the
    // denied publish paths.
    let got = support::basic_get(&mut admin_c, 1, "off-limits-q", true)
        .await
        .unwrap();
    assert!(
        got.is_none(),
        "forbidden queue unexpectedly contains a message"
    );

    // Cleanup via the API under test.
    let _ = api_delete("/api/users/svc-app", &admin).await;
}

// ---------------------------------------------------------------------
// 4) /metrics: content type + representative lines
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn metrics_content_type_and_representative_lines() {
    let _guard = lock().await;
    light_hashing();
    support::init_tracing();

    // Generate live traffic so the counters have real values: one AMQP
    // connection, one declared queue, one published message left queued
    // (the ready gauge lists only queues holding messages, so the message
    // stays put for the assertion below).
    let addr = gateway_support::shared_gateway().await;
    let mut c = support::connect_and_open(&addr, "/").await.unwrap();
    let got = declare(&mut c, 1, "metrics-traffic-q").await;
    assert!(matches!(got, Method::QueueDeclareOk { .. }));
    support::publish(
        &mut c,
        1,
        "",
        "metrics-traffic-q",
        &BasicProperties::default(),
        b"m1",
        false,
    )
    .await
    .unwrap();

    let (code, text) = http_request("GET", "/metrics", &[], None).await;
    assert_eq!(code, 200, "GET /metrics failed");

    // Content type: Prometheus text exposition, version 0.0.4.
    let ctype = header_of(&text, "Content-Type").unwrap_or_default();
    assert_eq!(ctype, "text/plain; version=0.0.4", "got: {ctype}");

    let body = body_of(&text);
    assert!(!body.is_empty(), "empty metrics body");

    // Representative message-lifecycle lines (the counters the soak engine
    // already computes), with HELP/TYPE metadata.
    assert!(
        body.contains("# TYPE switchboard_messages_enqueued_total counter"),
        "no enqueued counter: {body}"
    );
    assert!(
        body.contains("# TYPE switchboard_messages_acked_total counter"),
        "no acked counter"
    );
    let enqueued_line = body
        .lines()
        .find(|l| l.starts_with("switchboard_messages_enqueued_total "))
        .expect("no enqueued sample line");
    let enqueued: u64 = enqueued_line
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .expect("unparseable enqueued value");
    assert!(
        enqueued >= 1,
        "enqueued counter did not observe our publish: {enqueued_line}"
    );

    // Per-queue depth gauges carry our traffic queue with its message.
    assert!(
        body.contains(r#"switchboard_queue_ready_messages{queue="metrics-traffic-q"} 1"#),
        "no ready gauge with depth 1 for metrics-traffic-q: {body}"
    );

    // Per-protocol connection lines (gauge + since-start counter).
    assert!(
        body.lines()
            .any(|l| l.starts_with("switchboard_connections{protocol=\"amqp091\"} ")),
        "no amqp091 connection gauge: {body}"
    );
    let accepted = body
        .lines()
        .find(|l| l.starts_with("switchboard_connections_total{protocol=\"amqp091\"} "))
        .expect("no amqp091 connection total");
    let accepted_n: u64 = accepted
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .expect("unparseable connection total");
    assert!(
        accepted_n >= 1,
        "amqp091 total did not observe our connection: {accepted}"
    );

    // Node vitals and identity.
    assert!(
        body.lines()
            .any(|l| l.starts_with("switchboard_uptime_seconds ")),
        "no uptime gauge: {body}"
    );
    assert!(
        body.lines()
            .any(|l| l.starts_with("switchboard_node_info{node=") && l.ends_with("} 1")),
        "no node info line: {body}"
    );
    // Counters/gauges are plain integers; nothing non-numeric leaked in.
    for line in body.lines().filter(|l| !l.starts_with('#')) {
        let value = line
            .split_whitespace()
            .nth(1)
            .expect("sample without value");
        assert!(
            value.parse::<f64>().map(|v| v.is_finite()).unwrap_or(false),
            "non-finite sample in {line}"
        );
    }
}

// ---------------------------------------------------------------------
// 5) Exchange-to-exchange bindings honor the configure regex
//     (code-scanning alert #6)
// ---------------------------------------------------------------------

/// Declare a topic exchange on `channel` and return the server's reply.
async fn declare_exchange(
    c: &mut support::TestClient,
    channel: u16,
    exchange: &str,
    context: &str,
) -> Method {
    c.send_method(
        channel,
        &Method::ExchangeDeclare {
            ticket: 0,
            exchange: exchange.into(),
            exchange_type: "topic".into(),
            passive: false,
            durable: true,
            auto_delete: false,
            internal: false,
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap_or_else(|e| panic!("{context}: send failed: {e}"));
    c.expect(channel)
        .await
        .unwrap_or_else(|e| panic!("{context}: expect failed: {e}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn restricted_user_cannot_bind_or_unbind_out_of_scope_exchanges() {
    let _guard = lock().await;
    light_hashing();
    support::init_tracing();
    let admin = basic_auth_header("guest", "guest");

    // A user whose configure regex covers only ^svc-xb-.* exchanges.
    let (code, text) = api_put("/api/users/svc-xb", &admin, r#"{"password":"xb-pw"}"#).await;
    assert_eq!(code, 200, "create svc-xb failed: {text}");
    let (code, text) = api_put(
        "/api/permissions/%2F/svc-xb",
        &admin,
        r#"{"configure":"^svc-xb-.*","write":"^svc-xb-.*","read":"^svc-xb-.*"}"#,
    )
    .await;
    assert_eq!(code, 200, "set permissions failed: {text}");

    let addr = gateway_support::shared_gateway().await;
    // The admin pre-declares the out-of-scope destination exchange.
    let mut admin_c = support::connect_and_open(&addr, "/").await.unwrap();
    let got = declare_exchange(&mut admin_c, 1, "denied-xb-dst", "admin declare denied dst").await;
    assert!(
        matches!(got, Method::ExchangeDeclareOk { .. }),
        "admin declare failed: {got:?}"
    );

    // The restricted user declares both of THEIR exchanges, then binds
    // them — configure covers both endpoints, so this must succeed.
    let mut c = connect_and_open_creds(&addr, "/", "svc-xb", "xb-pw")
        .await
        .expect("restricted user connects and opens");
    for ex in ["svc-xb-src", "svc-xb-dst"] {
        let got = declare_exchange(&mut c, 1, ex, "restricted declare in-scope exchange").await;
        assert!(
            matches!(got, Method::ExchangeDeclareOk { .. }),
            "in-scope exchange declare failed: {got:?}"
        );
    }
    c.send_method(
        1,
        &Method::ExchangeBind {
            ticket: 0,
            destination: "svc-xb-dst".into(),
            source: "svc-xb-src".into(),
            routing_key: "#".into(),
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    let got = c.expect(1).await.unwrap();
    assert!(
        matches!(got, Method::ExchangeBindOk { .. }),
        "in-scope bind failed: {got:?}"
    );

    // Binding an out-of-scope DESTINATION is refused with 403.
    c.send_method(
        2,
        &Method::ExchangeBind {
            ticket: 0,
            destination: "denied-xb-dst".into(),
            source: "svc-xb-src".into(),
            routing_key: "#".into(),
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    expect_access_refused(&mut c, 2, "bind to denied destination").await;

    // Binding an out-of-scope SOURCE is refused too (RabbitMQ checks
    // configure on both endpoints).
    c.send_method(
        3,
        &Method::ExchangeBind {
            ticket: 0,
            destination: "svc-xb-dst".into(),
            source: "amq.topic".into(),
            routing_key: "#".into(),
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    expect_access_refused(&mut c, 3, "bind from denied source").await;

    // Unbinding the out-of-scope destination is equally refused.
    c.send_method(
        4,
        &Method::ExchangeUnbind {
            ticket: 0,
            destination: "denied-xb-dst".into(),
            source: "svc-xb-src".into(),
            routing_key: "#".into(),
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    expect_access_refused(&mut c, 4, "unbind of denied destination").await;

    // The in-scope unbind still works (proves the gate is not a blanket
    // denial and the authorized path answers ExchangeUnbindOk).
    c.send_method(
        1,
        &Method::ExchangeUnbind {
            ticket: 0,
            destination: "svc-xb-dst".into(),
            source: "svc-xb-src".into(),
            routing_key: "#".into(),
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    let got = c.expect(1).await.unwrap();
    assert!(
        matches!(got, Method::ExchangeUnbindOk { .. }),
        "in-scope unbind failed: {got:?}"
    );

    // Cleanup via the API under test.
    let _ = api_delete("/api/users/svc-xb", &admin).await;
}
