//! Rust tests for the C ABI surface: the extern functions are called from
//! plain `#[test]` code exactly the way C/Python/Go embedders call them —
//! including the error paths: NULL arguments, garbage handles, freed-then-
//! used, dead handles, double-free, and panic containment.
//!
//! These run against the crate's rlib target (the same extern fns the
//! cdylib exports), so `cargo test -p switchboard-capi` exercises the
//! shipping surface.

use std::ffi::CStr;
use std::ffi::CString;
use std::ffi::c_char;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use switchboard_capi::*;

/// A unique temp data_dir per broker (tests run in parallel; each broker
/// owns its RocksDB directory and its private tokio runtime).
fn temp_dir(tag: &str) -> CString {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "switchboard-capi-test-{}-{}-{}",
        tag,
        std::process::id(),
        nanos
    ));
    CString::new(dir.to_string_lossy().as_bytes().to_vec()).expect("temp path is UTF-8")
}

fn cstr_or_die(s: &str) -> CString {
    CString::new(s).expect("no interior NUL in test strings")
}

/// Assert a status is SB_OK. (The thread-local error message from an
/// EARLIER failure legitimately persists — like errno, only a new failure
/// overwrites it — so success must NOT be asserted message-free.)
fn must_ok(status: SbStatus, what: &str) {
    assert_eq!(status, SB_OK, "{what} failed");
}

fn expect_status(status: SbStatus, want: SbStatus, what: &str) {
    assert_eq!(status, want, "{what}: expected status {want:?}");
    let msg = sb_last_error();
    assert!(
        !msg.is_null(),
        "{what}: failed with {want:?} but sb_last_error() was NULL"
    );
    let text = unsafe { CStr::from_ptr(msg) }
        .to_string_lossy()
        .into_owned();
    assert!(
        !text.is_empty(),
        "{what}: last-error message was empty for {want:?}"
    );
}

/// Start a broker with a widened janitor tick (the embedded crate's
/// documented tx/commit race mitigation) and return the handle.
fn start_broker(tag: &str) -> *mut SbBroker {
    let dir = temp_dir(tag);
    let mut cfg: *mut SbConfig = std::ptr::null_mut();
    must_ok(sb_config_create(dir.as_ptr(), &mut cfg), "sb_config_create");
    must_ok(
        sb_config_set_janitor_interval_ms(cfg, 60_000),
        "sb_config_set_janitor_interval_ms",
    );
    let mut broker: *mut SbBroker = std::ptr::null_mut();
    must_ok(sb_broker_start(cfg, &mut broker), "sb_broker_start");
    assert!(!broker.is_null(), "sb_broker_start left a NULL broker");
    must_ok(sb_config_free(cfg), "sb_config_free (right after start)");
    broker
}

fn open_channel(broker: *mut SbBroker) -> *mut SbChannel {
    let mut client: *mut SbClient = std::ptr::null_mut();
    must_ok(
        sb_broker_connect(
            broker,
            cstr_or_die("guest").as_ptr(),
            cstr_or_die("guest").as_ptr(),
            &mut client,
        ),
        "sb_broker_connect",
    );
    let mut channel: *mut SbChannel = std::ptr::null_mut();
    must_ok(
        sb_client_open_channel(client, &mut channel),
        "sb_client_open_channel",
    );
    assert!(!channel.is_null());
    channel
}

/// Declare `queue`, publish `bodies`, and return the channel.
fn setup_queue(broker: *mut SbBroker, queue: &str, bodies: &[&[u8]]) -> *mut SbChannel {
    let channel = open_channel(broker);
    let q = cstr_or_die(queue);
    let mut name: *mut c_char = std::ptr::null_mut();
    must_ok(
        sb_channel_queue_declare(channel, q.as_ptr(), 0, 0, 0, 0, &mut name),
        "sb_channel_queue_declare",
    );
    let got = unsafe { CStr::from_ptr(name) }
        .to_string_lossy()
        .into_owned();
    assert_eq!(got, queue, "queue name round-trip");
    must_ok(sb_string_free(name), "sb_string_free(declared name)");
    for (i, body) in bodies.iter().enumerate() {
        must_ok(
            sb_channel_publish(
                channel,
                cstr_or_die("").as_ptr(), // default exchange -> direct to queue
                q.as_ptr(),
                std::ptr::null(),
                1, // transient
                body.as_ptr(),
                body.len(),
            ),
            &format!("sb_channel_publish[{i}]"),
        );
    }
    channel
}

/// Copy the message body WITHOUT taking ownership (the test then frees via
/// sb_message_free, the documented path).
fn copy_body(msg: &SbMessage) -> Vec<u8> {
    if msg.body_len == 0 {
        return Vec::new();
    }
    assert!(!msg.body.is_null(), "non-zero body_len with NULL body");
    unsafe { std::slice::from_raw_parts(msg.body, msg.body_len) }.to_vec()
}

/// sb_channel_get until a message arrives (with a bounded deadline).
///
/// Publishes are applied by the shard asynchronously relative to the
/// fire-and-forget write path — the embedded crate's own e2e suite polls
/// for exactly this reason (tests/support/mod.rs:107 `wait_until`, 50 ms
/// cadence), so the ABI tests poll too.
fn get_until_has(channel: *mut SbChannel, queue: &str, no_ack: i32) -> SbMessage {
    let q = cstr_or_die(queue);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let mut has: i32 = 0;
        let mut msg: SbMessage = unsafe { std::mem::zeroed() };
        must_ok(
            sb_channel_get(channel, q.as_ptr(), no_ack, &mut has, &mut msg),
            "sb_channel_get",
        );
        if has == 1 {
            return msg;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no message within 10s for {queue}"
        );
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
}

// ---------------------------------------------------------------------------
// 1. happy path: full round trip through the ABI
// ---------------------------------------------------------------------------

#[test]
fn happy_path_roundtrip() {
    let broker = start_broker("happy");
    let channel = setup_queue(broker, "capi-happy", &[b"one", b"two", b"three"]);

    for expected in [&b"one"[..], &b"two"[..], &b"three"[..]] {
        let mut msg = get_until_has(channel, "capi-happy", 0);
        assert_eq!(copy_body(&msg), expected, "payload round-trip");
        assert_eq!(msg.redelivered, 0);
        must_ok(sb_channel_ack(channel, msg.delivery_tag), "sb_channel_ack");
        must_ok(sb_message_free(&mut msg), "sb_message_free");
        // Freed message is re-zeroed: freeing again is a safe no-op.
        must_ok(sb_message_free(&mut msg), "sb_message_free (twice)");
    }

    // Queue drained: SB_OK + has_message = 0.
    let mut has: i32 = -1;
    let mut msg: SbMessage = unsafe { std::mem::zeroed() };
    must_ok(
        sb_channel_get(
            channel,
            cstr_or_die("capi-happy").as_ptr(),
            0,
            &mut has,
            &mut msg,
        ),
        "sb_channel_get (drained)",
    );
    assert_eq!(has, 0, "empty queue must report has_message = 0");

    // Push consumer flavor: publish one, drain with a timeout, ack, cancel.
    must_ok(
        sb_channel_publish(
            channel,
            cstr_or_die("").as_ptr(),
            cstr_or_die("capi-happy").as_ptr(),
            std::ptr::null(),
            0,
            b"pushed".as_ptr(),
            6,
        ),
        "publish for consumer",
    );
    let mut consumer: *mut SbConsumer = std::ptr::null_mut();
    must_ok(
        sb_channel_consume(
            channel,
            cstr_or_die("capi-happy").as_ptr(),
            std::ptr::null(),
            0,
            0,
            &mut consumer,
        ),
        "sb_channel_consume",
    );
    let mut tag: *mut c_char = std::ptr::null_mut();
    must_ok(sb_consumer_tag(consumer, &mut tag), "sb_consumer_tag");
    let tag_s = unsafe { CStr::from_ptr(tag) }
        .to_string_lossy()
        .into_owned();
    assert!(!tag_s.is_empty(), "server-generated tag must be non-empty");
    must_ok(sb_string_free(tag), "sb_string_free(tag)");

    let mut has: i32 = -1;
    let mut msg: SbMessage = unsafe { std::mem::zeroed() };
    must_ok(
        sb_consumer_next(consumer, 5_000, &mut has, &mut msg),
        "sb_consumer_next",
    );
    assert_eq!(has, 1);
    assert_eq!(copy_body(&msg), b"pushed");
    must_ok(
        sb_channel_ack(channel, msg.delivery_tag),
        "ack via consumer delivery",
    );
    must_ok(
        sb_message_free(&mut msg),
        "sb_message_free(consumer delivery)",
    );

    // Idle drain: 100 ms budget, nothing there -> SB_OK + 0.
    let mut has: i32 = -1;
    let mut msg: SbMessage = unsafe { std::mem::zeroed() };
    must_ok(
        sb_consumer_next(consumer, 100, &mut has, &mut msg),
        "sb_consumer_next (idle)",
    );
    assert_eq!(has, 0);

    must_ok(sb_consumer_cancel(consumer), "sb_consumer_cancel");
    // After cancel the handle is DEAD (still freeable).
    let mut has: i32 = -1;
    let mut msg: SbMessage = unsafe { std::mem::zeroed() };
    expect_status(
        sb_consumer_next(consumer, 100, &mut has, &mut msg),
        SB_ERR_HANDLE_DEAD,
        "sb_consumer_next after cancel",
    );
    must_ok(sb_message_free(&mut msg), "sb_message_free after cancel");
    must_ok(sb_consumer_free(consumer), "sb_consumer_free");

    // Transactions through the ABI: commit lands, rollback vanishes.
    must_ok(sb_channel_tx_select(channel), "tx_select");
    must_ok(
        sb_channel_publish(
            channel,
            cstr_or_die("").as_ptr(),
            cstr_or_die("capi-happy").as_ptr(),
            std::ptr::null(),
            0,
            b"committed".as_ptr(),
            9,
        ),
        "tx publish",
    );
    must_ok(sb_channel_tx_commit(channel), "tx_commit");
    let mut msg = get_until_has(channel, "capi-happy", 1);
    assert_eq!(copy_body(&msg), b"committed");
    must_ok(sb_message_free(&mut msg), "sb_message_free(tx)");

    must_ok(
        sb_channel_publish(
            channel,
            cstr_or_die("").as_ptr(),
            cstr_or_die("capi-happy").as_ptr(),
            std::ptr::null(),
            0,
            b"rolled-back".as_ptr(),
            11,
        ),
        "tx publish (rollback branch)",
    );
    must_ok(sb_channel_tx_rollback(channel), "tx_rollback");
    let mut has: i32 = -1;
    let mut msg: SbMessage = unsafe { std::mem::zeroed() };
    must_ok(
        sb_channel_get(
            channel,
            cstr_or_die("capi-happy").as_ptr(),
            1,
            &mut has,
            &mut msg,
        ),
        "get after tx_rollback",
    );
    assert_eq!(has, 0, "rolled-back publish must vanish");

    // Publisher confirms through the ABI — on a FRESH channel: tx and
    // confirm mode are mutually exclusive (406) and `channel` is still
    // transactional from the tx block above.
    let mut confirm_client: *mut SbClient = std::ptr::null_mut();
    must_ok(
        sb_broker_connect(
            broker,
            cstr_or_die("guest").as_ptr(),
            cstr_or_die("guest").as_ptr(),
            &mut confirm_client,
        ),
        "connect for confirm channel",
    );
    let mut confirm_channel: *mut SbChannel = std::ptr::null_mut();
    must_ok(
        sb_client_open_channel(confirm_client, &mut confirm_channel),
        "open confirm channel",
    );
    let mut confirmed: i32 = -1;
    must_ok(
        sb_channel_publish_confirmed(
            confirm_channel,
            cstr_or_die("").as_ptr(),
            cstr_or_die("capi-happy").as_ptr(),
            std::ptr::null(),
            0,
            b"acked".as_ptr(),
            5,
            &mut confirmed,
        ),
        "publish_confirmed",
    );
    assert_eq!(confirmed, 1, "routable publish must confirm as acked");
    let mut confirmed: i32 = -1;
    must_ok(
        sb_channel_publish_confirmed(
            confirm_channel,
            cstr_or_die("no-such-exchange").as_ptr(),
            cstr_or_die("x").as_ptr(),
            std::ptr::null(),
            0,
            b"gone".as_ptr(),
            4,
            &mut confirmed,
        ),
        "publish_confirmed (unroutable)",
    );
    assert_eq!(confirmed, 0, "unroutable publish must confirm as returned");
    must_ok(sb_channel_free(confirm_channel), "free confirm channel");
    must_ok(sb_client_close(confirm_client), "close confirm client");
    must_ok(sb_client_free(confirm_client), "free confirm client");

    // Admin through the ABI.
    must_ok(
        sb_broker_create_user(
            broker,
            cstr_or_die("capi-admin").as_ptr(),
            cstr_or_die("pw").as_ptr(),
        ),
        "sb_broker_create_user",
    );
    must_ok(
        sb_broker_set_permissions(
            broker,
            cstr_or_die("capi-admin").as_ptr(),
            cstr_or_die("/").as_ptr(),
            cstr_or_die(".*").as_ptr(),
            cstr_or_die(".*").as_ptr(),
            cstr_or_die(".*").as_ptr(),
        ),
        "sb_broker_set_permissions",
    );
    expect_status(
        sb_broker_set_permissions(
            broker,
            cstr_or_die("ghost-user").as_ptr(),
            cstr_or_die("/").as_ptr(),
            cstr_or_die(".*").as_ptr(),
            cstr_or_die(".*").as_ptr(),
            cstr_or_die(".*").as_ptr(),
        ),
        SB_ERR_ADMIN_REFUSED,
        "set_permissions for a missing user",
    );
    let mut vclient: *mut SbClient = std::ptr::null_mut();
    expect_status(
        sb_broker_connect_vhost(
            broker,
            cstr_or_die("guest").as_ptr(),
            cstr_or_die("guest").as_ptr(),
            cstr_or_die("nope").as_ptr(),
            &mut vclient,
        ),
        SB_ERR_NO_SUCH_VHOST,
        "connect to an unknown vhost",
    );
    expect_status(
        sb_broker_connect(
            broker,
            cstr_or_die("guest").as_ptr(),
            cstr_or_die("wrong-password").as_ptr(),
            &mut vclient,
        ),
        SB_ERR_CONNECT_ACCESS_REFUSED,
        "connect with a wrong password",
    );

    must_ok(sb_channel_free(channel), "sb_channel_free");
    must_ok(sb_broker_stop(broker, 10_000), "sb_broker_stop");
    must_ok(sb_broker_free(broker), "sb_broker_free (after stop)");
}

// ---------------------------------------------------------------------------
// 2. NULL arguments are rejected everywhere
// ---------------------------------------------------------------------------

#[test]
fn null_arguments_are_rejected() {
    // Broker/config entry points.
    let dir = temp_dir("null-args");
    expect_status(
        sb_config_create(std::ptr::null(), std::ptr::null_mut()),
        SB_ERR_NULL_ARG,
        "sb_config_create(NULL, NULL)",
    );
    let mut cfg: *mut SbConfig = std::ptr::null_mut();
    must_ok(sb_config_create(dir.as_ptr(), &mut cfg), "sb_config_create");
    expect_status(
        sb_broker_start(cfg, std::ptr::null_mut()),
        SB_ERR_NULL_ARG,
        "sb_broker_start(cfg, NULL out)",
    );
    expect_status(
        sb_broker_start(
            std::ptr::null(),
            &mut (std::ptr::null_mut() as *mut SbBroker),
        ),
        SB_ERR_NULL_ARG,
        "sb_broker_start(NULL cfg)",
    );
    must_ok(sb_config_free(cfg), "sb_config_free");

    // Untouched out-pointers on failure: sb_config_create must not write.
    let mut cfg2: *mut SbConfig = 0x1234 as *mut SbConfig;
    expect_status(
        sb_config_create(std::ptr::null(), &mut cfg2),
        SB_ERR_NULL_ARG,
        "sb_config_create(NULL dir)",
    );
    assert_eq!(
        cfg2 as usize, 0x1234,
        "failed create must leave *out untouched"
    );

    // With a REAL broker: channel-level NULL args.
    let broker = start_broker("null-args");
    let channel = open_channel(broker);

    expect_status(
        sb_channel_queue_declare(channel, std::ptr::null(), 0, 0, 0, 0, std::ptr::null_mut()),
        SB_ERR_NULL_ARG,
        "queue_declare(NULL name)",
    );
    expect_status(
        sb_channel_get(
            channel,
            std::ptr::null(),
            0,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        ),
        SB_ERR_NULL_ARG,
        "get(NULL queue, NULL outs)",
    );
    expect_status(
        sb_channel_publish(
            channel,
            cstr_or_die("").as_ptr(),
            cstr_or_die("q").as_ptr(),
            std::ptr::null(),
            0,
            std::ptr::null(),
            7, // NULL body with non-zero length
        ),
        SB_ERR_NULL_ARG,
        "publish(NULL body, len 7)",
    );
    // NULL body with len 0 is accepted by the ABI (frames written, SB_OK).
    // KNOWN PRE-EXISTING BROKER GAP (documented, not fixed here): the
    // server's session demux completes content only in its Body-frame arm
    // (session/mod.rs:549 take_content), but a body_size == 0 publish has
    // NO body frame on the wire (embedded driver.rs:1001 skips it per the
    // AMQP spec), so an empty-body message is never applied — for TCP
    // clients identically. The test therefore asserts the ABI contract
    // (argument validation: NULL + 0 is the empty body, accepted) without
    // depending on the broker's empty-body delivery.
    let mut qname: *mut c_char = std::ptr::null_mut();
    must_ok(
        sb_channel_queue_declare(channel, cstr_or_die("q").as_ptr(), 0, 0, 0, 0, &mut qname),
        "declare q",
    );
    must_ok(sb_string_free(qname), "free q name");
    must_ok(
        sb_channel_publish(
            channel,
            cstr_or_die("").as_ptr(),
            cstr_or_die("q").as_ptr(),
            std::ptr::null(),
            0,
            std::ptr::null(),
            0,
        ),
        "publish(NULL body, len 0) = empty body (accepted; delivery has the documented broker gap)",
    );
    // A NON-empty publish on the same queue still delivers end-to-end.
    must_ok(
        sb_channel_publish(
            channel,
            cstr_or_die("").as_ptr(),
            cstr_or_die("q").as_ptr(),
            std::ptr::null(),
            0,
            b"nonempty".as_ptr(),
            8,
        ),
        "publish non-empty",
    );
    let mut msg = get_until_has(channel, "q", 1);
    assert_eq!(copy_body(&msg), b"nonempty");
    must_ok(sb_message_free(&mut msg), "free non-empty message");

    let mut dropped_has: i32 = 0;
    expect_status(
        sb_channel_get(
            channel,
            cstr_or_die("q").as_ptr(),
            0,
            &mut dropped_has,
            std::ptr::null_mut(),
        ),
        SB_ERR_NULL_ARG,
        "get(NULL out message)",
    );

    must_ok(sb_channel_free(channel), "sb_channel_free");
    must_ok(sb_broker_stop(broker, 10_000), "sb_broker_stop");
    must_ok(sb_broker_free(broker), "sb_broker_free");
}

// ---------------------------------------------------------------------------
// 3. garbage handles are detected without being dereferenced
// ---------------------------------------------------------------------------

#[test]
fn garbage_handles_are_detected() {
    let fake_broker = 0xDEAD_C0FF_EE01usize as *mut SbBroker;
    let fake_client = 0xDEAD_C0FF_EE02usize as *mut SbClient;
    let fake_channel = 0xDEAD_C0FF_EE03usize as *mut SbChannel;
    let fake_consumer = 0xDEAD_C0FF_EE04usize as *mut SbConsumer;
    let fake_config = 0xDEAD_C0FF_EE05usize as *mut SbConfig;

    let mut ready_out: i32 = 0;
    expect_status(
        sb_broker_is_ready(fake_broker, &mut ready_out),
        SB_ERR_INVALID_HANDLE,
        "sb_broker_is_ready(fake)",
    );
    let mut ch_out: *mut SbChannel = std::ptr::null_mut();
    let mut id_out: u16 = 0;
    let mut tag_out: *mut c_char = std::ptr::null_mut();
    expect_status(
        sb_client_open_channel(fake_client, &mut ch_out),
        SB_ERR_INVALID_HANDLE,
        "sb_client_open_channel(fake)",
    );
    expect_status(
        sb_channel_id(fake_channel, &mut id_out),
        SB_ERR_INVALID_HANDLE,
        "sb_channel_id(fake)",
    );
    expect_status(
        sb_consumer_tag(fake_consumer, &mut tag_out),
        SB_ERR_INVALID_HANDLE,
        "sb_consumer_tag(fake)",
    );
    let mut broker_out: *mut SbBroker = std::ptr::null_mut();
    expect_status(
        sb_broker_start(fake_config, &mut broker_out),
        SB_ERR_INVALID_HANDLE,
        "sb_broker_start(fake config)",
    );
    expect_status(
        sb_channel_free(fake_channel),
        SB_ERR_INVALID_HANDLE,
        "sb_channel_free(fake)",
    );
    expect_status(
        sb_consumer_free(fake_consumer),
        SB_ERR_INVALID_HANDLE,
        "sb_consumer_free(fake)",
    );
    expect_status(
        sb_client_free(fake_client),
        SB_ERR_INVALID_HANDLE,
        "sb_client_free(fake)",
    );
    expect_status(
        sb_broker_free(fake_broker),
        SB_ERR_INVALID_HANDLE,
        "sb_broker_free(fake)",
    );
    // Wrong-KIND handle: a config ptr must not pass for a broker.
    let mut cfg: *mut SbConfig = std::ptr::null_mut();
    let dir = temp_dir("kind-confusion");
    must_ok(sb_config_create(dir.as_ptr(), &mut cfg), "sb_config_create");
    expect_status(
        sb_broker_stop(cfg as *mut SbBroker, 1_000),
        SB_ERR_INVALID_HANDLE,
        "config pointer used as broker",
    );
    expect_status(
        sb_config_free(cfg as *mut SbConfig),
        SB_OK,
        "sb_config_free(real config)",
    );
}

// ---------------------------------------------------------------------------
// 4. freed-then-used and dead-then-used detection
// ---------------------------------------------------------------------------

#[test]
fn freed_then_used_is_detected() {
    let dir = temp_dir("freed");
    let mut cfg: *mut SbConfig = std::ptr::null_mut();
    must_ok(sb_config_create(dir.as_ptr(), &mut cfg), "sb_config_create");
    must_ok(sb_config_free(cfg), "sb_config_free");
    // Freed config: start must refuse.
    let mut broker: *mut SbBroker = std::ptr::null_mut();
    expect_status(
        sb_broker_start(cfg, &mut broker),
        SB_ERR_INVALID_HANDLE,
        "sb_broker_start(freed config)",
    );
    // Double free: detected, no double-drop.
    expect_status(
        sb_config_free(cfg),
        SB_ERR_INVALID_HANDLE,
        "config double free",
    );
    assert!(broker.is_null(), "failed start must not write *out");

    // Client freed-then-used, via a real broker.
    let broker = start_broker("freed");
    let mut client: *mut SbClient = std::ptr::null_mut();
    must_ok(
        sb_broker_connect(
            broker,
            cstr_or_die("guest").as_ptr(),
            cstr_or_die("guest").as_ptr(),
            &mut client,
        ),
        "connect",
    );
    must_ok(sb_client_close(client), "sb_client_close");
    let mut channel: *mut SbChannel = std::ptr::null_mut();
    expect_status(
        sb_client_open_channel(client, &mut channel),
        SB_ERR_HANDLE_DEAD,
        "open_channel after close (dead, not freed)",
    );
    must_ok(sb_client_free(client), "sb_client_free after close");
    expect_status(
        sb_client_free(client),
        SB_ERR_INVALID_HANDLE,
        "client double free",
    );
    expect_status(
        sb_client_close(client),
        SB_ERR_INVALID_HANDLE,
        "client close after free",
    );

    // Channel freed-then-used.
    let channel = open_channel(broker);
    must_ok(sb_channel_free(channel), "sb_channel_free");
    let mut id_after_free: u16 = 0;
    expect_status(
        sb_channel_id(channel, &mut id_after_free),
        SB_ERR_INVALID_HANDLE,
        "sb_channel_id after free",
    );
    expect_status(
        sb_channel_free(channel),
        SB_ERR_INVALID_HANDLE,
        "channel double free",
    );

    // Broker: after stop the handle is DEAD; after free it is INVALID.
    must_ok(sb_broker_stop(broker, 10_000), "sb_broker_stop");
    let mut ready: i32 = -1;
    expect_status(
        sb_broker_is_ready(broker, &mut ready),
        SB_ERR_HANDLE_DEAD,
        "is_ready after stop",
    );
    expect_status(
        sb_broker_stop(broker, 1_000),
        SB_ERR_HANDLE_DEAD,
        "double stop",
    );
    must_ok(sb_broker_free(broker), "sb_broker_free after stop");
    expect_status(
        sb_broker_free(broker),
        SB_ERR_INVALID_HANDLE,
        "broker double free",
    );
    expect_status(
        sb_broker_is_ready(broker, &mut ready),
        SB_ERR_INVALID_HANDLE,
        "is_ready after free",
    );

    // Stale dependent handle: a channel outliving its stopped broker.
    let broker2 = start_broker("freed-dependent");
    let channel2 = open_channel(broker2);
    must_ok(sb_broker_free(broker2), "sb_broker_free (live broker path)");
    let mut id: u16 = 0;
    expect_status(
        sb_channel_id(channel2, &mut id),
        SB_ERR_HANDLE_DEAD,
        "channel of a freed broker",
    );
    must_ok(
        sb_channel_free(channel2),
        "channel still freeable after broker death",
    );
}

// ---------------------------------------------------------------------------
// 5. last-error thread locality
// ---------------------------------------------------------------------------

#[test]
fn last_error_is_thread_local() {
    // A thread that has never called a failing sb_* has NO error.
    let handle = std::thread::spawn(|| sb_last_error().is_null());
    assert!(
        handle.join().expect("join"),
        "fresh thread must report NULL last error"
    );

    // Failure sets it on THIS thread.
    let mut ready_out: i32 = 0;
    expect_status(
        sb_broker_is_ready(0xBAD0usize as *mut SbBroker, &mut ready_out),
        SB_ERR_INVALID_HANDLE,
        "bad handle",
    );
    let msg = sb_last_error();
    assert!(!msg.is_null());
    let text = unsafe { CStr::from_ptr(msg) }
        .to_string_lossy()
        .into_owned();
    assert!(
        text.contains("SB_ERR_INVALID_HANDLE"),
        "message should name the status, got: {text}"
    );

    // And a second thread still sees NULL (never failed there).
    let handle = std::thread::spawn(|| sb_last_error().is_null());
    assert!(handle.join().expect("join"));
}

// ---------------------------------------------------------------------------
// 6. panic containment: a panic never crosses the boundary
// ---------------------------------------------------------------------------

#[test]
fn panics_are_contained() {
    let status = sb_test_force_panic();
    assert_eq!(
        status, SB_ERR_PANIC,
        "forced panic must map to SB_ERR_PANIC"
    );
    let msg = sb_last_error();
    assert!(!msg.is_null(), "panic must leave a last-error message");
    let text = unsafe { CStr::from_ptr(msg) }
        .to_string_lossy()
        .into_owned();
    assert!(
        text.contains("intentional test panic"),
        "panic payload should reach the last-error message, got: {text}"
    );
    // The process is alive and the ABI still works afterwards.
    let version = sb_version_string();
    assert!(!version.is_null());
    let v = unsafe { CStr::from_ptr(version) }
        .to_string_lossy()
        .into_owned();
    assert!(!v.is_empty(), "version string must be non-empty");
}

// ---------------------------------------------------------------------------
// 7. multi-channel interleaving + qos + mandatory publish
// ---------------------------------------------------------------------------

#[test]
fn channels_are_independent_and_mandatory_reports_routing() {
    let broker = start_broker("channels");
    // ONE connection, TWO channels on it (channel numbers are
    // connection-scoped, so they must differ).
    let mut client: *mut SbClient = std::ptr::null_mut();
    must_ok(
        sb_broker_connect(
            broker,
            cstr_or_die("guest").as_ptr(),
            cstr_or_die("guest").as_ptr(),
            &mut client,
        ),
        "sb_broker_connect",
    );
    let mut c1: *mut SbChannel = std::ptr::null_mut();
    let mut c2: *mut SbChannel = std::ptr::null_mut();
    must_ok(sb_client_open_channel(client, &mut c1), "open c1");
    must_ok(sb_client_open_channel(client, &mut c2), "open c2");

    for (channel, queue) in [(c1, "ch-a"), (c2, "ch-b")] {
        let mut name: *mut c_char = std::ptr::null_mut();
        must_ok(
            sb_channel_queue_declare(channel, cstr_or_die(queue).as_ptr(), 0, 0, 0, 0, &mut name),
            &format!("declare {queue}"),
        );
        must_ok(sb_string_free(name), "free declared name");
        must_ok(
            sb_channel_publish(
                channel,
                cstr_or_die("").as_ptr(),
                cstr_or_die(queue).as_ptr(),
                std::ptr::null(),
                1,
                if queue == "ch-a" {
                    b"a1".as_ptr()
                } else {
                    b"b1".as_ptr()
                },
                2,
            ),
            &format!("publish to {queue}"),
        );
    }

    // Independent channel numbers.
    let mut id1: u16 = 0;
    let mut id2: u16 = 0;
    must_ok(sb_channel_id(c1, &mut id1), "id c1");
    must_ok(sb_channel_id(c2, &mut id2), "id c2");
    assert_ne!(id1, id2, "channels on one connection get distinct numbers");

    // qos on one channel does not disturb the other.
    must_ok(sb_channel_qos(c1, 10, 0), "qos c1");

    // Mandatory publish to a routing key with NO queue: unroutable -> 0.
    let mut routed: i32 = -1;
    must_ok(
        sb_channel_publish_mandatory(
            c2,
            cstr_or_die("").as_ptr(),
            cstr_or_die("nothing-binds-this").as_ptr(),
            std::ptr::null(),
            0,
            b"x".as_ptr(),
            1,
            &mut routed,
        ),
        "publish_mandatory (unroutable)",
    );
    assert_eq!(
        routed, 0,
        "unroutable mandatory publish must report routed = 0"
    );

    // Drain both queues to prove independence.
    for (channel, queue, expected) in [(c1, "ch-a", &b"a1"[..]), (c2, "ch-b", &b"b1"[..])] {
        let mut msg = get_until_has(channel, queue, 1);
        assert_eq!(copy_body(&msg), expected);
        must_ok(sb_message_free(&mut msg), "free");
    }

    must_ok(sb_channel_free(c1), "free c1");
    must_ok(sb_channel_free(c2), "free c2");
    must_ok(sb_client_close(client), "close client");
    must_ok(sb_client_free(client), "free client");
    must_ok(sb_broker_free(broker), "free broker");
}

// ---------------------------------------------------------------------------
// 8. data_dir + version helpers
// ---------------------------------------------------------------------------

#[test]
fn broker_data_dir_round_trips() {
    let broker = start_broker("datadir");
    let mut dir: *mut c_char = std::ptr::null_mut();
    must_ok(sb_broker_data_dir(broker, &mut dir), "sb_broker_data_dir");
    let s = unsafe { CStr::from_ptr(dir) }
        .to_string_lossy()
        .into_owned();
    assert!(s.contains("switchboard-capi-test-datadir"), "data dir: {s}");
    must_ok(sb_string_free(dir), "free data dir");
    must_ok(sb_broker_free(broker), "free broker");

    // NULL tolerance on free fns.
    must_ok(sb_string_free(std::ptr::null_mut()), "string_free(NULL)");
    must_ok(sb_message_free(std::ptr::null_mut()), "message_free(NULL)");
    must_ok(sb_config_free(std::ptr::null_mut()), "config_free(NULL)");
    must_ok(sb_client_free(std::ptr::null_mut()), "client_free(NULL)");
    must_ok(sb_channel_free(std::ptr::null_mut()), "channel_free(NULL)");
    must_ok(
        sb_consumer_free(std::ptr::null_mut()),
        "consumer_free(NULL)",
    );
    must_ok(sb_broker_free(std::ptr::null_mut()), "broker_free(NULL)");
}
