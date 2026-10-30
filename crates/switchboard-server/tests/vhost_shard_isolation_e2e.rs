//! Alert #14 e2e: vhost-qualified shard-data keys — the same queue name
//! declared in two vhosts holds strictly separate message data, in both
//! publish directions.

mod support;

use support::TestClient;
use switchboard_cluster::BrokerCommand;
use switchboard_core::topology::MetaCmd;
use switchboard_wire::method::Method;
use switchboard_wire::properties::BasicProperties;

/// Create a vhost through the meta write path (what the admin API does).
async fn create_vhost(node: &std::sync::Arc<switchboard_cluster::ClusterNode>, name: &str) {
    node.write(
        switchboard_cluster::META_GROUP,
        BrokerCommand::Meta(MetaCmd::DeclareVhost {
            name: name.to_string(),
        }),
    )
    .await
    .unwrap();
    node.refresh_topology().await;
}

async fn declare(c: &mut TestClient, queue: &str) {
    c.send_method(
        1,
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
    .unwrap();
    assert!(matches!(
        c.expect(1).await.unwrap(),
        Method::QueueDeclareOk { .. }
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn same_queue_name_in_two_vhosts_never_shares_data() {
    support::init_tracing();
    let (node, addr) = support::start_broker("vhost-isol").await;
    create_vhost(&node, "/tenant2").await;

    let mut a = support::connect_and_open(&addr, "/").await.unwrap();
    let mut b = support::connect_and_open(&addr, "/tenant2").await.unwrap();
    declare(&mut a, "shared-orders").await;
    declare(&mut b, "shared-orders").await;

    // Both publish directions: each vhost publishes into its OWN queue.
    support::publish(
        &mut a,
        1,
        "",
        "shared-orders",
        &BasicProperties::new(),
        b"from-vhost-a",
        false,
    )
    .await
    .unwrap();
    support::publish(
        &mut b,
        1,
        "",
        "shared-orders",
        &BasicProperties::new(),
        b"from-vhost-b",
        false,
    )
    .await
    .unwrap();

    // Vhost A drains its message, never vhost B's — and vice versa.
    let (_, body) = support::basic_get(&mut a, 1, "shared-orders", true)
        .await
        .unwrap()
        .expect("vhost A must see its own message");
    assert_eq!(body, b"from-vhost-a");
    assert_eq!(
        support::basic_get(&mut a, 1, "shared-orders", true)
            .await
            .unwrap(),
        None,
        "vhost A must never see vhost B's message"
    );
    let (_, body) = support::basic_get(&mut b, 1, "shared-orders", true)
        .await
        .unwrap()
        .expect("vhost B must see its own message");
    assert_eq!(body, b"from-vhost-b");
    assert_eq!(
        support::basic_get(&mut b, 1, "shared-orders", true)
            .await
            .unwrap(),
        None,
        "vhost B must never see vhost A's message"
    );

    // More traffic after the first drain: still isolated, both ways.
    support::publish(
        &mut a,
        1,
        "",
        "shared-orders",
        &BasicProperties::new(),
        b"from-vhost-a-2",
        false,
    )
    .await
    .unwrap();
    assert_eq!(
        support::basic_get(&mut b, 1, "shared-orders", true)
            .await
            .unwrap(),
        None,
        "vhost B stays isolated after further vhost-A publishes"
    );
    let (_, body) = support::basic_get(&mut a, 1, "shared-orders", true)
        .await
        .unwrap()
        .expect("vhost A's second message is there");
    assert_eq!(body, b"from-vhost-a-2");
}
