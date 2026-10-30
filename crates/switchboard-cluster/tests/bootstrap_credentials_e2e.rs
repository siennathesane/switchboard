//! Alert #16 e2e: the CLI `--user`/`--password` flags (wired through
//! `NodeConfig::bootstrap_user`/`bootstrap_password`) reach the
//! formation-time bootstrap write, so a deployment gets the credentials
//! it asked for instead of the public dev default.

use std::sync::Arc;

use switchboard_cluster::ClusterNode;
use switchboard_cluster::NodeConfig;

#[tokio::test(flavor = "multi_thread")]
async fn bootstrap_credentials_flow_into_formation() {
    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }
    let dir = std::env::temp_dir().join(format!(
        "sb-bootcred-{}-{}",
        std::process::id(),
        free_port()
    ));
    let cfg = NodeConfig {
        id: 1,
        data_dir: dir,
        client_addr: format!("127.0.0.1:{}", free_port()),
        internal_addr: format!("127.0.0.1:{}", free_port()),
        seeds: vec![],
        bootstrap: true,
        expected_nodes: 1,
        peers: vec![],
        // Exactly what `main.rs` installs for
        // `--user alice --password s3cret-pw`.
        bootstrap_user: "alice".into(),
        bootstrap_password: "s3cret-pw".into(),
        internal_auth_secret: None,
        timeouts: Default::default(),
    };
    let node = Arc::new(ClusterNode::start(cfg).await.unwrap());

    // Formation: the bootstrap write (vhost + user) applied and the shard
    // layout installed — the same readiness predicate the suites use.
    for _ in 0..400 {
        let topo = node.topology();
        if !topo.vhosts.is_empty() && !topo.groups.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(
        !node.topology().vhosts.is_empty(),
        "node never finished formation"
    );

    // The configured credentials authenticate…
    node.topology()
        .check_credentials("alice", "s3cret-pw")
        .expect("alice/s3cret-pw must authenticate after formation");
    // …the public default pair was never installed…
    assert!(
        node.topology().check_credentials("guest", "guest").is_err(),
        "guest/guest must not exist when --user/--password override it"
    );
    // …and a wrong password is refused.
    assert!(node.topology().check_credentials("alice", "wrong").is_err());

    node.shutdown().await;
}
