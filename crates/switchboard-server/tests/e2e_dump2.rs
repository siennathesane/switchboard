mod support;
use support::start_broker;

#[tokio::test(flavor = "multi_thread")]
async fn dump_unknown_command_behavior() {
    let (_node, addr) = start_broker("dump2").await;
    let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
    use tokio::io::{AsyncWriteExt, AsyncReadExt};
    c.write_all(b"BOGUS\n\n\0").await.unwrap();
    c.flush().await.unwrap();
    let mut buf = Vec::new();
    let n = c.read_to_end(&mut buf).await.unwrap();
    eprintln!("RAW ({} bytes): {:02x?}", n, &buf[..n.min(64)]);
}
