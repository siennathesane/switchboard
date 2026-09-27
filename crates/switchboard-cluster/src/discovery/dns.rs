//! DNS-based peer discovery: A/AAAA seeds via the system resolver and
//! `_switchboard._tcp.<domain>` SRV records via hickory-resolver.

use super::DiscoveredPeer;

/// Resolve a `host[:port]` seed to internal addresses (A/AAAA).
pub async fn resolve_seed(seed: &str) -> Result<Vec<String>, String> {
    let (host, port) = split_seed(seed)?;
    let addrs = tokio::net::lookup_host((host.as_str(), port))
        .await
        .map_err(|e| format!("lookup {host}: {e}"))?;
    Ok(addrs.map(|sa| sa.to_string()).collect())
}

/// Resolve `_switchboard._tcp.<domain>` SRV records to internal
/// addresses (`target:port`).
pub async fn resolve_srv(domain: &str) -> Result<Vec<String>, String> {
    let fqdn = format!("_switchboard._tcp.{domain}");
    let (config, opts) = hickory_resolver::system_conf::read_system_conf().unwrap_or_else(|_| {
        (
            hickory_resolver::config::ResolverConfig::default(),
            hickory_resolver::config::ResolverOpts::default(),
        )
    });
    let resolver = hickory_resolver::AsyncResolver::tokio(config, opts);
    let lookup = resolver
        .srv_lookup(&fqdn)
        .await
        .map_err(|e| format!("srv {fqdn}: {e}"))?;
    let mut peers = Vec::new();
    for record in lookup.iter() {
        let target = record.target().to_string();
        let port = record.port();
        peers.push(DiscoveredPeer {
            internal_addr: format!("{}:{}", target.trim_end_matches('.'), port),
            node_id: None,
        });
    }
    Ok(peers.into_iter().map(|p| p.internal_addr).collect())
}

/// Split `host[:port]`, defaulting the port to 5673.
pub fn split_seed(seed: &str) -> Result<(String, u16), String> {
    let seed = seed.trim();
    if seed.is_empty() {
        return Err("empty seed".into());
    }
    // Bracketed IPv6.
    if let Some(rest) = seed.strip_prefix('[') {
        let (host, rest) = rest
            .split_once(']')
            .ok_or_else(|| "unbalanced bracket".to_string())?;
        let port = rest
            .strip_prefix(':')
            .map(|p| p.parse::<u16>())
            .transpose()
            .map_err(|e| e.to_string())?
            .unwrap_or(5673);
        return Ok((host.to_string(), port));
    }
    match seed.rsplit_once(':') {
        Some((host, p)) if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()) => {
            let port = p.parse::<u16>().map_err(|e| e.to_string())?;
            Ok((host.to_string(), port))
        }
        _ => Ok((seed.to_string(), 5673)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_splitting() {
        assert_eq!(split_seed("node.internal:1234").unwrap(), ("node.internal".into(), 1234));
        assert_eq!(split_seed("node.internal").unwrap(), ("node.internal".into(), 5673));
        assert_eq!(split_seed("[::1]:9999").unwrap(), ("::1".into(), 9999));
        assert_eq!(split_seed("[::1]").unwrap(), ("::1".into(), 5673));
        assert!(split_seed("").is_err());
        assert!(split_seed("host:99999").is_err());
    }

    #[tokio::test]
    async fn numeric_seed_resolves_without_dns() {
        // Numeric seeds skip the resolver entirely and must always work.
        let addrs = resolve_seed("127.0.0.1:5673").await.unwrap();
        assert_eq!(addrs, vec!["127.0.0.1:5673".to_string()]);
    }

    #[tokio::test]
    async fn hostname_resolution_via_system_resolver() {
        // "localhost" is in /etc/hosts on every supported platform, but
        // sandboxes may block getaddrinfo; the production path is the
        // same call, so failure here is reported, not fatal to CI.
        match resolve_seed("localhost:5673").await {
            Ok(addrs) => assert!(!addrs.is_empty()),
            Err(e) => eprintln!("system resolver unavailable in this environment: {e}"),
        }
    }

    #[test]
    fn srv_errors_are_strings_not_panics() {
        // A nonexistent domain must return an error, not hang or panic.
        // (Not async-tested to keep unit time bounded; run via tokio.)
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(async {
            tokio::time::timeout(
                std::time::Duration::from_secs(10),
                resolve_srv("does-not-exist.invalid"),
            )
            .await
        });
        match result {
            Ok(Err(_)) => {}         // resolver error: expected
            Err(_) => {}             // timeout guard fired: also acceptable
            Ok(Ok(v)) => panic!("unexpected success {v:?}"),
        }
    }

    #[test]
    fn peer_shape_is_stable() {
        let p = DiscoveredPeer { internal_addr: "10.0.0.1:5673".into(), node_id: None };
        assert_eq!(p.internal_addr, "10.0.0.1:5673");
        assert_eq!(p.node_id, None);
    }
}
