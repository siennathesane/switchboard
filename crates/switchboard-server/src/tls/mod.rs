//! TLS support for the client-facing listener: rustls with the aws-lc-rs
//! (aws-lc-sys) crypto provider. Certificate paths come from the node
//! configuration; when unset the listener serves plain TCP.

use std::sync::Arc;

use tokio_rustls::TlsAcceptor;

/// PEM-encoded identity for a server listener.
pub struct TlsIdentity {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

pub fn acceptor(identity: &TlsIdentity) -> Result<TlsAcceptor, String> {
    let certs: Vec<_> = rustls_pemfile::certs(&mut &identity.cert_pem[..])
        .collect::<Result<_, _>>()
        .map_err(|e| format!("bad certificate PEM: {e}"))?;
    let key = rustls_pemfile::private_key(&mut &identity.key_pem[..])
        .map_err(|e| format!("bad private key PEM: {e}"))?
        .ok_or("no private key found")?;

    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS12, &rustls::version::TLS13])
        .map_err(|e| format!("tls versions: {e}"))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("bad certificate/key pair: {e}"))?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

#[cfg(test)]
mod tests {
    use super::*;

    const CERT: &str = "-----BEGIN CERTIFICATE-----\nZm9v\n-----END CERTIFICATE-----\n";

    #[test]
    fn bad_pem_is_reported_not_panicked() {
        let id = TlsIdentity { cert_pem: CERT.as_bytes().to_vec(), key_pem: b"junk".to_vec() };
        assert!(acceptor(&id).is_err());
        let id = TlsIdentity { cert_pem: b"junk".to_vec(), key_pem: b"junk".to_vec() };
        assert!(acceptor(&id).is_err());
    }
}
