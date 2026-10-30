//! Shared-secret proof of knowledge for the internal cluster transport
//! (code-scanning alert #5).
//!
//! The internal listener accepts Raft, Forward, and Admin traffic that
//! drives the whole replicated control plane, so any host that can reach
//! the internal port owns the cluster. When a deployment configures
//! `NodeConfig::internal_auth_secret` (CLI `--internal-auth-secret` /
//! `SWITCHBOARD_INTERNAL_AUTH_SECRET`), every fresh internal connection
//! must open with an [`InternalRequest::Auth`] frame proving knowledge of
//! that secret; anything else is refused before a single command runs.
//!
//! The proof is `HMAC-SHA256(secret, nonce)` with a fresh random nonce
//! per connection (aws-lc-rs):
//!
//! * the secret itself never crosses the wire and never appears in logs
//!   (only success/failure and the peer's claimed node id are logged);
//! * verification is constant-time (`aws_lc_rs::hmac::verify`);
//! * an on-path eavesdropper who captures a proof cannot recover the
//!   secret — though a *replay* of one captured proof would open one
//!   connection, so plaintext internal networks that face active
//!   on-path attackers should also configure TLS.
//!
//! Unset/empty secret = the historical unauthenticated internal
//! transport (insecure default kept for compatibility with existing
//! deployments; the flag help says so).

use aws_lc_rs::hmac;

use crate::proto::Envelope;
use crate::proto::InternalMessage;
use crate::proto::InternalRequest;

/// Fresh nonce length (bytes).
pub const NONCE_LEN: usize = 32;

/// A fresh per-connection nonce.
pub fn new_nonce() -> Vec<u8> {
    use rand::RngCore;
    let mut nonce = vec![0u8; NONCE_LEN];
    rand::rng().fill_bytes(&mut nonce);
    nonce
}

/// The proof for `secret` over `nonce`: HMAC-SHA256, constant-key,
/// returned as raw tag bytes.
pub fn proof(secret: &str, nonce: &[u8]) -> Vec<u8> {
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
    hmac::sign(&key, nonce).as_ref().to_vec()
}

/// Verify a presented proof in constant time.
pub fn verify(secret: &str, nonce: &[u8], presented: &[u8]) -> bool {
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
    hmac::verify(&key, nonce, presented).is_ok()
}

/// The wire frame a dialing node sends as the first frame on a fresh
/// internal connection when it is configured with `secret`: a framed
/// bincode [`Envelope`] carrying the auth request. `from` carries no
/// identity meaning here (0); the proof is what authenticates.
pub fn auth_frame(secret: &str) -> Vec<u8> {
    let nonce = new_nonce();
    let tag = proof(secret, &nonce);
    let env = Envelope {
        from: 0,
        message: InternalMessage::Request(InternalRequest::Auth { nonce, proof: tag }),
    };
    bincode::serialize(&env).expect("auth envelope serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proof_verifies_against_the_same_secret_and_nonce() {
        let nonce = new_nonce();
        let tag = proof("cluster-secret", &nonce);
        assert!(verify("cluster-secret", &nonce, &tag));
    }

    #[test]
    fn wrong_secret_or_nonce_or_tampered_proof_fails() {
        let nonce = new_nonce();
        let tag = proof("cluster-secret", &nonce);
        assert!(!verify("wrong-secret", &nonce, &tag));
        let other = new_nonce();
        assert!(!verify("cluster-secret", &other, &tag));
        let mut tampered = tag.clone();
        tampered[0] ^= 0x01;
        assert!(!verify("cluster-secret", &nonce, &tampered));
        assert!(!verify("cluster-secret", &nonce, b""));
    }

    #[test]
    fn nonces_and_proofs_vary_per_connection() {
        let a = new_nonce();
        let b = new_nonce();
        assert_ne!(a, b, "32 random bytes colliding is a broken RNG");
        assert_ne!(proof("s", &a), proof("s", &b));
    }

    #[test]
    fn the_secret_never_appears_in_the_wire_frame() {
        let secret = "do-not-leak-this-secret";
        let frame = auth_frame(secret);
        assert!(
            !frame.windows(secret.len()).any(|w| w == secret.as_bytes()),
            "secret must not cross the wire in cleartext"
        );
        let env: Envelope = bincode::deserialize(&frame).unwrap();
        match env.message {
            InternalMessage::Request(InternalRequest::Auth { nonce, proof: tag }) => {
                assert_eq!(nonce.len(), NONCE_LEN);
                assert!(verify(secret, &nonce, &tag));
            }
            other => panic!("expected an auth request, got {other:?}"),
        }
    }
}
