//! SASL authentication for Connection.Start-Ok (§2.2.4).
//!
//! The server sends `mechanisms` in Connection.Start; the client picks one
//! in Start-Ok. We implement the two mechanisms real clients use:
//!
//! * `PLAIN` (RFC 4616): response is `\0authzid\0authcid\0passwd` —
//!   authzid is typically empty.
//! * `AMQPLAIN`: the client's response is a field table (without its
//!   length prefix) containing `LOGIN` and `PASSWORD`.

use switchboard_wire::field::{FieldValue, FieldTable};
use switchboard_wire::wireio::Decoder;

use crate::error::BrokerError;

/// The security mechanisms this server advertises (§2.2.4: space-separated
/// in the `mechanisms` longstr).
pub const MECHANISMS: &str = "PLAIN AMQPLAIN";
/// The locale this server serves.
pub const LOCALES: &str = "en_US";

/// Extract (user, password) from a Start-Ok response, or `None` for an
/// unsupported/malformed mechanism.
pub fn credentials(mechanism: &str, response: &[u8]) -> Option<(String, String)> {
    match mechanism {
        "PLAIN" => {
            let mut parts = response.split(|b| *b == 0);
            let _authzid = parts.next()?;
            let authcid = parts.next()?;
            let passwd = parts.next()?;
            Some((
                String::from_utf8_lossy(authcid).into_owned(),
                String::from_utf8_lossy(passwd).into_owned(),
            ))
        }
        "AMQPLAIN" => {
            // The response is a field table *without* its length prefix, so
            // parse name/value pairs directly.
            let mut d = Decoder::new(response);
            let mut table = FieldTable::new();
            while !d.is_empty() {
                let name = d.short_str().ok()?;
                let value = FieldValue::decode(&mut d).ok()?;
                table.insert(name, value);
            }
            let user = match table.get("LOGIN") {
                Some(FieldValue::ShortString(s)) => s.clone(),
                _ => return None,
            };
            let pass = match table.get("PASSWORD") {
                Some(FieldValue::ShortString(s)) => s.clone(),
                _ => return None,
            };
            Some((user, pass))
        }
        _ => None,
    }
}

/// Validate a Start-Ok response for the chosen mechanism against a
/// credentials check (user → expected password).
pub fn authenticate(
    mechanism: &str,
    response: &[u8],
    check: impl Fn(&str, &str) -> Result<(), BrokerError>,
) -> Result<String, BrokerError> {
    if mechanism != "PLAIN" && mechanism != "AMQPLAIN" {
        return Err(BrokerError::not_implemented(format!(
            "security mechanism {mechanism:?} is not supported (supported: {MECHANISMS})"
        )));
    }
    let Some((user, pass)) = credentials(mechanism, response) else {
        return Err(BrokerError::access_refused(format!(
            "malformed {mechanism} response"
        )));
    };
    check(&user, &pass)?;
    Ok(user)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Level;

    fn check(user: &str, pass: &str) -> Result<(), BrokerError> {
        if user == "guest" && pass == "guest" {
            Ok(())
        } else {
            Err(BrokerError::connection_access_refused("nope"))
        }
    }

    #[test]
    fn plain_roundtrip() {
        let response = [0, b'g', b'u', b'e', b's', b't', 0, b'g', b'u', b'e', b's', b't'];
        assert_eq!(authenticate("PLAIN", &response, check).unwrap(), "guest");
    }

    #[test]
    fn plain_with_authzid() {
        let mut response = vec![4, b'a', b'g', b'e', b'n', b't', 0];
        response.extend(b"guest");
        response.push(0);
        response.extend(b"guest");
        assert_eq!(authenticate("PLAIN", &response, check).unwrap(), "guest");
    }

    #[test]
    fn bad_plain_credentials_are_connection_level_403() {
        let response = [0, b'e', b'v', b'e', 0, b'h', b'a', b'c', b'k'];
        let e = authenticate("PLAIN", &response, check).unwrap_err();
        assert_eq!(e.code, 403);
        assert_eq!(e.level, Level::Connection);
    }

    #[test]
    fn amqplain_roundtrip() {
        let mut t = FieldTable::new();
        t.insert("LOGIN", switchboard_wire::FieldValue::ShortString("guest".into()));
        t.insert("PASSWORD", switchboard_wire::FieldValue::ShortString("guest".into()));
        let mut e = switchboard_wire::wireio::Encoder::new();
        t.encode(&mut e);
        // AMQPLAIN response is the table WITHOUT its length prefix.
        let body = e.finish();
        let response = &body[4..];
        assert_eq!(authenticate("AMQPLAIN", response, check).unwrap(), "guest");
    }

    #[test]
    fn unknown_mechanisms_are_540() {
        let e = authenticate("SCRAM-SHA-256", b"", check).unwrap_err();
        assert_eq!(e.code, 540);
    }

    #[test]
    fn amqplain_missing_fields() {
        let mut t = FieldTable::new();
        t.insert("LOGIN", switchboard_wire::FieldValue::ShortString("guest".into()));
        let mut e = switchboard_wire::wireio::Encoder::new();
        t.encode(&mut e);
        let response = &e.finish()[4..];
        let err = authenticate("AMQPLAIN", response, check).unwrap_err();
        assert_eq!(err.code, 403);
    }
}

fn hash_memo() -> &'static std::sync::Mutex<Option<(String, String)>> {
    static MEMO: std::sync::OnceLock<std::sync::Mutex<Option<(String, String)>>> =
        std::sync::OnceLock::new();
    MEMO.get_or_init(|| std::sync::Mutex::new(None))
}

/// Argon2 parameters for NEW hashes: OWASP baseline by default
/// (m=19 MiB, t=2, p=1), tunable per deployment via
/// `SWITCHBOARD_ARGON2_M_KIB` / `_T` / `_P`. CI test clusters use light
/// parameters — dozens of brokers bootstrap and authenticate in parallel
/// under tight test deadlines, and a 19 MiB memory-hard verify per
/// handshake starved them (whole formation rounds timed out).
/// Verification of an EXISTING credential always uses the parameters
/// embedded in its PHC string, so tuning only affects new hashes.
fn argon2_params() -> argon2::Params {
    fn num(key: &str, default: u32) -> u32 {
        std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
    }
    let m_kib = num("SWITCHBOARD_ARGON2_M_KIB", 19_456);
    let t = num("SWITCHBOARD_ARGON2_T", 2);
    let p = num("SWITCHBOARD_ARGON2_P", 1);
    argon2::Params::new(m_kib, t, p, None).expect("argon2 params")
}

fn argon2_instance() -> argon2::Argon2<'static> {
    argon2::Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, argon2_params())
}

/// Hash a password for at-rest storage: argon2id. The PHC string carries
/// the random salt and parameters, so stored hashes are self-describing.
///
/// Identical passwords return the identical (memoized) hash: every node
/// boot re-derives the bootstrap credential, and a fresh memory-hard
/// hash per start measurably starved parallel test formation. Memoizing
/// loses nothing at rest — repeat hashes of the same password would
/// never coexist in the users map.
pub fn hash_password(password: &str) -> String {
    if let Some((pw, hash)) = hash_memo().lock().unwrap().as_ref() {
        if pw == password {
            return hash.clone();
        }
    }
    use argon2::password_hash::{PasswordHasher, SaltString};
    use rand::RngCore;
    let mut salt_bytes = [0u8; 16];
    rand::rng().fill_bytes(&mut salt_bytes);
    let salt = SaltString::encode_b64(&salt_bytes).expect("salt b64");
    let hash = argon2_instance()
        .hash_password(password.as_bytes(), &salt)
        .expect("argon2 hash")
        .to_string();
    *hash_memo().lock().unwrap() = Some((password.to_string(), hash.clone()));
    hash
}

/// Verify a password against a stored credential. Normal credentials are
/// argon2 PHC strings — verified with the parameters embedded in the PHC
/// string, so old hashes keep working after the deployment retunes —
/// while plaintext survives from snapshots written before hashing
/// existed and is matched as-is (re-set the password to upgrade it).
pub fn verify_password(password: &str, stored: &str) -> bool {
    if stored.starts_with("$argon2") {
        use argon2::password_hash::PasswordVerifier;
        match argon2::password_hash::PasswordHash::new(stored) {
            Ok(parsed) => {
                let params = match argon2::Params::try_from(&parsed) {
                    Ok(p) => p,
                    Err(_) => return false,
                };
                let instance = argon2::Argon2::new(
                    argon2::Algorithm::Argon2id,
                    argon2::Version::V0x13,
                    params,
                );
                instance.verify_password(password.as_bytes(), &parsed).is_ok()
            }
            Err(_) => false,
        }
    } else {
        stored == password
    }
}

#[cfg(test)]
mod hashing_tests {
    use super::*;

    #[test]
    fn hash_and_verify_roundtrip() {
        let stored = hash_password("s3cret");
        assert!(stored.starts_with("$argon2"), "{stored}");
        assert!(verify_password("s3cret", &stored));
        assert!(!verify_password("wrong", &stored));
    }

    #[test]
    fn legacy_plaintext_still_verifies() {
        assert!(verify_password("guest", "guest"));
        assert!(!verify_password("guest", "not-guest"));
    }

    #[test]
    fn repeated_hashes_are_memoized() {
        assert_eq!(hash_password("memo-me"), hash_password("memo-me"));
    }
}
