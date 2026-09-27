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
