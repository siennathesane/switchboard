//! Protocol exceptions and reply codes (§4.8).
//!
//! "Any operational error (message queue not found, insufficient access
//! rights, etc.) results in a channel exception. Any structural error
//! (invalid argument, bad sequence of methods, etc.) results in a connection
//! exception."

use switchboard_wire::constants::reply;

/// Which level the exception closes (§4.8.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Level {
    /// Closes the channel that caused the error.
    Channel,
    /// Closes the socket connection.
    Connection,
}

/// A broker exception, carrying the reply code, text, and the level it
/// closes. `class_id`/`method_id` of the offending method accompany
/// connection/channel closes per the Close methods (§3.2.2).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, serde::Serialize, serde::Deserialize)]
#[error("{code} {text}")]
pub struct BrokerError {
    pub code: u16,
    pub text: String,
    pub level: Level,
    pub class_id: u16,
    pub method_id: u16,
}

impl BrokerError {
    fn new(code: u16, text: impl Into<String>, level: Level) -> Self {
        BrokerError { code, text: text.into(), level, class_id: 0, method_id: 0 }
    }

    /// Attach the offending method, as Connection.Close/Channel.Close want.
    pub fn for_method(mut self, class_id: u16, method_id: u16) -> Self {
        self.class_id = class_id;
        self.method_id = method_id;
        self
    }

    /// 403: "access refused by the server" — wrong credentials, reserved
    /// names, use of a foreign exclusive queue.
    pub fn access_refused(text: impl Into<String>) -> Self {
        Self::new(reply::ACCESS_REFUSED, text, Level::Channel)
    }

    /// 403 at connection level: failed authentication, unknown vhost access.
    pub fn connection_access_refused(text: impl Into<String>) -> Self {
        Self::new(reply::ACCESS_REFUSED, text, Level::Connection)
    }

    /// 404: "the client attempted to work with a server entity that does not
    /// exist".
    pub fn not_found(text: impl Into<String>) -> Self {
        Self::new(reply::NOT_FOUND, text, Level::Channel)
    }

    /// 405: "the client attempted to work with a server entity to which it
    /// has no access because another client is working with it" — e.g. an
    /// exclusive queue held elsewhere.
    pub fn resource_locked(text: impl Into<String>) -> Self {
        Self::new(reply::RESOURCE_LOCKED, text, Level::Channel)
    }

    /// 406: "the client attempted to work with a server entity that was
    /// re-declared with different arguments" — the strong assertion model.
    pub fn precondition_failed(text: impl Into<String>) -> Self {
        Self::new(reply::PRECONDITION_FAILED, text, Level::Channel)
    }

    /// 501: malformed frame (oversized payload, bad frame-end, class-id
    /// mismatch in content headers).
    pub fn frame_error(text: impl Into<String>) -> Self {
        Self::new(reply::FRAME_ERROR, text, Level::Connection)
    }

    /// 502: frame content is malformed (bad method arguments, bad field names).
    pub fn syntax_error(text: impl Into<String>) -> Self {
        Self::new(reply::SYNTAX_ERROR, text, Level::Connection)
    }

    /// 503: unsupported or out-of-order command.
    pub fn command_invalid(text: impl Into<String>) -> Self {
        Self::new(reply::COMMAND_INVALID, text, Level::Connection)
    }

    /// 504: channel misuse (second channel-open, content on closed channel).
    pub fn channel_error(text: impl Into<String>) -> Self {
        Self::new(reply::CHANNEL_ERROR, text, Level::Connection)
    }

    /// 505: a frame unexpected in the current context (content without a
    /// publishing method, missing content header, ...).
    pub fn unexpected_frame(text: impl Into<String>) -> Self {
        Self::new(reply::UNEXPECTED_FRAME, text, Level::Connection)
    }

    /// 530: content larger than the server's configured limit.
    pub fn not_allowed(text: impl Into<String>) -> Self {
        Self::new(reply::NOT_ALLOWED, text, Level::Connection)
    }

    /// 540: the peer used something this server does not implement.
    pub fn not_implemented(text: impl Into<String>) -> Self {
        Self::new(reply::NOT_IMPLEMENTED, text, Level::Connection)
    }

    /// 311: content too large for the channel (channel-level).
    pub fn content_too_large(text: impl Into<String>) -> Self {
        Self::new(reply::CONTENT_TOO_LARGE, text, Level::Channel)
    }

    /// 320: operator-intervened shutdown.
    pub fn connection_forced(text: impl Into<String>) -> Self {
        Self::new(reply::CONNECTION_FORCED, text, Level::Connection)
    }

    /// 402: unknown virtual host in Connection.Open.
    pub fn invalid_path(text: impl Into<String>) -> Self {
        Self::new(reply::INVALID_PATH, text, Level::Connection)
    }

    /// 506: a resource could not be allocated.
    pub fn resource_error(text: impl Into<String>) -> Self {
        Self::new(reply::RESOURCE_ERROR, text, Level::Channel)
    }

    /// 312: consumer limit reached.
    #[allow(dead_code)]
    pub fn no_consumers(text: impl Into<String>) -> Self {
        Self::new(reply::NO_CONSUMERS, text, Level::Channel)
    }

    /// 200: success reply code, for tests and admin paths.
    pub fn success() -> Self {
        Self::new(reply::REPLY_SUCCESS, "OK", Level::Channel)
    }

    /// 503 at channel level: an invalid method argument in an otherwise
    /// operational context (e.g. an unknown exchange type spelling).
    pub fn command_invalid_channel(text: impl Into<String>) -> Self {
        Self::new(reply::COMMAND_INVALID, text, Level::Channel)
    }

    /// Force the exception to channel level.
    pub fn channel_level(mut self) -> Self {
        self.level = Level::Channel;
        self
    }

    /// Force the exception to connection level.
    pub fn connection_level(mut self) -> Self {
        self.level = Level::Connection;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_follow_the_spec_constants() {
        assert_eq!(BrokerError::not_found("nope").code, 404);
        assert_eq!(BrokerError::access_refused("x").code, 403);
        assert_eq!(BrokerError::resource_locked("x").code, 405);
        assert_eq!(BrokerError::precondition_failed("x").code, 406);
        assert_eq!(BrokerError::frame_error("x").code, 501);
        assert_eq!(BrokerError::syntax_error("x").code, 502);
        assert_eq!(BrokerError::command_invalid("x").code, 503);
        assert_eq!(BrokerError::channel_error("x").code, 504);
        assert_eq!(BrokerError::unexpected_frame("x").code, 505);
        assert_eq!(BrokerError::not_allowed("x").code, 530);
        assert_eq!(BrokerError::not_implemented("x").code, 540);
        assert_eq!(BrokerError::connection_forced("x").code, 320);
        assert_eq!(BrokerError::invalid_path("x").code, 402);
        assert_eq!(BrokerError::content_too_large("x").code, 311);
        assert_eq!(BrokerError::no_consumers("x").code, 312);
        assert_eq!(BrokerError::resource_error("x").code, 506);
        assert_eq!(BrokerError::success().code, 200);
    }

    #[test]
    fn levels_and_method_attachment() {
        let e = BrokerError::not_found("no exchange 'x'")
            .for_method(40, 20);
        assert_eq!(e.level, Level::Channel);
        assert_eq!((e.class_id, e.method_id), (40, 20));
        assert_eq!(BrokerError::frame_error("x").level, Level::Connection);
        // Display carries code + text, §4.8.2 style.
        assert_eq!(BrokerError::not_found("q").to_string(), "404 q");
    }
}

#[cfg(test)]
mod level_tests {
    use super::*;

    #[test]
    fn connection_level_forces_connection() {
        let e = BrokerError::not_found("x").channel_level().connection_level();
        assert_eq!(e.level, Level::Connection);
    }

    #[test]
    fn command_invalid_channel_is_channel_level() {
        let e = BrokerError::command_invalid_channel("bad");
        assert_eq!(e.level, Level::Channel);
        assert_eq!(e.code, 503);
    }
}
