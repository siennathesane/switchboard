//! The admin HTTP API under `/api/`: users, vhosts, permissions CRUD plus
//! queue and exchange listings. JSON in/out, mapped onto the EXISTING meta
//! state-machine commands (`CreateUser`, `DeleteUser`, `SetPermissions`,
//! `DeclareVhost`) — no new state, no new commands; every mutation
//! replicates through the meta raft group exactly like the AMQP/bridge
//! paths do.
//!
//! There is no `DeleteVhost` command in the meta SM, so `DELETE
//! /api/vhosts/{name}` answers 405 rather than improvising a
//! half-deletion through unrelated commands.
//!
//! Authentication: HTTP basic, verified by the same LOCAL credential
//! check every protocol gateway uses — `MetaState::check_credentials`
//! (argon2 verification against the replicated user table), wrapped by
//! [`super::shared::authorize`] so the guest-loopback rule applies here
//! too. Missing/invalid credentials → 401.
//!
//! Authorization (code-scanning alert #19): every `/api` operation —
//! listings included — requires the caller to be an ADMIN, decided by
//! the replicated `MetaState::admins` flag (the bootstrap user is
//! flagged at formation; `PUT /api/users/{name}/admin` grants/revokes
//! it, admin-only). Previously ANY authenticated user absent from the
//! permissions table was admin-equivalent, so every newly created user
//! silently owned the control plane.
//!
//! Grandfathering (default) so no existing deployment regresses: a user
//! is also treated as admin when they have a permissions entry whose
//! `configure` regex is exactly `".*"` (the conventional full-power
//! grant) — and, because formation flags it, the bootstrap-created user
//! carries the flag outright. Setting the environment variable
//! `SWITCHBOARD_ADMIN_STRICT` to a truthy value (anything but "", "0",
//! "false", case-insensitive) disables grandfathering: the flag becomes
//! the only path to the admin API, for strict deployments. Under both
//! modes a user restricted by the permissions table (without the
//! full-power entry) gets 403 on every `/api` operation, mirroring
//! RabbitMQ's management-tag requirement — and non-admin users are
//! still ordinary broker users: their AMQP/MQTT/STOMP/AMQP-1.0 access
//! is governed by the same permissions regexes as before.
//!
//! Served from [`super::http::serve`] on the client port, next to
//! `/stats` and `/metrics`; the endpoints ride the same `http` protocol
//! flag as the rest of the health surface.
//!
//! Routes (all JSON; percent-decoded segments, so the default vhost is
//! addressed as `%2F`):
//!
//! ```text
//! GET    /api/users                      → [{name, administrator, permissions}]
//! GET    /api/users/{name}               → {name, administrator, permissions} | 404
//! PUT    /api/users/{name}  {"password"} → CreateUser (create or rotate)
//! DELETE /api/users/{name}               → DeleteUser (cascades permissions)
//! PUT    /api/users/{name}/admin {"admin": true|false}
//!                                      → SetAdmin (grant/revoke; admin-only)
//! GET    /api/vhosts                     → [{name, queues, exchanges, bindings}]
//! PUT    /api/vhosts/{name}              → DeclareVhost (idempotent)
//! DELETE /api/vhosts/{name}              → 405 (no meta command exists)
//! GET    /api/permissions                → [{user, vhost, configure, write, read}]
//! GET    /api/permissions/{vhost}/{user} → entry | 404
//! PUT    /api/permissions/{vhost}/{user}
//!        {"configure","write","read"}    → SetPermissions
//! DELETE /api/permissions/{vhost}/{user} → SetPermissions (all-empty clears)
//! GET    /api/queues                     → [{vhost, name, durable, …}]
//! GET    /api/exchanges                  → [{vhost, name, type, …}]
//! ```

use std::sync::Arc;

use switchboard_cluster::BrokerCommand;
use switchboard_cluster::BrokerReply;
use switchboard_cluster::ClusterError;
use switchboard_cluster::ClusterNode;
use switchboard_cluster::META_GROUP;
use switchboard_core::error::BrokerError;
use switchboard_core::topology::MetaCmd;
use switchboard_core::topology::MetaReply;
use switchboard_core::topology::Permissions;
use switchboard_wire::constants::reply;

/// Upper bound on a request body (JSON payloads here are tiny).
const MAX_BODY: usize = 64 * 1024;

/// Serve one `/api` request. `head` is the already-read request head
/// (request line + headers); the body is read here.
pub async fn serve<S>(
    mut io: super::gateway::Prefixed<S>,
    head: &[u8],
    node: Arc<ClusterNode>,
    limits: crate::channel::ConnectionLimits,
) -> std::io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let first_line = head
        .split(|&b| b == b'\n')
        .next()
        .map(|l| String::from_utf8_lossy(l).into_owned())
        .unwrap_or_default();
    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_ascii_uppercase();
    let path = parts.next().unwrap_or("");
    let auth_header = header_value(head, "authorization");

    // The caller's `read_head` stops at the first `\r\n\r\n` but may have
    // read PAST it (the client often sends head and body in one write), so
    // any bytes after the head terminator are the body's beginning.
    let embedded = find_head_terminator(head)
        .map(|i| &head[i + 4..])
        .unwrap_or(&[]);

    // Read the body (if any) before responding.
    let content_length = header_value(head, "content-length")
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = embedded.to_vec();
    if content_length > 0 {
        if content_length > MAX_BODY {
            let _ = respond(
                &mut io,
                "413 Payload Too Large",
                None,
                "{\"error\":\"body too large\"}",
            )
            .await;
            return Ok(());
        }
        if body.len() < content_length {
            body.resize(content_length, 0);
            let mut filled = embedded.len();
            use tokio::io::AsyncReadExt;
            while filled < content_length {
                let n = io.read(&mut body[filled..]).await?;
                if n == 0 {
                    break;
                }
                filled += n;
            }
            body.truncate(filled);
        }
    }

    // Split BEFORE percent-decoding: an encoded `%2F` inside a segment is
    // data (the default vhost's name), not a separator.
    let rest = path.strip_prefix("/api").unwrap_or(path);
    let segments: Vec<String> = rest
        .split('/')
        .filter(|s| !s.is_empty())
        .map(percent_decode)
        .collect();

    let outcome = dispatch(
        &node,
        &limits,
        &method,
        &segments,
        auth_header.as_deref(),
        &body,
    )
    .await;
    let (status, extra, json) = match outcome {
        Ok(v) => ("200 OK", None, v),
        Err(ApiError::Unauthorized) => (
            "401 Unauthorized",
            Some("WWW-Authenticate: Basic realm=\"switchboard-admin\""),
            "{\"error\":\"authentication required\"}".to_string(),
        ),
        Err(ApiError::Forbidden(msg)) => (
            "403 Forbidden",
            None,
            serde_json::json!({"error": msg}).to_string(),
        ),
        Err(ApiError::NotFound(msg)) => (
            "404 Not Found",
            None,
            serde_json::json!({"error": msg}).to_string(),
        ),
        Err(ApiError::BadRequest(msg)) => (
            "400 Bad Request",
            None,
            serde_json::json!({"error": msg}).to_string(),
        ),
        Err(ApiError::MethodNotAllowed(msg)) => (
            "405 Method Not Allowed",
            None,
            serde_json::json!({"error": msg}).to_string(),
        ),
        Err(ApiError::Internal(msg)) => (
            "500 Internal Server Error",
            None,
            serde_json::json!({"error": msg}).to_string(),
        ),
    };
    let _ = respond(&mut io, status, extra, &json).await;
    Ok(())
}

/// One routed request: authentication, then authorization, then the
/// operation itself.
async fn dispatch(
    node: &Arc<ClusterNode>,
    limits: &crate::channel::ConnectionLimits,
    method: &str,
    segments: &[String],
    auth_header: Option<&str>,
    body: &[u8],
) -> Result<String, ApiError> {
    // ---- authentication (HTTP basic) ----
    let (user, pass) = parse_basic_auth(auth_header).ok_or(ApiError::Unauthorized)?;
    super::shared::authorize(
        node,
        &user,
        &pass,
        limits.peer_ip,
        limits.allow_remote_guest,
    )
    .await
    .map_err(|_| ApiError::Unauthorized)?;

    // ---- authorization: the control plane is for ADMIN users ----
    // (code-scanning alert #19). The replicated admin flag is the real
    // gate; grandfathering keeps pre-flag deployments working (a
    // configure-".*" permissions entry is the conventional full-power
    // grant) unless SWITCHBOARD_ADMIN_STRICT disables it. This subsumes
    // the old "restricted users are refused" rule: a user restricted
    // without the full-power entry is simply not an admin.
    {
        let topo = node.topology();
        let strict = admin_strict_mode();
        let flagged = topo.admins.contains(&user);
        let grandfathered = !strict
            && (user == switchboard_wire::constants::DEFAULT_USER
                || topo
                    .permissions
                    .get(&user)
                    .is_some_and(|per_vhost| per_vhost.values().any(|p| p.configure == ".*")));
        if !flagged && !grandfathered {
            return Err(ApiError::Forbidden(format!(
                "user {user:?} is not an admin; the admin API is limited to admin-flagged users{}",
                if strict {
                    " (SWITCHBOARD_ADMIN_STRICT is on: the flag is the only path)"
                } else {
                    " (grant it with PUT /api/users/{name}/admin)"
                }
            )));
        }
    }

    let seg = |i: usize| segments.get(i).map(|s| s.as_str());
    match (method, seg(0), seg(1), seg(2)) {
        ("GET", Some("users"), None, _) => list_users(node),
        ("GET", Some("users"), Some(name), _) => get_user(node, name),
        // PUT /api/users/{name}/admin — grant/revoke the admin flag. This
        // arm MUST precede the generic PUT /api/users/{name} arm below,
        // which would otherwise swallow the third segment (code-scanning
        // alert #19).
        ("PUT", Some("users"), Some(name), Some("admin")) => put_user_admin(node, name, body).await,
        ("PUT", Some("users"), Some(name), _) => put_user(node, name, body).await,
        ("DELETE", Some("users"), Some(name), _) => delete_user(node, name).await,
        ("GET", Some("vhosts"), None, _) => list_vhosts(node),
        ("PUT", Some("vhosts"), Some(name), _) => put_vhost(node, name).await,
        ("DELETE", Some("vhosts"), Some(_), _) => Err(ApiError::MethodNotAllowed(
            "no vhost-deletion command exists in the meta state machine".into(),
        )),
        ("GET", Some("permissions"), None, _) => list_permissions(node),
        ("GET", Some("permissions"), Some(vhost), Some(user)) => get_permission(node, vhost, user),
        ("PUT", Some("permissions"), Some(vhost), Some(user)) => {
            put_permission(node, vhost, user, body).await
        }
        ("DELETE", Some("permissions"), Some(vhost), Some(user)) => {
            clear_permission(node, vhost, user).await
        }
        ("GET", Some("queues"), _, _) => list_queues(node),
        ("GET", Some("exchanges"), _, _) => list_exchanges(node),
        _ => Err(ApiError::NotFound(format!(
            "no admin API route for {method} /api/{}",
            segments.join("/")
        ))),
    }
}

// ---- request parsing helpers ----

/// `SWITCHBOARD_ADMIN_STRICT` (default off): set to any truthy value —
/// everything except the empty string, "0", and "false"
/// (case-insensitive) — the replicated admin flag becomes the ONLY path
/// to the admin API; grandfathered full-power users stop counting
/// (code-scanning alert #19, for strict deployments).
fn admin_strict_mode() -> bool {
    match std::env::var("SWITCHBOARD_ADMIN_STRICT") {
        Ok(v) => !v.is_empty() && !v.eq_ignore_ascii_case("0") && !v.eq_ignore_ascii_case("false"),
        Err(_) => false,
    }
}

/// `Authorization: Basic base64(user:password)` → `(user, password)`.
fn parse_basic_auth(header: Option<&str>) -> Option<(String, String)> {
    let header = header?.trim();
    let (scheme, encoded) = header.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("basic") {
        return None;
    }
    use base64::Engine;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim())
        .ok()?;
    let text = String::from_utf8(decoded).ok()?;
    // RFC 7617: split on the FIRST colon; the password may contain colons.
    let (user, pass) = text.split_once(':')?;
    if user.is_empty() {
        return None;
    }
    Some((user.to_string(), pass.to_string()))
}

/// Byte offset of the first `\r\n\r\n` in `head` (the head terminator).
fn find_head_terminator(head: &[u8]) -> Option<usize> {
    head.windows(4).position(|w| w == b"\r\n\r\n")
}

fn header_value(head: &[u8], name: &str) -> Option<String> {
    // Only the head proper carries headers; `head` may have body bytes
    // glued past the terminator, which must not be scanned.
    let end = find_head_terminator(head).unwrap_or(head.len());
    let text = String::from_utf8_lossy(&head[..end]);
    for line in text.split("\r\n").skip(1) {
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        if k.eq_ignore_ascii_case(name) {
            return Some(v.to_string());
        }
    }
    None
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    let hex = |c: u8| -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    };
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let (Some(h), Some(l)) = (hex(b[i + 1]), hex(b[i + 2])) {
                out.push(h * 16 + l);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

// ---- HTTP writer ----

async fn respond<S>(
    io: &mut S,
    status: &str,
    extra_header: Option<&str>,
    body: &str,
) -> std::io::Result<()>
where
    S: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;
    let mut response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    if let Some(extra) = extra_header {
        response.push_str(extra);
        response.push_str("\r\n");
    }
    response.push_str("\r\n");
    response.push_str(body);
    io.write_all(response.as_bytes()).await?;
    io.flush().await?;
    io.shutdown().await?;
    Ok(())
}

// ---- errors ----

enum ApiError {
    Unauthorized,
    Forbidden(String),
    NotFound(String),
    BadRequest(String),
    MethodNotAllowed(String),
    Internal(String),
}

impl ApiError {
    fn from_broker(e: BrokerError) -> Self {
        match e.code {
            reply::NOT_FOUND => ApiError::NotFound(e.text),
            reply::ACCESS_REFUSED => ApiError::Forbidden(e.text),
            reply::PRECONDITION_FAILED => ApiError::BadRequest(e.text),
            _ => ApiError::BadRequest(format!("broker refused: {}", e)),
        }
    }
}

/// Run one meta write and fold every error channel into [`ApiError`].
/// Apply errors DO travel back through the reply (`BrokerReply::Error`,
/// folded by the state-machine apply), and forwarding failures surface as
/// `ClusterError::Broker` — both mean the operation did not take effect.
async fn meta_write(node: &Arc<ClusterNode>, cmd: MetaCmd) -> Result<MetaReply, ApiError> {
    let broker_reply = node
        .write(META_GROUP, BrokerCommand::Meta(cmd))
        .await
        .map_err(|e| match e {
            ClusterError::Broker(b) => ApiError::from_broker(b),
            other => ApiError::Internal(other.to_string()),
        })?;
    match broker_reply {
        BrokerReply::Meta(r) => Ok(r),
        BrokerReply::Error(e) => Err(ApiError::from_broker(e)),
        other => Err(ApiError::Internal(format!(
            "unexpected meta reply {other:?}"
        ))),
    }
}

/// Run one meta write and refresh the local topology view before
/// returning, so a GET immediately after a PUT observes the write
/// (read-your-writes; the same discipline the bridge declare/bind paths
/// apply in [`super::shared`]).
async fn meta_write_then_refresh(
    node: &Arc<ClusterNode>,
    cmd: MetaCmd,
) -> Result<MetaReply, ApiError> {
    let reply = meta_write(node, cmd).await?;
    node.refresh_topology().await;
    Ok(reply)
}

// ---- reads (local replicated snapshot; no raft round-trip) ----

fn permissions_json(p: &Permissions) -> serde_json::Value {
    serde_json::json!({
        "configure": p.configure,
        "write": p.write,
        "read": p.read,
    })
}

fn list_users(node: &Arc<ClusterNode>) -> Result<String, ApiError> {
    let topo = node.topology();
    let users: Vec<serde_json::Value> = topo
        .users
        .keys()
        .map(|name| {
            serde_json::json!({
                "name": name,
                "administrator": topo.admins.contains(name),
                "permissions": topo
                    .permissions
                    .get(name)
                    .map(|per_vhost| per_vhost
                        .iter()
                        .map(|(vh, p)| (vh.clone(), permissions_json(p)))
                        .collect::<serde_json::Map<String, serde_json::Value>>())
                    .unwrap_or_default(),
            })
        })
        .collect();
    Ok(serde_json::to_string(&users).unwrap_or_else(|_| "[]".into()))
}

fn get_user(node: &Arc<ClusterNode>, name: &str) -> Result<String, ApiError> {
    let topo = node.topology();
    if !topo.users.contains_key(name) {
        return Err(ApiError::NotFound(format!("no user {name:?}")));
    }
    let permissions: serde_json::Map<String, serde_json::Value> = topo
        .permissions
        .get(name)
        .map(|per_vhost| {
            per_vhost
                .iter()
                .map(|(vh, p)| (vh.clone(), permissions_json(p)))
                .collect()
        })
        .unwrap_or_default();
    Ok(serde_json::json!({
        "name": name,
        "administrator": topo.admins.contains(name),
        "permissions": permissions,
    })
    .to_string())
}

fn list_vhosts(node: &Arc<ClusterNode>) -> Result<String, ApiError> {
    let topo = node.topology();
    let vhosts: Vec<serde_json::Value> = topo
        .vhosts
        .iter()
        .map(|(name, v)| {
            serde_json::json!({
                "name": name,
                "queues": v.queues.len(),
                "exchanges": v.exchanges.len(),
                "bindings": v.bindings.len(),
            })
        })
        .collect();
    Ok(serde_json::to_string(&vhosts).unwrap_or_else(|_| "[]".into()))
}

fn list_permissions(node: &Arc<ClusterNode>) -> Result<String, ApiError> {
    let topo = node.topology();
    let mut out = Vec::new();
    for (user, per_vhost) in &topo.permissions {
        for (vhost, p) in per_vhost {
            out.push(serde_json::json!({
                "user": user,
                "vhost": vhost,
                "configure": p.configure,
                "write": p.write,
                "read": p.read,
            }));
        }
    }
    Ok(serde_json::to_string(&out).unwrap_or_else(|_| "[]".into()))
}

fn get_permission(node: &Arc<ClusterNode>, vhost: &str, user: &str) -> Result<String, ApiError> {
    let topo = node.topology();
    match topo
        .permissions
        .get(user)
        .and_then(|per_vhost| per_vhost.get(vhost))
    {
        Some(p) => Ok(serde_json::json!({
            "user": user,
            "vhost": vhost,
            "configure": p.configure,
            "write": p.write,
            "read": p.read,
        })
        .to_string()),
        None => Err(ApiError::NotFound(format!(
            "no permissions for user {user:?} in vhost {vhost:?}"
        ))),
    }
}

fn list_queues(node: &Arc<ClusterNode>) -> Result<String, ApiError> {
    let topo = node.topology();
    let mut out = Vec::new();
    for (vhost, v) in &topo.vhosts {
        for (name, qi) in &v.queues {
            out.push(serde_json::json!({
                "vhost": vhost,
                "name": name,
                "durable": qi.options.durable,
                "exclusive": qi.options.exclusive,
                "auto_delete": qi.options.auto_delete,
                "shard": qi.shard,
                "owner": qi.owner.map(|o| serde_json::json!({"node": o.node, "conn": o.conn})),
            }));
        }
    }
    Ok(serde_json::to_string(&out).unwrap_or_else(|_| "[]".into()))
}

fn list_exchanges(node: &Arc<ClusterNode>) -> Result<String, ApiError> {
    let topo = node.topology();
    let mut out = Vec::new();
    for (vhost, v) in &topo.vhosts {
        for (name, e) in &v.exchanges {
            out.push(serde_json::json!({
                "vhost": vhost,
                "name": name,
                "type": e.kind,
                "durable": e.durable,
                "auto_delete": e.auto_delete,
                "internal": e.internal,
            }));
        }
    }
    Ok(serde_json::to_string(&out).unwrap_or_else(|_| "[]".into()))
}

// ---- writes (replicated meta commands) ----

async fn put_user(node: &Arc<ClusterNode>, name: &str, body: &[u8]) -> Result<String, ApiError> {
    if name.is_empty() {
        return Err(ApiError::BadRequest("user name must not be empty".into()));
    }
    let payload: serde_json::Value = serde_json::from_slice(body)
        .map_err(|e| ApiError::BadRequest(format!("invalid JSON body: {e}")))?;
    let password = payload["password"]
        .as_str()
        .ok_or_else(|| ApiError::BadRequest("body must carry {\"password\": \"...\"}".into()))?
        .to_string();
    // CreateUser inserts or replaces the stored (hashed) credential: the
    // meta SM's create path is the password-rotate path too.
    meta_write_then_refresh(
        node,
        MetaCmd::CreateUser {
            name: name.into(),
            password,
        },
    )
    .await?;
    Ok(serde_json::json!({"name": name, "status": "ok"}).to_string())
}

/// `PUT /api/users/{name}/admin` with `{"admin": true|false}`: grant or
/// revoke the admin flag through the replicated [`MetaCmd::SetAdmin`]
/// (code-scanning alert #19). Admin-only (the dispatch gate above) and
/// refused for a user that does not exist, like SetPermissions.
async fn put_user_admin(
    node: &Arc<ClusterNode>,
    name: &str,
    body: &[u8],
) -> Result<String, ApiError> {
    let payload: serde_json::Value = serde_json::from_slice(body)
        .map_err(|e| ApiError::BadRequest(format!("invalid JSON body: {e}")))?;
    let admin = payload["admin"]
        .as_bool()
        .ok_or_else(|| ApiError::BadRequest("body must carry {\"admin\": true|false}".into()))?;
    meta_write_then_refresh(
        node,
        MetaCmd::SetAdmin {
            user: name.into(),
            admin,
        },
    )
    .await?;
    Ok(serde_json::json!({"name": name, "administrator": admin, "status": "ok"}).to_string())
}

async fn delete_user(node: &Arc<ClusterNode>, name: &str) -> Result<String, ApiError> {
    // DeleteUser removes the user and cascades their permissions; it is
    // idempotent (removing an absent user still applies Ok), so this
    // answers success either way.
    meta_write_then_refresh(node, MetaCmd::DeleteUser { name: name.into() }).await?;
    Ok(serde_json::json!({"name": name, "status": "deleted"}).to_string())
}

async fn put_vhost(node: &Arc<ClusterNode>, name: &str) -> Result<String, ApiError> {
    if name.is_empty() {
        return Err(ApiError::BadRequest("vhost name must not be empty".into()));
    }
    // DeclareVhost is idempotent and installs the mandatory exchanges on a
    // fresh vhost.
    meta_write_then_refresh(node, MetaCmd::DeclareVhost { name: name.into() }).await?;
    Ok(serde_json::json!({"name": name, "status": "ok"}).to_string())
}

async fn put_permission(
    node: &Arc<ClusterNode>,
    vhost: &str,
    user: &str,
    body: &[u8],
) -> Result<String, ApiError> {
    let payload: serde_json::Value = serde_json::from_slice(body)
        .map_err(|e| ApiError::BadRequest(format!("invalid JSON body: {e}")))?;
    let permissions = Permissions {
        configure: payload["configure"].as_str().unwrap_or("").to_string(),
        write: payload["write"].as_str().unwrap_or("").to_string(),
        read: payload["read"].as_str().unwrap_or("").to_string(),
    };
    // The meta SM refuses SetPermissions for an absent user; a missing
    // vhost would install a dead entry, so check it here first.
    if !node.topology().users.contains_key(user) {
        return Err(ApiError::NotFound(format!("no user {user:?}")));
    }
    if !node.topology().vhosts.contains_key(vhost) {
        return Err(ApiError::NotFound(format!("no vhost {vhost:?}")));
    }
    meta_write_then_refresh(
        node,
        MetaCmd::SetPermissions {
            user: user.into(),
            vhost: vhost.into(),
            permissions,
        },
    )
    .await?;
    Ok(serde_json::json!({"user": user, "vhost": vhost, "status": "ok"}).to_string())
}

async fn clear_permission(
    node: &Arc<ClusterNode>,
    vhost: &str,
    user: &str,
) -> Result<String, ApiError> {
    if !node.topology().users.contains_key(user) {
        return Err(ApiError::NotFound(format!("no user {user:?}")));
    }
    // All-empty patterns clear the entry (meta SM semantics).
    meta_write_then_refresh(
        node,
        MetaCmd::SetPermissions {
            user: user.into(),
            vhost: vhost.into(),
            permissions: Permissions::default(),
        },
    )
    .await?;
    Ok(serde_json::json!({"user": user, "vhost": vhost, "status": "cleared"}).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_decoding() {
        assert_eq!(percent_decode("%2F"), "/");
        assert_eq!(percent_decode("app%2Fx"), "app/x");
        assert_eq!(percent_decode("plain"), "plain");
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("%zz"), "%zz");
        assert_eq!(percent_decode("%41"), "A");
    }

    #[test]
    fn header_lookup_is_case_insensitive() {
        let head = b"GET /api/users HTTP/1.1\r\nHost: x\r\naUtHoRiZaTiOn: Basic abc\r\n\r\n";
        assert_eq!(
            header_value(head, "authorization").as_deref(),
            Some(" Basic abc")
        );
        assert_eq!(header_value(head, "content-length"), None);
        let head2 = b"PUT /api/users HTTP/1.1\r\nContent-Length: 25\r\n\r\n";
        assert_eq!(
            header_value(head2, "CONTENT-LENGTH").as_deref(),
            Some(" 25")
        );
    }

    #[test]
    fn body_bytes_glued_to_head_are_not_headers() {
        // A client that sends head and body in one write leaves the body
        // bytes past the terminator; a JSON body's `password:` key must
        // never surface as an Authorization header.
        let head = b"PUT /api/users/x HTTP/1.1\r\nHost: x\r\n\r\n{\"password\":\"coloned\"}";
        assert_eq!(header_value(head, "authorization"), None);
        assert_eq!(header_value(head, "content-length"), None);
        let i = find_head_terminator(head).unwrap();
        assert_eq!(&head[i + 4..], b"{\"password\":\"coloned\"}");
    }

    #[test]
    fn basic_auth_parsing() {
        let b64 = |s: &str| {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD.encode(s)
        };
        assert_eq!(
            parse_basic_auth(Some(&format!("Basic {}", b64("user:pw")))),
            Some(("user".into(), "pw".into()))
        );
        // Password may contain a colon; split on the FIRST one.
        assert_eq!(
            parse_basic_auth(Some(&format!("Basic {}", b64("user:p:w")))),
            Some(("user".into(), "p:w".into()))
        );
        assert_eq!(parse_basic_auth(Some("Bearer xyz")), None);
        assert_eq!(parse_basic_auth(None), None);
        assert_eq!(parse_basic_auth(Some("Basic !notbase64!")), None);
        assert_eq!(
            parse_basic_auth(Some(&format!("Basic {}", b64(":pw")))),
            None
        );
        // The raw header value carries the colon's trailing space.
        assert_eq!(
            parse_basic_auth(Some(&format!(" Basic {}", b64("u:p")))),
            Some(("u".into(), "p".into()))
        );
    }
}
