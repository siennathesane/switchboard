//! Exchange routing (§3.1.3): which queues receive a published message.
//!
//! Routing happens against a *topology snapshot* — the set of exchanges,
//! bindings and queues the receiving node currently knows. The snapshot is
//! the local replica of the meta group's applied state; publishes do not
//! block on the meta leader, mirroring how real brokers treat the binding
//! table as eventually-consistent routing configuration.

use crate::model::{Binding, Exchange, ExchangeKind};
use crate::topic;
use switchboard_wire::field::{FieldValue, FieldTable};
use switchboard_wire::BasicProperties;

/// Everything routing needs from the topology.
pub trait TopologyView {
    fn exchange(&self, name: &str) -> Option<&Exchange>;
    fn bindings_of(&self, exchange: &str) -> impl Iterator<Item = &Binding>;
    fn queue_exists(&self, name: &str) -> bool;
}

/// Resolve a publish to destination queue names, in binding order.
///
/// * The default exchange (empty name) routes to the queue named by the
///   routing key — "all message queues MUST BE automatically bound to the
///   nameless exchange using the message queue's name as routing key"
///   (§3.1.3.1).
/// * Unroutable messages vanish (the publisher chose via `mandatory`).
/// * Returns `Err(queue)`-free semantics: the caller inspects emptiness.
pub fn route<T: TopologyView + ?Sized>(
    topo: &T,
    exchange: &str,
    routing_key: &str,
    properties: &BasicProperties,
) -> Vec<String> {
    // The default exchange (§3.1.3.1): every queue is implicitly bound by
    // its own name, so the routing key addresses a queue directly.
    if exchange.is_empty() {
        return if topo.queue_exists(routing_key) {
            vec![routing_key.to_string()]
        } else {
            vec![]
        };
    }
    let Some(ex) = topo.exchange(exchange) else {
        return Vec::new();
    };
    let mut dest: Vec<String> = Vec::new();
    let mut visited: Vec<String> = vec![exchange.to_string()];
    expand_routes(
        topo,
        exchange,
        ex,
        routing_key,
        properties,
        &mut dest,
        &mut visited,
    );
    dest
}

/// Depth-first expansion of an exchange's bindings. A binding whose
/// destination is itself an exchange (exchange-to-exchange binding) is
/// followed recursively; cycle-guarded by `visited`.
fn expand_routes<T: TopologyView + ?Sized>(
    topo: &T,
    exchange: &str,
    ex: &crate::model::Exchange,
    routing_key: &str,
    properties: &BasicProperties,
    dest: &mut Vec<String>,
    visited: &mut Vec<String>,
) {
    for b in topo.bindings_of(exchange) {
        if dest.iter().any(|q| q == &b.queue) {
            continue; // one copy per queue even with duplicate bindings
        }
        // Exchange-to-exchange: the destination is another exchange.
        if topo.queue_exists(&b.queue) {
            if binding_matches(ex, b, routing_key, properties) {
                dest.push(b.queue.clone());
            }
            continue;
        }
        if let Some(inner) = topo.exchange(&b.queue) {
            if visited.iter().any(|v| v == &b.queue) {
                continue; // cycle guard
            }
            let binding_matched =
                binding_matches(ex, b, routing_key, properties);
            if binding_matched {
                visited.push(b.queue.clone());
                expand_routes(
                    topo,
                    &b.queue,
                    inner,
                    routing_key,
                    properties,
                    dest,
                    visited,
                );
                visited.pop();
            }
            continue;
        }
        // Destination vanished (queue deleted): binding is stale; skip.
    }
}

/// Per-binding match rule, selected by the exchange type.
pub fn binding_matches(
    ex: &Exchange,
    b: &Binding,
    routing_key: &str,
    properties: &BasicProperties,
) -> bool {
    match ex.kind {
        ExchangeKind::Direct => b.routing_key == routing_key,
        ExchangeKind::Fanout => true,
        ExchangeKind::Topic => topic::matches(&b.routing_key, routing_key),
        ExchangeKind::Headers => headers_match(&b.arguments, properties.headers.as_ref()),
    }
}

/// Headers matching (§3.1.3.4).
///
/// The binding's `x-match` argument selects `all` (default) or `any`. "A
/// field in the bind arguments matches a field in the message if either the
/// field in the bind arguments has no value and a field of the same name is
/// present in the message headers or if the field ... has a value and a
/// field of the same name exists in the message headers and has that same
/// value." Any argument other than `x-match` starting with `x-` is ignored.
pub fn headers_match(bind_args: &FieldTable, message_headers: Option<&FieldTable>) -> bool {
    let mode = match bind_args.get("x-match") {
        Some(FieldValue::ShortString(s)) if s == "any" => MatchMode::Any,
        _ => MatchMode::All,
    };

    let mut any = false;
    let mut all = true;
    let mut saw_field = false;
    for (name, bind_value) in bind_args.iter() {
        if name.starts_with("x-") {
            continue; // reserved; only x-match is meaningful
        }
        saw_field = true;
        let msg_value = message_headers.and_then(|h| h.get(name));
        let matched = match (bind_value, msg_value) {
            // Argument with no value: presence in the message suffices. The
            // void value (`V`) is how clients spell "no value".
            (FieldValue::Void, Some(_)) => true,
            (FieldValue::Void, None) => false,
            // Argument with a value: same-named field with that same value.
            (v, Some(mv)) => v == mv,
            (_, None) => false,
        };
        any |= matched;
        all &= matched;
    }

    match mode {
        MatchMode::All => saw_field && all,
        MatchMode::Any => saw_field && any,
    }
}

#[derive(PartialEq)]
enum MatchMode {
    All,
    Any,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::exchange;
    use std::collections::BTreeMap;

    struct FakeTopo {
        exchanges: BTreeMap<String, Exchange>,
        bindings: Vec<Binding>,
        queues: Vec<String>,
    }

    impl FakeTopo {
        fn new() -> Self {
            FakeTopo { exchanges: BTreeMap::new(), bindings: Vec::new(), queues: Vec::new() }
        }
        fn ex(mut self, name: &str, kind: ExchangeKind) -> Self {
            self.exchanges.insert(name.into(), exchange(name, kind, false));
            self
        }
        fn bind(mut self, ex: &str, q: &str, key: &str) -> Self {
            self.bindings.push(Binding {
                exchange: ex.into(),
                queue: q.into(),
                routing_key: key.into(),
                arguments: FieldTable::new(),
            });
            self.queues.push(q.into());
            self
        }
    }

    impl TopologyView for FakeTopo {
        fn exchange(&self, name: &str) -> Option<&Exchange> {
            self.exchanges.get(name)
        }
        fn bindings_of(&self, exchange: &str) -> impl Iterator<Item = &Binding> {
            self.bindings.iter().filter(move |b| b.exchange == exchange)
        }
        fn queue_exists(&self, name: &str) -> bool {
            self.queues.iter().any(|q| q == name)
        }
    }

    fn props() -> BasicProperties {
        BasicProperties::new()
    }

    #[test]
    fn default_exchange_routes_by_queue_name() {
        let topo = FakeTopo::new().bind("", "jobs", "jobs");
        let dest = route(&topo, "", "jobs", &props());
        assert_eq!(dest, vec!["jobs"]);
        assert!(route(&topo, "", "other", &props()).is_empty());
    }

    #[test]
    fn direct_matches_exact_keys() {
        let topo = FakeTopo::new()
            .ex("d", ExchangeKind::Direct)
            .bind("d", "a", "k1")
            .bind("d", "b", "k2");
        assert_eq!(route(&topo, "d", "k1", &props()), vec!["a"]);
        assert_eq!(route(&topo, "d", "k2", &props()), vec!["b"]);
        assert!(route(&topo, "d", "k3", &props()).is_empty());
    }

    #[test]
    fn fanout_hits_every_queue_once() {
        let topo = FakeTopo::new()
            .ex("f", ExchangeKind::Fanout)
            .bind("f", "a", "ignored")
            .bind("f", "b", "whatever")
            .bind("f", "a", "duplicate");
        let mut dest = route(&topo, "f", "", &props());
        dest.sort();
        assert_eq!(dest, vec!["a", "b"]);
    }

    #[test]
    fn topic_uses_patterns() {
        let topo = FakeTopo::new()
            .ex("t", ExchangeKind::Topic)
            .bind("t", "usd", "*.stock.#")
            .bind("t", "all", "#");
        let dest = route(&topo, "t", "usd.stock.nyse", &props());
        assert!(dest.contains(&"usd".to_string()) && dest.contains(&"all".to_string()));
        assert_eq!(route(&topo, "t", "gbp.bond", &props()), vec!["all"]);
    }

    #[test]
    fn unknown_exchange_and_missing_queues_drop() {
        let topo = FakeTopo::new().ex("d", ExchangeKind::Direct).bind("d", "gone", "k");
        assert!(route(&topo, "missing", "k", &props()).is_empty());
        // Queue "gone" is not in the queue list: binding is dead.
        let topo = FakeTopo { queues: vec![], ..FakeTopo::new().ex("d", ExchangeKind::Direct).bind("d", "gone", "k") };
        assert!(route(&topo, "d", "k", &props()).is_empty());
    }

    #[test]
    fn headers_all_mode() {
        let args_all = {
            let mut a = FieldTable::new();
            a.insert("x-match", FieldValue::ShortString("all".into()));
            a.insert("format", FieldValue::ShortString("pdf".into()));
            a.insert("type", FieldValue::ShortString("report".into()));
            a
        };

        let mut headers = FieldTable::new();
        headers.insert("format", FieldValue::ShortString("pdf".into()));
        headers.insert("type", FieldValue::ShortString("report".into()));
        assert!(headers_match(&args_all, Some(&headers)));

        // One mismatching field fails "all".
        let mut h2 = FieldTable::new();
        h2.insert("format", FieldValue::ShortString("pdf".into()));
        h2.insert("type", FieldValue::ShortString("invoice".into()));
        assert!(!headers_match(&args_all, Some(&h2)));

        // Extra message fields do not hurt "all".
        let mut h3 = FieldTable::new();
        h3.insert("format", FieldValue::ShortString("pdf".into()));
        h3.insert("type", FieldValue::ShortString("report".into()));
        h3.insert("extra", FieldValue::SignedInt(7));
        assert!(headers_match(&args_all, Some(&h3)));

        // Value equality is typed.
        let mut wrong_type = FieldTable::new();
        wrong_type.insert("format", FieldValue::ShortString("pdf".into()));
        wrong_type.insert("type", FieldValue::SignedInt(1));
        assert!(!headers_match(&args_all, Some(&wrong_type)));
    }

    #[test]
    fn headers_presence_only_and_any_mode() {
        // Presence-only: bind value of type Void.
        let mut args = FieldTable::new();
        args.insert("x-match", FieldValue::ShortString("any".into()));
        args.insert("signed", FieldValue::Void);
        args.insert("urgent", FieldValue::Void);

        let mut signed_only = FieldTable::new();
        signed_only.insert("signed", FieldValue::Boolean(true));
        let mut p = props();
        p.headers = Some(signed_only);
        assert!(headers_match(&args, p.headers.as_ref()));

        let mut none = FieldTable::new();
        none.insert("other", FieldValue::Void);
        let mut p2 = props();
        p2.headers = Some(none);
        assert!(!headers_match(&args, p2.headers.as_ref()));
    }

    #[test]
    fn headers_reserved_args_ignored_and_empty_bindings_never_match() {
        let mut args = FieldTable::new();
        args.insert("x-match", FieldValue::ShortString("all".into()));
        args.insert("x-custom", FieldValue::SignedInt(1));
        // No non-reserved fields: nothing to match -> false even with headers.
        let mut h = FieldTable::new();
        h.insert("x-custom", FieldValue::SignedInt(1));
        let mut p = props();
        p.headers = Some(h);
        assert!(!headers_match(&args, p.headers.as_ref()));
    }

    #[test]
    fn headers_without_message_headers_never_match() {
        let mut args = FieldTable::new();
        args.insert("a", FieldValue::ShortString("1".into()));
        assert!(!headers_match(&args, None));
        // mode "any" with no message headers also fails.
        let mut args2 = FieldTable::new();
        args2.insert("x-match", FieldValue::ShortString("any".into()));
        args2.insert("a", FieldValue::ShortString("1".into()));
        assert!(!headers_match(&args2, None));
    }
}

#[cfg(test)]
mod e2e_exchange_tests {
    use super::*;
    use crate::model::{Exchange, ExchangeKind};
    use switchboard_wire::field::FieldTable;

    struct Vw {
        exchanges: std::collections::BTreeMap<String, Exchange>,
        bindings: std::collections::BTreeMap<String, Vec<Binding>>,
        queues: std::collections::BTreeMap<String, ()>,
    }

    impl TopologyView for Vw {
        fn exchange(&self, name: &str) -> Option<&Exchange> {
            self.exchanges.get(name)
        }
        fn queue_exists(&self, name: &str) -> bool {
            self.queues.contains_key(name)
        }
        fn bindings_of(&self, exchange: &str) -> impl Iterator<Item = &Binding> {
            self.bindings.get(exchange).into_iter().flatten()
        }
    }

    fn ex(kind: ExchangeKind) -> Exchange {
        Exchange {
            name: String::new(),
            kind,
            durable: true,
            auto_delete: false,
            internal: false,
            arguments: FieldTable::new(),
        }
    }

    fn b(exchange: &str, queue: &str, rk: &str) -> Binding {
        Binding {
            exchange: exchange.into(),
            queue: queue.into(),
            routing_key: rk.into(),
            arguments: FieldTable::new(),
        }
    }

    fn vw() -> Vw {
        let exchanges = [
            ("src".to_string(), ex(ExchangeKind::Direct)),
            ("dst".to_string(), ex(ExchangeKind::Direct)),
        ]
        .into_iter()
        .collect();
        let bindings = [
            ("src".to_string(), vec![b("src", "dst", "k")]),
            ("dst".to_string(), vec![b("dst", "final-q", "k")]),
        ]
        .into_iter()
        .collect();
        let queues = [("final-q".to_string(), ())].into_iter().collect();
        Vw { exchanges, bindings, queues }
    }

    #[test]
    fn exchange_to_exchange_chain_routes_to_terminal_queue() {
        let v = vw();
        let props = BasicProperties::new();
        assert_eq!(route(&v, "src", "k", &props), vec!["final-q".to_string()]);
        assert!(route(&v, "src", "other", &props).is_empty());
    }

    #[test]
    fn exchange_cycle_terminates() {
        let mut v = vw();
        // src → dst and dst → src (cycle), plus a real queue on dst.
        v.bindings.get_mut("dst").unwrap().push(b("dst", "src", "k"));
        let props = BasicProperties::new();
        assert_eq!(route(&v, "src", "k", &props), vec!["final-q".to_string()]);
    }

    #[test]
    fn destination_vanished_is_skipped() {
        let mut v = vw();
        // The bound destination is neither queue nor exchange anymore.
        v.queues.remove("final-q");
        let props = BasicProperties::new();
        assert!(route(&v, "src", "k", &props).is_empty());
    }
}

#[cfg(test)]
mod headers_binding_tests {
    use super::*;
    use switchboard_wire::field::{FieldValue, FieldTable};

    fn ex_headers() -> Exchange {
        Exchange {
            name: "h".into(),
            kind: ExchangeKind::Headers,
            durable: true,
            auto_delete: false,
            internal: false,
            arguments: FieldTable::new(),
        }
    }

    fn bind_args(pairs: &[(&str, FieldValue)]) -> FieldTable {
        let mut t = FieldTable::new();
        for (k, v) in pairs {
            t.insert((*k).to_string(), v.clone());
        }
        t
    }

    fn b(exchange: &str, queue: &str, rk: &str) -> Binding {
        Binding {
            exchange: exchange.into(),
            queue: queue.into(),
            routing_key: rk.into(),
            arguments: FieldTable::new(),
        }
    }

    fn bind(args: FieldTable) -> Binding {
        Binding { exchange: "h".into(), queue: "q".into(), routing_key: String::new(), arguments: args }
    }

    #[test]
    fn headers_all_mode_matches_value_and_presence() {
        let ex = ex_headers();
        let args = bind_args(&[
            ("a", FieldValue::LongString(b"1".to_vec())),
            ("b", FieldValue::Void),
        ]);
        let mut msg_headers = FieldTable::new();
        msg_headers.insert("a", FieldValue::LongString(b"1".to_vec()));
        msg_headers.insert("b", FieldValue::LongString(b"anything".to_vec()));
        let props = BasicProperties { headers: Some(msg_headers), ..BasicProperties::new() };
        assert!(binding_matches(&ex, &bind(args.clone()), "rk", &props));

        // Wrong value for a → no match.
        let mut wrong = FieldTable::new();
        wrong.insert("a", FieldValue::LongString(b"2".to_vec()));
        wrong.insert("b", FieldValue::LongString(b"anything".to_vec()));
        let props = BasicProperties { headers: Some(wrong), ..BasicProperties::new() };
        assert!(!binding_matches(&ex, &bind(args), "rk", &props));
    }

    #[test]
    fn headers_any_mode_needs_one_match() {
        let ex = ex_headers();
        let args = bind_args(&[
            ("x-match", FieldValue::ShortString("any".into())),
            ("a", FieldValue::LongString(b"1".to_vec())),
            ("missing", FieldValue::LongString(b"zz".to_vec())),
        ]);
        let mut msg_headers = FieldTable::new();
        msg_headers.insert("a", FieldValue::LongString(b"1".to_vec()));
        let props = BasicProperties { headers: Some(msg_headers), ..BasicProperties::new() };
        assert!(binding_matches(&ex, &bind(args), "rk", &props));
    }

    #[test]
    fn direct_binding_requires_equal_routing_key() {
        let mut ex = ex_headers();
        ex.kind = ExchangeKind::Direct;
        let bind = b("direct", "q", "k");
        let props = BasicProperties::new();
        assert!(binding_matches(&ex, &bind, "k", &props));
        assert!(!binding_matches(&ex, &bind, "other", &props));
    }
}
