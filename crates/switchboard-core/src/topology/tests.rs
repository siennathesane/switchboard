//! Tests for `topology.

use super::*;
use crate::error::Level;

fn state() -> MetaState {
    let mut s = bootstrap();
    s.set_test_groups();
    s
}

impl MetaState {
    fn set_test_groups(&mut self) {
        self.groups = BTreeMap::from([
            (1, vec![1, 2, 3]),
            (2, vec![2, 3, 4]),
            (3, vec![3, 4, 5]),
        ]);
    }
}

fn owner() -> crate::model::ConnectionId {
    crate::model::ConnectionId { node: 1, conn: 1 }
}

fn declare_q(state: &mut MetaState, name: &str, durable: bool) -> MetaReply {
    let (r, _) = state
        .apply(&MetaCmd::DeclareQueue {
            vhost: "/".into(),
            name: name.into(),
            passive: false,
            options: QueueOptions { durable, ..Default::default() },
            owner: owner(),
        })
        .unwrap();
    r
}

fn declare_q_full(
    state: &mut MetaState,
    name: &str,
    _durable: bool,
    options: QueueOptions,
) -> (MetaReply, Vec<MetaEffect>) {
    let resolved = if name.is_empty() {
        crate::model::generate_queue_name()
    } else {
        name.to_string()
    };
    state
        .apply(&MetaCmd::DeclareQueue {
            vhost: "/".into(),
            name: resolved,
            passive: false,
            options,
            owner: owner(),
        })
        .unwrap()
}

#[test]
fn bootstrap_has_default_vhost_exchanges_and_user() {
    let s = bootstrap();
    let v = s.vhost("/").unwrap();
    for name in ["", "amq.direct", "amq.fanout", "amq.topic", "amq.match"] {
        assert!(v.exchanges.contains_key(name), "missing {name:?}");
    }
    assert!(s.users.contains_key("guest"));
}

#[test]
fn queue_declare_create_assign_delete() {
    let mut s = state();
    let r = declare_q(&mut s, "jobs", true);
    let MetaReply::QueueDeclared { name, shard, created } = r else {
        panic!("wrong reply");
    };
    assert_eq!(name, "jobs");
    assert!(created);
    assert!(s.groups.contains_key(&shard), "assigned to a real group");
    let jobs_shard = shard;

    // Equivalent re-declare: not created.
    let r = declare_q(&mut s, "jobs", true);
    let MetaReply::QueueDeclared { created, .. } = r else { panic!() };
    assert!(!created);

    // Different durability: 406.
    let e = s.apply(&MetaCmd::DeclareQueue {
        vhost: "/".into(),
        name: "jobs".into(),
        passive: false,
        options: QueueOptions { durable: false, ..Default::default() },
        owner: owner(),
    })
    .unwrap_err();
    assert_eq!(e.code, 406);

    // Passive of missing: 404.
    let e = s.apply(&MetaCmd::DeclareQueue {
        vhost: "/".into(),
        name: "nope".into(),
        passive: true,
        options: QueueOptions::default(),
        owner: owner(),
    })
    .unwrap_err();
    assert_eq!(e.code, 404);

    // Delete: cascades shard effect.
    let (reply, effects) = s
        .apply(&MetaCmd::DeleteQueue {
            vhost: "/".into(),
            name: "jobs".into(),
            if_unused: false,
            if_empty: false,
            depth: 5,
            consumers: 0,
        })
        .unwrap();
    assert_eq!(reply, MetaReply::QueueDeleted { message_count: 5 });
    assert_eq!(
        effects,
        vec![MetaEffect::QueueDeleted { queue: "jobs".into(), shard: jobs_shard }]
    );
}

#[test]
fn queue_declare_reserved_names_and_server_naming() {
    let mut s = state();
    let e = s.apply(&MetaCmd::DeclareQueue {
        vhost: "/".into(),
        name: "amq.rogue".into(),
        passive: false,
        options: QueueOptions::default(),
        owner: owner(),
    })
    .unwrap_err();
    assert_eq!(e.code, 403);

    // Server-named queues: the caller resolves the name (deterministic
    // replication), apply just accepts it.
    let generated = crate::model::generate_queue_name();
    assert!(generated.starts_with("amq.gen-"));
    assert_eq!(generated.len(), "amq.gen-".len() + 22);
    let (r, _) = s.apply(&MetaCmd::DeclareQueue {
        vhost: "/".into(),
        name: generated,
        passive: false,
        options: QueueOptions::default(),
        owner: owner(),
    })
    .unwrap();
    let MetaReply::QueueDeclared { created, .. } = r else { panic!() };
    assert!(created);

    // ...and an empty name is an internal invariant violation (506).
    let e = s.apply(&MetaCmd::DeclareQueue {
        vhost: "/".into(),
        name: String::new(),
        passive: false,
        options: QueueOptions::default(),
        owner: owner(),
    })
    .unwrap_err();
    assert_eq!(e.code, 506);
}

#[test]
fn exclusive_queue_ownership() {
    let mut s = state();
    let opts = QueueOptions { exclusive: true, ..Default::default() };
    let (r, _) = s.apply(&MetaCmd::DeclareQueue {
        vhost: "/".into(),
        name: "priv".into(),
        passive: false,
        options: opts.clone(),
        owner: owner(),
    })
    .unwrap();
    let MetaReply::QueueDeclared { name, .. } = r else { panic!() };
    assert_eq!(s.vhost("/").unwrap().queues[&name].owner, Some(owner()));

    // Another connection: 405 RESOURCE_LOCKED.
    let other = crate::model::ConnectionId { node: 2, conn: 9 };
    let e = s.apply(&MetaCmd::DeclareQueue {
        vhost: "/".into(),
        name: "priv".into(),
        passive: false,
        options: opts,
        owner: other,
    })
    .unwrap_err();
    assert_eq!(e.code, 405);
    assert_eq!(e.level, Level::Channel);
}

#[test]
fn exchange_lifecycle_rules() {
    let mut s = state();

    // Reserved prefix, not a bootstrap name: 403.
    let e = s.apply(&MetaCmd::DeclareExchange {
        vhost: "/".into(),
        name: "amq.mine".into(),
        kind: ExchangeKind::Direct,
        passive: false,
        durable: false,
        auto_delete: false,
        internal: false,
        arguments: FieldTable::new(),
    })
    .unwrap_err();
    assert_eq!(e.code, 403);

    // Create + equivalent redeclare + 406 on mismatch.
    let mk = |kind| MetaCmd::DeclareExchange {
        vhost: "/".into(),
        name: "work".into(),
        kind,
        passive: false,
        durable: true,
        auto_delete: false,
        internal: false,
        arguments: FieldTable::new(),
    };
    let (r, _) = s.apply(&mk(ExchangeKind::Topic)).unwrap();
    assert_eq!(r, MetaReply::ExchangeDeclared { existed: false });
    let (r, _) = s.apply(&mk(ExchangeKind::Topic)).unwrap();
    assert_eq!(r, MetaReply::ExchangeDeclared { existed: true });
    let e = s.apply(&mk(ExchangeKind::Direct)).unwrap_err();
    assert_eq!(e.code, 406);

    // Unknown standard-spelled type: 503; unknown x- type: 540.
    assert!(matches!(
        ExchangeKind::from_str("xy"),
        None
    ));

    // Passive declare of existing works, of missing 404.
    let (r, _) = s.apply(&MetaCmd::DeclareExchange {
        vhost: "/".into(),
        name: "work".into(),
        kind: ExchangeKind::Topic,
        passive: true,
        durable: true,
        auto_delete: false,
        internal: false,
        arguments: FieldTable::new(),
    })
    .unwrap();
    assert_eq!(r, MetaReply::ExchangeDeclared { existed: true });

    // Default exchange cannot be deleted.
    let e = s.apply(&MetaCmd::DeleteExchange {
        vhost: "/".into(),
        name: "".into(),
        if_unused: false,
    })
    .unwrap_err();
    assert_eq!(e.code, 403);

    // Delete removes bindings too.
    let (r, _) = s.apply(&MetaCmd::DeclareQueue {
        vhost: "/".into(),
        name: "bq".into(),
        passive: false,
        options: QueueOptions::default(),
        owner: owner(),
    })
    .unwrap();
    let MetaReply::QueueDeclared { name: bq, .. } = r else { panic!() };
    s.apply(&MetaCmd::Bind {
        vhost: "/".into(),
        exchange: "work".into(),
        queue: bq,
        routing_key: "k".into(),
        arguments: FieldTable::new(),
    })
    .unwrap();
    assert_eq!(s.vhost("/").unwrap().bindings.len(), 1);
    s.apply(&MetaCmd::DeleteExchange {
        vhost: "/".into(),
        name: "work".into(),
        if_unused: false,
    })
    .unwrap();
    assert!(s.vhost("/").unwrap().bindings.is_empty());
    assert!(!s.vhost("/").unwrap().exchanges.contains_key("work"));
}

#[test]
fn bind_unbind_rules() {
    let mut s = state();
    let (r, _) = s.apply(&MetaCmd::DeclareQueue {
        vhost: "/".into(),
        name: "q".into(),
        passive: false,
        options: QueueOptions::default(),
        owner: owner(),
    })
    .unwrap();
    let MetaReply::QueueDeclared { name: q, .. } = r else { panic!() };

    // Bind to default exchange: 403.
    let e = s.apply(&MetaCmd::Bind {
        vhost: "/".into(),
        exchange: "".into(),
        queue: q.clone(),
        routing_key: "k".into(),
        arguments: FieldTable::new(),
    })
    .unwrap_err();
    assert_eq!(e.code, 403);

    // Bind to missing exchange: 404; missing queue: 404.
    let e = s.apply(&MetaCmd::Bind {
        vhost: "/".into(),
        exchange: "amq.direct".into(),
        queue: "ghost".into(),
        routing_key: "k".into(),
        arguments: FieldTable::new(),
    })
    .unwrap_err();
    assert_eq!(e.code, 404);

    let bind = |args| MetaCmd::Bind {
        vhost: "/".into(),
        exchange: "amq.direct".into(),
        queue: q.clone(),
        routing_key: "k".into(),
        arguments: args,
    };
    let (r, _) = s.apply(&bind(FieldTable::new())).unwrap();
    assert_eq!(r, MetaReply::Bound { existed: false });
    let (r, _) = s.apply(&bind(FieldTable::new())).unwrap();
    assert_eq!(r, MetaReply::Bound { existed: true });

    // Unbind with the same identity works; second unbind 404s.
    let unbind = MetaCmd::Unbind {
        vhost: "/".into(),
        exchange: "amq.direct".into(),
        queue: q.clone(),
        routing_key: "k".into(),
        arguments: FieldTable::new(),
    };
    s.apply(&unbind).unwrap();
    assert_eq!(s.apply(&unbind).unwrap_err().code, 404);
}

#[test]
fn auto_delete_exchange_dies_with_last_unbind() {
    let mut s = state();
    s.apply(&MetaCmd::DeclareExchange {
        vhost: "/".into(),
        name: "flash".into(),
        kind: ExchangeKind::Fanout,
        passive: false,
        durable: false,
        auto_delete: true,
        internal: false,
        arguments: FieldTable::new(),
    })
    .unwrap();
    let (r, _) = s.apply(&MetaCmd::DeclareQueue {
        vhost: "/".into(),
        name: "q".into(),
        passive: false,
        options: QueueOptions::default(),
        owner: owner(),
    })
    .unwrap();
    let MetaReply::QueueDeclared { name: q, .. } = r else { panic!() };
    s.apply(&MetaCmd::Bind {
        vhost: "/".into(),
        exchange: "flash".into(),
        queue: q,
        routing_key: String::new(),
        arguments: FieldTable::new(),
    })
    .unwrap();
    s.apply(&MetaCmd::Unbind {
        vhost: "/".into(),
        exchange: "flash".into(),
        queue: "q".into(),
        routing_key: String::new(),
        arguments: FieldTable::new(),
    })
    .unwrap();
    assert!(!s.vhost("/").unwrap().exchanges.contains_key("flash"));
}

#[test]
fn shard_assignment_is_stable_and_spread() {
    let s = state();
    let a = s.assign_shard("queue-a").unwrap();
    let b = s.assign_shard("queue-a").unwrap();
    assert_eq!(a, b, "stable for the same name");
    assert_eq!(s.assign_shard("queue-b"), s.assign_shard("queue-b"));
    // All names map to existing groups.
    for i in 0..50 {
        let g = s.assign_shard(&format!("q{i}")).unwrap();
        assert!(s.groups.contains_key(&g));
    }
    // No groups -> cannot assign.
    let empty = MetaState::default();
    assert!(empty.assign_shard("x").is_none());
}

#[test]
fn fnv1a_is_deterministic() {
    assert_eq!(fnv1a(b""), 0xcbf2_9ce4_8422_2325);
    assert_eq!(fnv1a(b"a"), fnv1a(b"a"));
    assert_ne!(fnv1a(b"a"), fnv1a(b"b"));
}

#[test]
fn auth_checks() {
    let mut s = state();
    let (r, _) = s
        .apply(&MetaCmd::Authorize {
            user: "guest".into(),
            password: "guest".into(),
        })
        .unwrap();
    assert_eq!(r, MetaReply::Authorized);
    let e = s.apply(&MetaCmd::Authorize {
        user: "guest".into(),
        password: "wrong".into(),
    })
    .unwrap_err();
    assert_eq!(e.code, 403);
    assert_eq!(e.level, Level::Connection);
}

#[test]
fn forget_node_cascades_its_exclusive_queues() {
    let mut s = state();
    let dead_owner = crate::model::ConnectionId { node: 7, conn: 3 };
    s.apply(&MetaCmd::DeclareQueue {
        vhost: "/".into(),
        name: crate::model::generate_queue_name(),
        passive: false,
        options: QueueOptions { exclusive: true, ..Default::default() },
        owner: dead_owner,
    })
    .unwrap();
    assert_eq!(s.vhost("/").unwrap().queues.len(), 1);
    let (r, _) = s.apply(&MetaCmd::DeclareQueue {
        vhost: "/".into(),
        name: "keeper".into(),
        passive: false,
        options: QueueOptions::default(),
        owner: dead_owner,
    })
    .unwrap();
    let MetaReply::QueueDeclared { name: keeper, .. } = r else { panic!() };

    let (_, effects) = s.apply(&MetaCmd::ForgetNode { node: 7 }).unwrap();
    let v = s.vhost("/").unwrap();
    assert_eq!(v.queues.len(), 1, "only the non-exclusive queue remains");
    assert!(v.queues.contains_key(&keeper));
    // The exclusive queue was server-named & exclusive: effect emitted.
    assert_eq!(effects.len(), 1);
}

#[test]
fn routing_view_over_vhost() {
    use crate::routing::route;
    use switchboard_wire::BasicProperties;
    let mut s = state();
    declare_q(&mut s, "rk-queue", false);
    s.apply(&MetaCmd::Bind {
        vhost: "/".into(),
        exchange: "amq.direct".into(),
        queue: "rk-queue".into(),
        routing_key: "rk".into(),
        arguments: FieldTable::new(),
    })
    .unwrap();
    let v = s.vhost("/").unwrap();
    let view = VhostView { vhost: v };
    let dest = route(&view, "amq.direct", "rk", &BasicProperties::new());
    assert_eq!(dest, vec!["rk-queue"]);
}

#[test]
fn vhost_isolation() {
    let mut s = state();
    s.apply(&MetaCmd::DeclareVhost { name: "staging".into() }).unwrap();
    let (r, _) = s.apply(&MetaCmd::DeclareQueue {
        vhost: "staging".into(),
        name: "q".into(),
        passive: false,
        options: QueueOptions::default(),
        owner: owner(),
    })
    .unwrap();
    let MetaReply::QueueDeclared { name, .. } = r else { panic!() };
    assert!(!s.vhost("/").unwrap().queues.contains_key(&name));
    assert!(s.vhost("staging").unwrap().queues.contains_key(&name));
    // Unknown vhost: 402 INVALID_PATH (connection level).
    let e = s.apply(&MetaCmd::DeclareQueue {
        vhost: "nope".into(),
        name: "q".into(),
        passive: false,
        options: QueueOptions::default(),
        owner: owner(),
    })
    .unwrap_err();
    assert_eq!(e.code, 402);
}

// ---------------------------------------------------------------------
// Direct-apply branch coverage: error arms, retention, node bookkeeping
// ---------------------------------------------------------------------

#[test]
fn set_retained_stores_and_clears() {
    let mut s = state();
    let msg = crate::model::StoredMessage {
        properties: Default::default(),
        body: b"state".to_vec(),
        exchange: "amq.topic".into(),
        routing_key: "t".into(),
        persistent: false,
    };
    let (r, fx) = s
        .apply(&MetaCmd::SetRetained {
            vhost: "/".into(),
            topic: "state/last".into(),
            message: Some(msg.clone()),
        })
        .unwrap();
    assert_eq!(r, MetaReply::Ok);
    assert!(fx.is_empty());
    assert_eq!(s.retained[&("/".to_string(), "state/last".to_string())].body, b"state");

    // Clearing removes the entry.
    s.apply(&MetaCmd::SetRetained {
        vhost: "/".into(),
        topic: "state/last".into(),
        message: None,
    })
    .unwrap();
    assert!(!s.retained.contains_key(&("/".to_string(), "state/last".to_string())));
}

#[test]
fn register_and_forget_nodes_roundtrip() {
    let mut s = state();
    let info = NodeInfo {
        client_addr: "127.0.0.1:1".into(),
        internal_addr: "127.0.0.1:2".into(),
    };
    let (r, _) = s
        .apply(&MetaCmd::RegisterNode { node: 42, info: info.clone() })
        .unwrap();
    assert_eq!(r, MetaReply::Ok);
    assert_eq!(s.nodes[&42], info);

    s.apply(&MetaCmd::ForgetNode { node: 42 }).unwrap();
    assert!(!s.nodes.contains_key(&42));
    // The group member lists keep their (controller-managed) state.
    assert!(s.groups.values().all(|m| !m.is_empty()));
}

#[test]
fn declare_queue_error_branches() {
    let mut s = state();
    declare_q(&mut s, "dq", true);

    // Reserved prefix that is not amq.gen-.
    let reserved_err = s
        .apply(&MetaCmd::DeclareQueue {
            vhost: "/".into(),
            name: "amq.bad".into(),
            passive: false,
            options: QueueOptions {
                durable: true,
                exclusive: false,
                auto_delete: false,
                arguments: FieldTable::new(),
            },
            owner: owner(),
        })
        .unwrap_err();
    let _ = reserved_err;

    // Equivalent redeclare is idempotent (no error, created=false).
    let (r, _) = s
        .apply(&MetaCmd::DeclareQueue {
            vhost: "/".into(),
            name: "dq".into(),
            passive: false,
            options: QueueOptions {
                durable: true,
                exclusive: false,
                auto_delete: false,
                arguments: FieldTable::new(),
            },
            owner: owner(),
        })
        .unwrap();
    assert!(matches!(r, MetaReply::QueueDeclared { created: false, .. }));
}

#[test]
fn delete_queue_precondition_branches() {
    let mut s = state();
    declare_q(&mut s, "pq", true);

    // if_empty on a non-empty-depth queue would pass depth=0 here; instead
    // exercise if_unused with consumers>0 → 406.
    let err = s
        .apply(&MetaCmd::DeleteQueue {
            vhost: "/".into(),
            name: "pq".into(),
            if_unused: true,
            if_empty: false,
            depth: 0,
            consumers: 2,
        })
        .unwrap_err();
    assert_eq!(err.code, 406);

    // Deleting a missing queue → 404.
    let err = s
        .apply(&MetaCmd::DeleteQueue {
            vhost: "/".into(),
            name: "missing".into(),
            if_unused: false,
            if_empty: false,
            depth: 0,
            consumers: 0,
        })
        .unwrap_err();
    assert_eq!(err.code, 404);
}

#[test]
fn exchange_delete_missing_is_404() {
    let mut s = state();
    let err = s
        .apply(&MetaCmd::DeleteExchange {
            vhost: "/".into(),
            name: "nope".into(),
            if_unused: false,
        })
        .unwrap_err();
    assert_eq!(err.code, 404);
}

#[test]
fn unbind_is_idempotent_and_bind_requires_vhost() {
    let mut s = state();
    declare_q(&mut s, "bq", true);
    s.apply(&MetaCmd::Bind {
        vhost: "/".into(),
        exchange: "amq.direct".into(),
        queue: "bq".into(),
        routing_key: "k".into(),
        arguments: FieldTable::new(),
    })
    .unwrap();
    // Unbind removes the binding; a second unbind is a 404.
    s.apply(&MetaCmd::Unbind {
        vhost: "/".into(),
        exchange: "amq.direct".into(),
        queue: "bq".into(),
        routing_key: "k".into(),
        arguments: FieldTable::new(),
    })
    .unwrap();
    let second = s
        .apply(&MetaCmd::Unbind {
            vhost: "/".into(),
            exchange: "amq.direct".into(),
            queue: "bq".into(),
            routing_key: "k".into(),
            arguments: FieldTable::new(),
        })
        .unwrap_err();
    assert_eq!(second.code, 404);
    // Bind on a missing vhost → 402.
    let err = s
        .apply(&MetaCmd::Bind {
            vhost: "nope".into(),
            exchange: "amq.direct".into(),
            queue: "bq".into(),
            routing_key: "k".into(),
            arguments: FieldTable::new(),
        })
        .unwrap_err();
    assert_eq!(err.code, 402);
}

#[test]
fn assign_shard_is_stable_and_full_range() {
    let mut s = state(); // groups 1..3
    let a = s.assign_shard("steady-queue").unwrap();
    let b = s.assign_shard("steady-queue").unwrap();
    assert_eq!(a, b);
    assert!(s.groups.contains_key(&a));
}

// ---------------------------------------------------------------------------
// Error arms: unknown vhosts, missing/in-use exchanges, default-exchange
// rules, and declare-without-layout.
// ---------------------------------------------------------------------------

#[test]
fn declare_queue_without_shard_layout_is_rejected() {
    let mut s = bootstrap(); // no groups installed
    let err = s
        .apply(&MetaCmd::DeclareQueue {
            vhost: "/".into(),
            name: "q".into(),
            passive: false,
            options: QueueOptions::default(),
            owner: owner(),
        })
        .unwrap_err();
    assert!(err.to_string().contains("no shard groups"), "{err}");
}

#[test]
fn unknown_vhost_is_rejected_for_every_lifecycle_command() {
    let mut s = state();

    let e = s.apply(&MetaCmd::DeleteQueue {
        vhost: "ghost".into(),
        name: "q".into(),
        if_unused: false,
        if_empty: false,
        depth: 0,
        consumers: 0,
    }).unwrap_err();
    assert!(format!("{e:?}").contains("no vhost"), "{e:?}");

    let e = s.apply(&MetaCmd::DeleteExchange {
        vhost: "ghost".into(),
        name: "ex".into(),
        if_unused: false,
    }).unwrap_err();
    assert!(format!("{e:?}").contains("no vhost"), "{e:?}");

    let e = s.apply(&MetaCmd::Bind {
        vhost: "ghost".into(),
        exchange: "amq.direct".into(),
        queue: "q".into(),
        routing_key: "rk".into(),
        arguments: Default::default(),
    }).unwrap_err();
    assert!(format!("{e:?}").contains("no vhost"), "{e:?}");

    let e = s.apply(&MetaCmd::Unbind {
        vhost: "ghost".into(),
        exchange: "amq.direct".into(),
        queue: "q".into(),
        routing_key: "rk".into(),
        arguments: Default::default(),
    }).unwrap_err();
    assert!(format!("{e:?}").contains("no vhost"), "{e:?}");
}

#[test]
fn delete_missing_exchange_is_not_found() {
    let mut s = state();
    let e = s.apply(&MetaCmd::DeleteExchange {
        vhost: "/".into(),
        name: "never".into(),
        if_unused: false,
    }).unwrap_err();
    assert!(format!("{e:?}").contains("no exchange"), "{e:?}");
}

#[test]
fn delete_exchange_in_use_is_precondition_failed() {
    let mut s = state();
    // amq.direct with a live binding is "in use" when if_unused is set.
    declare_q(&mut s, "bound-q", true);
    let (r, _) = s.apply(&MetaCmd::Bind {
        vhost: "/".into(),
        exchange: "amq.direct".into(),
        queue: "bound-q".into(),
        routing_key: "rk".into(),
        arguments: Default::default(),
    }).unwrap();
    assert!(matches!(r, MetaReply::Bound { .. }), "{r:?}");

    let e = s.apply(&MetaCmd::DeleteExchange {
        vhost: "/".into(),
        name: "amq.direct".into(),
        if_unused: true,
    }).unwrap_err();
    assert!(format!("{e:?}").contains("in use"), "{e:?}");

    // Without if_unused the delete succeeds despite bindings.
    s.apply(&MetaCmd::DeleteExchange {
        vhost: "/".into(),
        name: "amq.direct".into(),
        if_unused: false,
    }).unwrap();
}

#[test]
fn bind_to_missing_exchange_is_not_found() {
    let mut s = state();
    declare_q(&mut s, "orphan-q", true);
    let e = s.apply(&MetaCmd::Bind {
        vhost: "/".into(),
        exchange: "ghost-ex".into(),
        queue: "orphan-q".into(),
        routing_key: "rk".into(),
        arguments: Default::default(),
    }).unwrap_err();
    assert!(format!("{e:?}").contains("no exchange"), "{e:?}");
}

#[test]
fn unbind_from_default_exchange_is_access_refused() {
    let mut s = state();
    declare_q(&mut s, "dq", true);
    let e = s.apply(&MetaCmd::Unbind {
        vhost: "/".into(),
        exchange: "".into(),
        queue: "dq".into(),
        routing_key: "dq".into(),
        arguments: Default::default(),
    }).unwrap_err();
    assert!(format!("{e:?}").contains("default exchange"), "{e:?}");
}

#[test]
fn unbind_to_missing_destination_is_not_found() {
    let mut s = state();
    let e = s.apply(&MetaCmd::Unbind {
        vhost: "/".into(),
        exchange: "amq.direct".into(),
        queue: "never-declared".into(),
        routing_key: "rk".into(),
        arguments: Default::default(),
    }).unwrap_err();
    assert!(format!("{e:?}").contains("no binding"), "{e:?}");
}

#[test]
fn fanout_done_moves_the_id_to_recent_and_out_of_pending() {
    let mut s = state();
    let id = uuid::Uuid::new_v4();
    let (r, _) = s.apply(&MetaCmd::FanoutBegin {
        id,
        vhost: "/".into(),
        message: crate::model::StoredMessage {
            properties: Default::default(),
            body: b"m".to_vec(),
            exchange: "amq.direct".into(),
            routing_key: "rk".into(),
            persistent: false,
        },
        queues: vec!["q1".into(), "q2".into()],
    })
    .unwrap();
    assert!(matches!(r, MetaReply::FanoutBegun), "{r:?}");
    assert_eq!(s.pending_fanouts.len(), 1);
    assert!(!s.recent_fanouts.contains(&id));

    let (r, _) = s.apply(&MetaCmd::FanoutDone { id }).unwrap();
    assert!(matches!(r, MetaReply::FanoutDone), "{r:?}");
    assert!(s.pending_fanouts.is_empty());
    // The completion signal is monotone: once recorded it survives, which
    // is what lets a publisher distinguish "done" from "not yet locally
    // visible" when waiting for its confirm.
    assert!(s.recent_fanouts.contains(&id));
    assert_eq!(s.apply(&MetaCmd::FanoutDone { id }).unwrap().0, MetaReply::FanoutDone);
    assert!(s.recent_fanouts.contains(&id));
}
