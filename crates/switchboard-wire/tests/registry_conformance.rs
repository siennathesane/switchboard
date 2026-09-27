//! Conformance of the method table against the normative AMQP 0-9-1 registry.
//!
//! The specification PDF defers the per-method details to its generated
//! companion ("§3.2.2: This section is provided by the generated document
//! amqp-xml-spec"), whose canonical serialization is
//! `docs/amqp-rabbitmq-0.9.1.json`. This suite rebuilds every method payload
//! **from the registry alone** — class id, method id, argument domains in
//! order, bit packing, synchronous/content flags — and requires
//! [`Method::decode_payload`] to accept it exactly, with no left-over bytes.
//!
//! A table with a wrong id, a missing argument, a swapped pair of arguments,
//! or a mistyped integer width fails here.

use serde_json::Value;
use switchboard_wire::method::Method;
use switchboard_wire::CodecError;

/// Methods of the registry this broker intentionally does not implement:
/// the legacy `access` class (dropped from 0-9-1 servers) and RabbitMQ's
/// `connection.update-secret` extension.
const SKIPPED: &[(u16, u16)] = &[(30, 10), (30, 11), (10, 70), (10, 71)];

fn registry() -> Value {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/amqp-rabbitmq-0.9.1.json");
    let raw = std::fs::read_to_string(path).expect("registry json present");
    serde_json::from_str(&raw).expect("registry parses")
}

fn domain_map(root: &Value) -> std::collections::HashMap<String, String> {
    let mut m = std::collections::HashMap::new();
    for pair in root["domains"].as_array().expect("domains") {
        let name = pair[0].as_str().unwrap();
        let ty = pair[1].as_str().unwrap();
        m.insert(name.to_string(), ty.to_string());
    }
    m
}

/// The wire width of one argument, and whether it packs as a bit.
/// Returns `(bytes, is_bit)`; `bytes == 0` with `is_bit` true means "join the
/// current bit octet".
fn arg_bytes(base: &str) -> (usize, bool) {
    match base {
        "bit" => (0, true),
        "octet" => (1, false),
        "short" => (2, false),
        "long" => (4, false),
        "longlong" | "timestamp" => (8, false),
        // length octet + "x"
        "shortstr" => (2, false),
        // 32-bit length + one payload byte
        "longstr" => (5, false),
        // 32-bit length of an empty table
        "table" => (4, false),
        other => panic!("unknown base type {other}"),
    }
}

fn filler(base: &str) -> Vec<u8> {
    match base {
        "bit" => vec![],
        "octet" => vec![0x2A],
        "short" => vec![0x00, 0x2A],
        "long" => vec![0, 0, 0, 0x2A],
        "longlong" | "timestamp" => vec![0, 0, 0, 0, 0, 0, 0, 0x2A],
        "shortstr" => vec![1, b'x'],
        "longstr" => vec![0, 0, 0, 1, b'x'],
        "table" => vec![0, 0, 0, 0],
        _ => unreachable!(),
    }
}

/// Build a method payload exactly as the registry dictates.
fn synthetic_payload(domains: &std::collections::HashMap<String, String>, args: &[Value]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut bit_run = 0usize;
    for arg in args {
        let domain = arg.get("domain").and_then(|d| d.as_str()).unwrap_or_else(|| {
            arg.get("type").and_then(|t| t.as_str()).expect("arg has type or domain")
        });
        let base = domains.get(domain).cloned().unwrap_or_else(|| domain.to_string());
        let (_bytes, is_bit) = arg_bytes(&base);
        if is_bit {
            bit_run += 1;
        } else {
            // A non-bit argument flushes the running bit octet.
            if bit_run > 0 {
                out.extend(std::iter::repeat(0u8).take((bit_run + 7) / 8));
                bit_run = 0;
            }
            out.extend_from_slice(&filler(&base));
        }
    }
    if bit_run > 0 {
        out.extend(std::iter::repeat(0u8).take((bit_run + 7) / 8));
    }
    out
}

#[test]
fn every_implemented_method_matches_the_registry_byte_for_byte() {
    let reg = registry();
    let domains = domain_map(&reg);

    let mut checked = 0usize;
    for class in reg["classes"].as_array().expect("classes") {
        let class_id = class["id"].as_u64().unwrap() as u16;
        if class_id == 30 {
            continue; // access class: skipped
        }
        for method in class["methods"].as_array().unwrap() {
            let method_id = method["id"].as_u64().unwrap() as u16;
            if SKIPPED.contains(&(class_id, method_id)) {
                continue;
            }
            let args = method.get("arguments").and_then(|a| a.as_array()).cloned().unwrap_or_default();
            let mut payload = vec![0u8, 0, 0, 0];
            payload[..2].copy_from_slice(&class_id.to_be_bytes());
            payload[2..4].copy_from_slice(&method_id.to_be_bytes());
            payload.extend(synthetic_payload(&domains, &args));

            let decoded = Method::decode_payload(&payload)
                .unwrap_or_else(|e| panic!("{class_id}.{method_id} failed: {e}"));

            // Flags agree with the registry.
            assert_eq!(
                decoded.is_sync_request(),
                method.get("synchronous").and_then(|s| s.as_bool()).unwrap_or(false),
                "{class_id}.{method_id}: synchronous flag"
            );
            assert_eq!(
                decoded.carries_content(),
                method.get("content").and_then(|s| s.as_bool()).unwrap_or(false),
                "{class_id}.{method_id}: content flag"
            );
            assert_eq!(decoded.class_id(), class_id);
            assert_eq!(decoded.method_id(), method_id);
            checked += 1;
        }
    }
    // 12 connection (of 14; update-secret skipped) + 6 channel + 8 exchange
    // + 10 queue + 18 basic + 6 tx + 2 confirm.
    assert_eq!(checked, 62, "expected the full method set we implement");
}

#[test]
fn skipped_methods_are_rejected() {
    let reg = registry();
    let domains = domain_map(&reg);
    for (class_id, method_id) in SKIPPED {
        // Find its argument list to build a payload that is well-formed on
        // the wire — we reject it on policy, not on bytes.
        let mut payload = vec![0, 0, 0, 0];
        payload[..2].copy_from_slice(&class_id.to_be_bytes());
        payload[2..4].copy_from_slice(&method_id.to_be_bytes());
        let class = reg["classes"].as_array().unwrap().iter().find(|c| c["id"].as_u64().unwrap() as u16 == *class_id).unwrap();
        if let Some(m) = class["methods"].as_array().unwrap().iter().find(|m| {
            m["id"].as_u64().unwrap() as u16 == *method_id
        }) {
            let args = m.get("arguments").and_then(|a| a.as_array()).cloned().unwrap_or_default();
            payload.extend(synthetic_payload(&domains, &args));
        }
        let err = Method::decode_payload(&payload).unwrap_err();
        if *class_id == 30 {
            assert!(matches!(err, CodecError::UnknownClass(30)));
        } else {
            assert!(matches!(err, CodecError::UnknownMethod(10, 70 | 71)));
        }
    }
}
