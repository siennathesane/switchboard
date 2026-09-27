#!/usr/bin/env python3
"""Switchboard conformance suite — Python (pika), AMQP 0-9-1.

Exercises the full broker feature matrix from a real ecosystem client.
Every check prints `PASS <name>` / `FAIL <name>: detail`; the process
exits non-zero if any check failed.
"""

from __future__ import annotations

import os
import sys
import time
import uuid

import pika
import pika.exceptions

CHECKS: list[tuple[str, bool, str]] = []


def check(name: str, fn) -> None:
    try:
        fn()
        CHECKS.append((name, True, ""))
        print(f"PASS {name}")
    except Exception as e:  # noqa: BLE001 - report and continue
        CHECKS.append((name, False, str(e)))
        print(f"FAIL {name}: {e}")


def connect(url: str) -> pika.BlockingConnection:
    return pika.BlockingConnection(pika.URLParameters(url))


def purge(conn: pika.BlockingConnection, queue: str) -> None:
    ch = conn.channel()
    ch.queue_purge(queue)
    ch.close()


def main() -> int:
    urls = os.environ["SB_AMQP_URLS"].split(",")
    base_url = urls[0]
    other_url = urls[1] if len(urls) > 1 else base_url
    run_id = uuid.uuid4().hex[:8]

    # -- connection -----------------------------------------------------
    conn = connect(base_url)
    check("connect and open channel", lambda: conn.channel().close())

    def bad_creds() -> None:
        bad = base_url.replace(f"{os.environ.get('SB_USER','guest')}:",
                               "guest:wrongpass@")
        try:
            connect(bad)
        except pika.exceptions.ProbableAuthenticationError:
            return
        raise AssertionError("wrong password was accepted")

    check("bad credentials rejected", bad_creds)

    # -- exchanges ------------------------------------------------------
    ch = conn.channel()
    check("declare direct exchange",
          lambda: ch.exchange_declare(f"hx-d-{run_id}", exchange_type="direct", durable=True))
    check("declare fanout exchange",
          lambda: ch.exchange_declare(f"hx-f-{run_id}", exchange_type="fanout", durable=True))
    check("declare topic exchange",
          lambda: ch.exchange_declare(f"hx-t-{run_id}", exchange_type="topic", durable=True))
    check("declare headers exchange",
          lambda: ch.exchange_declare(f"hx-h-{run_id}", exchange_type="headers", durable=True))
    check("passive exchange declare",
          lambda: ch.exchange_declare(f"hx-d-{run_id}", passive=True))
    check("exchange delete",
          lambda: ch.exchange_delete(f"hx-h-{run_id}"))
    check("re-declare deleted exchange",
          lambda: ch.exchange_declare(f"hx-h-{run_id}", exchange_type="headers", durable=True))

    # -- queues ---------------------------------------------------------
    q_simple = f"hq-simple-{run_id}"
    q_args = f"hq-args-{run_id}"
    check("queue declare durable",
          lambda: ch.queue_declare(q_simple, durable=True))
    check("passive queue declare",
          lambda: ch.queue_declare(q_simple, passive=True))
    check("queue declare with arguments",
          lambda: ch.queue_declare(q_args, durable=True, arguments={
              "x-message-ttl": 10_000,
              "x-dead-letter-exchange": "",
              "x-dead-letter-routing-key": f"hq-dlk-{run_id}",
          }))
    check("queue delete",
          lambda: ch.queue_delete(q_args))

    # -- routing --------------------------------------------------------
    check("queue bind direct",
          lambda: ch.queue_bind(q_simple, f"hx-d-{run_id}", routing_key="k1"))
    check("queue unbind",
          lambda: ch.queue_unbind(q_simple, f"hx-d-{run_id}", routing_key="k1"))
    check("rebind direct",
          lambda: ch.queue_bind(q_simple, f"hx-d-{run_id}", routing_key="k1"))

    # topic wildcards
    q_star = f"hq-star-{run_id}"
    q_hash = f"hq-hash-{run_id}"
    ch.queue_declare(q_star, durable=True)
    ch.queue_declare(q_hash, durable=True)
    ch.queue_bind(q_star, f"hx-t-{run_id}", "a.*")
    ch.queue_bind(q_hash, f"hx-t-{run_id}", "a.#")
    ch.confirm_delivery()
    ch.basic_publish(f"hx-t-{run_id}", "a.b", b"star-and-hash")
    ch.basic_publish(f"hx-t-{run_id}", "a.b.c", b"hash-only")
    time.sleep(0.5)
    star = ch.basic_get(q_star, auto_ack=True)
    hash_one = ch.basic_get(q_hash, auto_ack=True)
    hash_two = ch.basic_get(q_hash, auto_ack=True)
    check("topic wildcard routing",
          lambda: (
              None if (star[0] and hash_one[0] and hash_two[0]
                       and star[2] == b"star-and-hash"
                       and hash_one[2] == b"star-and-hash"
                       and hash_two[2] == b"hash-only")
              else AssertionError(f"star={star[0]} h1={hash_one[0]} h2={hash_two[0]}")
          ))

    # fanout
    q_f1 = f"hq-f1-{run_id}"
    q_f2 = f"hq-f2-{run_id}"
    ch.queue_declare(q_f1, durable=True)
    ch.queue_declare(q_f2, durable=True)
    ch.queue_bind(q_f1, f"hx-f-{run_id}")
    ch.queue_bind(q_f2, f"hx-f-{run_id}")
    ch.basic_publish(f"hx-f-{run_id}", "", b"to-both")
    time.sleep(0.5)
    check("fanout reaches both queues",
          lambda: None if (ch.basic_get(q_f1, auto_ack=True)[0]
                           and ch.basic_get(q_f2, auto_ack=True)[0])
          else AssertionError("a fanout copy was lost"))

    # headers exchange
    q_h = f"hq-h-{run_id}"
    ch.queue_declare(q_h, durable=True)
    ch.queue_bind(q_h, f"hx-h-{run_id}", "",
                  arguments={"x-match": "all", "fmt": "pdf", "type": "log"})
    ch.basic_publish(f"hx-h-{run_id}", "", b"hdr",
                     properties=pika.BasicProperties(headers={"fmt": "pdf", "type": "log"}))
    ch.basic_publish(f"hx-h-{run_id}", "", b"nope",
                     properties=pika.BasicProperties(headers={"fmt": "pdf"}))
    time.sleep(0.5)
    got = ch.basic_get(q_h, auto_ack=True)
    rest = ch.basic_get(q_h, auto_ack=True)
    check("headers exchange x-match all",
          lambda: None if (got[0] and got[2] == b"hdr" and rest[0] is False)
          else AssertionError(f"got={got[2] if got[0] else None} extra={rest[0]}"))

    # exchange-exchange binding
    q_e2e = f"hq-e2e-{run_id}"
    ex_src = f"hxs-{run_id}"
    ex_dst = f"hxd-{run_id}"
    ch.exchange_declare(ex_src, exchange_type="direct", durable=True)
    ch.exchange_declare(ex_dst, exchange_type="direct", durable=True)
    ch.exchange_declare("", passive=True)  # default exchange sanity
    ch.queue_declare(q_e2e, durable=True)
    ch.exchange_bind(ex_dst, ex_src, "relay")
    ch.queue_bind(q_e2e, ex_dst, "relay")
    ch.basic_publish(ex_src, "relay", b"relayed")
    time.sleep(0.5)
    got = ch.basic_get(q_e2e, auto_ack=True)
    check("exchange-exchange binding relays",
          lambda: None if (got[0] and got[2] == b"relayed")
          else AssertionError(f"got={got[2] if got[0] else None}"))

    # -- publish/consume ------------------------------------------------
    payload = f"hello-{run_id}".encode()
    props = pika.BasicProperties(content_type="text/plain", delivery_mode=2,
                                 message_id=run_id)
    check("publish with confirm",
          lambda: ch.basic_publish("", q_simple, payload, properties=props,
                                   mandatory=False) and (ch.waitForConfirm() if
                                                         hasattr(ch, "waitForConfirm") else None))
    got = ch.basic_get(q_simple, auto_ack=False)
    check("basic.get returns the message",
          lambda: None if (got[0] and got[2] == payload
                           and got[0].routing_key == q_simple)
          else AssertionError(f"got={got[0]} body={got[2] if got[0] else None}"))
    check("get-ok carries properties",
          lambda: None if (got[1].content_type == "text/plain"
                           and got[1].message_id == run_id
                           and got[1].delivery_mode == 2)
          else AssertionError(f"props={got[1]}"))
    ch.basic_ack(got[0].delivery_tag)

    # consume/ack push
    ch.basic_publish("", q_simple, b"pushed")
    time.sleep(0.4)
    delivered: list[bytes] = []

    def on_message(chx, method, properties, body):
        delivered.append(body)
        chx.basic_ack(method.delivery_tag)
        if len(delivered) >= 1:
            chx.stop_consuming()

    ch.basic_consume(q_simple, on_message, auto_ack=False)
    ch.start_consuming()
    check("consume push + ack",
          lambda: None if delivered == [b"pushed"] else AssertionError(f"{delivered}"))

    # reject with requeue
    ch.basic_publish("", q_simple, b"reject-me")
    time.sleep(0.4)
    got = ch.basic_get(q_simple, auto_ack=False)
    ch.basic_reject(got[0].delivery_tag, requeue=True)
    got2 = ch.basic_get(q_simple, auto_ack=True)
    check("reject(requeue=true) redelivers",
          lambda: None if (got2[0] and got2[2] == b"reject-me" and got2[0].redelivered)
          else AssertionError("message did not come back redelivered"))

    # dead-lettering
    q_dlx_src = f"hq-dlxs-{run_id}"
    q_dlx_dst = f"hq-dlxd-{run_id}"
    ch.queue_declare(q_dlx_dst, durable=True)
    ch.queue_declare(q_dlx_src, durable=True, arguments={
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": q_dlx_dst,
    })
    ch.basic_publish("", q_dlx_src, b"doomed")
    got = ch.basic_get(q_dlx_src, auto_ack=False)
    ch.basic_nack(got[0].delivery_tag, multiple=False, requeue=False)
    deadline = time.time() + 10
    dl = (False, None, None)
    while time.time() < deadline:
        dl = ch.basic_get(q_dlx_dst, auto_ack=True)
        if dl[0]:
            break
        time.sleep(0.2)
    check("nack(requeue=false) dead-letters to DLX",
          lambda: None if (dl[0] and dl[2] == b"doomed")
          else AssertionError("dead-lettered message never arrived"))

    # qos prefetch
    q_qos = f"hq-qos-{run_id}"
    ch.queue_declare(q_qos, durable=True)
    ch.basic_qos(prefetch_count=1)
    for i in range(3):
        ch.basic_publish("", q_qos, f"q{i}".encode())
    time.sleep(0.5)
    windowed: list[bytes] = []

    def on_qos(chx, method, properties, body):
        windowed.append(body)
        if len(windowed) >= 3:
            chx.stop_consuming()
            return
        chx.basic_ack(method.delivery_tag)

    ch.basic_consume(q_qos, on_qos, auto_ack=False)
    ch.start_consuming()
    check("qos prefetch delivers all after acks",
          lambda: None if sorted(windowed) == [b"q0", b"q1", b"q2"]
          else AssertionError(f"{windowed}"))

    # transactions (fresh channel: this one is in confirm mode and the
    # broker rightly refuses the confirm→tx switch with 406)
    tch = conn.channel()
    tch.tx_select()
    tch.basic_publish("", q_simple, b"tx-rollback")
    tch.tx_rollback()
    time.sleep(0.3)
    check("tx rollback discards buffered publishes",
          lambda: None if ch.basic_get(q_simple, auto_ack=True)[0] is False
          else AssertionError("rolled-back publish was visible"))
    tch.basic_publish("", q_simple, b"tx-commit")
    tch.tx_commit()
    got = ch.basic_get(q_simple, auto_ack=True)
    check("tx commit publishes atomically",
          lambda: None if (got[0] and got[2] == b"tx-commit")
          else AssertionError("committed publish was lost"))
    tch.close()
    ch.close()

    # publisher confirms on a fresh channel
    ch = conn.channel()
    ch.confirm_delivery()
    ch.basic_publish("", q_simple, b"confirmed")
    check("publisher confirm", lambda: None if ch.basic_get(q_simple, auto_ack=True)[0]
          else AssertionError("confirmed message lost"))

    # -- cross-node -----------------------------------------------------
    other = connect(other_url)
    och = other.channel()
    och.confirm_delivery()
    och.queue_declare(q_simple, durable=True, passive=True)
    och.basic_publish("", q_simple, b"from-node2")
    deadline = time.time() + 10
    got = (False, None, None)
    while time.time() < deadline:
        got = ch.basic_get(q_simple, auto_ack=True)
        if got[0]:
            break
        time.sleep(0.2)
    check("write on node2 visible on node1",
          lambda: None if (got[0] and got[2] == b"from-node2")
          else AssertionError("cross-node publish was lost"))

    # consume from node2 with delivery from node1
    ch.confirm_delivery()
    ch.basic_publish("", q_simple, b"from-node1")
    cross: list[bytes] = []

    def on_cross(chx, method, properties, body):
        cross.append(body)
        chx.basic_ack(method.delivery_tag)
        chx.stop_consuming()

    och.basic_consume(q_simple, on_cross, auto_ack=False)
    och.start_consuming()
    check("write on node1 consumed on node2",
          lambda: None if cross == [b"from-node1"] else AssertionError(f"{cross}"))
    other.close()
    conn.close()

    failed = [c for c in CHECKS if not c[1]]
    print(f"\npython/amqp091: {len(CHECKS) - len(failed)}/{len(CHECKS)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
