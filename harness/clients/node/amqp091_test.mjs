// Switchboard conformance suite — Node.js (amqplib).
import amqp from "amqplib";

const checks = [];
let failed = 0;

function check(name, fn) {
  return Promise.resolve()
    .then(fn)
    .then(() => {
      checks.push([name, true]);
      console.log(`PASS ${name}`);
    })
    .catch((e) => {
      checks.push([name, false]);
      failed++;
      console.log(`FAIL ${name}: ${e.message ?? e}`);
    });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function getEventually(ch, queue, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const d = await ch.get(queue, { noAck: false });
    if (d) return d;
    await sleep(100);
  }
  return null;
}

async function main() {
  const urls = process.env.SB_AMQP_URLS.split(",");
  const otherUrl = urls[1] ?? urls[0];
  const runID = `${Date.now() % 1_000_000}`;

  const conn = await amqp.connect(urls[0]);
  const dx = `nx-d-${runID}`, fx = `nx-f-${runID}`, tx = `nx-t-${runID}`;
  const q = `nq-${runID}`;

  await check("connect + channel", async () => {
    const ch = await conn.createChannel();
    await ch.close();
  });

  const ch = await conn.createConfirmChannel();

  await check("declare exchanges", async () => {
    await ch.assertExchange(dx, "direct", { durable: true });
    await ch.assertExchange(fx, "fanout", { durable: true });
    await ch.assertExchange(tx, "topic", { durable: true });
  });

  await check("queue declare + bind", async () => {
    await ch.assertQueue(q, { durable: true });
    await ch.bindQueue(q, dx, "k1");
  });

  await check("publish confirmed", async () => {
    await ch.publish(dx, "k1", Buffer.from("node-payload"), {
      contentType: "text/plain",
      persistent: true,
      messageId: runID,
    });
    await ch.waitForConfirms();
  });

  await check("basic.get roundtrip + properties", async () => {
    const d = await getEventually(ch, q, 10_000);
    if (!d) throw new Error("no message");
    if (d.content.toString() !== "node-payload") throw new Error(`body=${d.content}`);
    if (d.properties.contentType !== "text/plain") throw new Error("content-type lost");
    if (d.fields.routingKey !== "k1") throw new Error(`rk=${d.fields.routingKey}`);
    ch.ack(d);
  });

  await check("topic wildcards", async () => {
    const qStar = `nq-star-${runID}`, qHash = `nq-hash-${runID}`;
    await ch.assertQueue(qStar, { durable: true });
    await ch.assertQueue(qHash, { durable: true });
    await ch.bindQueue(qStar, tx, "a.*");
    await ch.bindQueue(qHash, tx, "a.#");
    await ch.publish(tx, "a.b", Buffer.from("both"));
    await ch.publish(tx, "a.b.c", Buffer.from("deep"));
    await ch.waitForConfirms();
    await sleep(300);
    const s1 = await getEventually(ch, qStar, 5_000);
    const h1 = await getEventually(ch, qHash, 5_000);
    const h2 = await getEventually(ch, qHash, 5_000);
    if (!s1 || !h1 || !h2) throw new Error(`star=${!!s1} hash=${!!h1},${!!h2}`);
    if (s1.content.toString() !== "both" || h1.content.toString() !== "both" || h2.content.toString() !== "deep")
      throw new Error(`bodies ${s1.content} ${h1.content} ${h2.content}`);
    ch.ack(s1); ch.ack(h1); ch.ack(h2);
  });

  await check("consume push + ack", async () => {
    await ch.publish(dx, "k1", Buffer.from("pushed"));
    await ch.waitForConfirms();
    const got = await new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("no push delivery")), 15_000);
      ch.consume(q, (d) => {
        clearTimeout(t);
        ch.ack(d);
        ch.cancel(d.fields.consumerTag).then(() => resolve(d.content.toString()));
      }, { noAck: false });
    });
    if (got !== "pushed") throw new Error(`got=${got}`);
  });

  await check("dead-letter via nack", async () => {
    const dlq = `nq-dlx-${runID}`, src = `nq-dlxs-${runID}`;
    await ch.assertQueue(dlq, { durable: true });
    await ch.assertQueue(src, { durable: true, arguments: {
      "x-dead-letter-exchange": "",
      "x-dead-letter-routing-key": dlq,
    } });
    await ch.publish("", src, Buffer.from("doomed"), { persistent: true });
    await ch.waitForConfirms();
    const d = await getEventually(ch, src, 10_000);
    if (!d) throw new Error("no delivery to nack");
    ch.nack(d, false, false);
    const dl = await getEventually(ch, dlq, 10_000);
    if (!dl || dl.content.toString() !== "doomed") throw new Error("dead-letter lost");
    ch.ack(dl);
  });

  await check("nack with requeue redelivers", async () => {
    const rq = `nq-rq-${runID}`;
    await ch.assertQueue(rq, { durable: true });
    await ch.publish("", rq, Buffer.from("again"), { persistent: true });
    await ch.waitForConfirms();
    const d = await getEventually(ch, rq, 10_000);
    if (!d) throw new Error("no delivery");
    ch.nack(d, false, true);
    const d2 = await getEventually(ch, rq, 10_000);
    if (!d2 || d2.content.toString() !== "again") throw new Error("did not requeue");
    if (!d2.fields.redelivered) throw new Error("redelivered flag missing");
    ch.ack(d2);
  });

  await check("cross-node publish (write node2, read node1)", async () => {
    const conn2 = await amqp.connect(otherUrl);
    const ch2 = await conn2.createConfirmChannel();
    await ch2.assertQueue(q, { durable: true });
    await ch2.publish("", q, Buffer.from("cross"));
    await ch2.waitForConfirms();
    const d = await getEventually(ch, q, 10_000);
    if (!d || d.content.toString() !== "cross") throw new Error("cross lost");
    ch.ack(d);
    await conn2.close();
  });

  await check("bad credentials rejected", async () => {
    const bad = urls[0].replace("guest:guest", "guest:wrongpass");
    try {
      await amqp.connect(bad);
      throw new Error("wrong password accepted");
    } catch (e) {
      if (e.message === "wrong password accepted") throw e;
      // ACCESS_REFUSED — expected
    }
  });

  await ch.close();
  await conn.close();

  console.log(`\nnode/amqp091: ${checks.length - failed}/${checks.length} checks passed`);
  process.exit(failed ? 1 : 0);
}

main().catch((e) => {
  console.error(e);
  process.exit(2);
});
