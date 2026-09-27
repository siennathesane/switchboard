
import amqp from "amqplib";
const conn = await amqp.connect(process.env.SB_AMQP_URLS.split(",")[0]);
const ch = await conn.createConfirmChannel();
conn.on('error', (e) => console.log('CONN ERROR:', e.message));
conn.on('close', (e) => console.log('CONN CLOSE:', e?.message));
ch.on('error', (e) => console.log('CH ERROR:', e.message));
ch.on('close', (e) => console.log('CH CLOSE:', e?.message));
await ch.assertQueue("node-min", { durable: true });
await ch.publish("", "node-min", Buffer.from("x"), { persistent: true });
await ch.waitForConfirms();
console.log("published");
const got = await new Promise((resolve, reject) => {
  const t = setTimeout(() => reject(new Error("no push delivery")), 10000);
  ch.consume("node-min", (d) => {
    clearTimeout(t);
    ch.ack(d);
    ch.cancel(d.fields.consumerTag).then(() => resolve(d.content.toString()));
  }, { noAck: false });
});
console.log("consumed:", got);
await ch.close(); await conn.close();
