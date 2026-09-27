# Switchboard conformance suite — Ruby (bunny).
require "bunny"

$checks = 0
$failed = 0

def check(name)
  yield
  $checks += 1
  puts "PASS #{name}"
rescue StandardError => e
  $failed += 1
  puts "FAIL #{name}: #{e.message}"
end

# Bunny's basic_get returns a triple [get_ok, properties, payload] (all
# nil when the queue is empty). get_ok exposes .delivery_tag etc.
def get_eventually(ch, queue, timeout: 10.0)
  deadline = Time.now + timeout
  loop do
    return nil if Time.now > deadline
    triple = ch.basic_get(queue, manual_ack: true)
    return triple if triple[0]
    sleep 0.1
  end
end

def tag_of(d) = d[0].delivery_tag

def body_of(d) = d[2]

def prop(d, key)
  d[1].respond_to?(key) ? d[1].public_send(key) : d[1][key]
end

urls = ENV.fetch("SB_AMQP_URLS").split(",")
other_url = urls[1] || urls[0]
run_id = Process.clock_gettime(Process::CLOCK_REALTIME).to_i % 1_000_000

conn = Bunny.new(urls[0]).start
dx = "rx-d-#{run_id}"; fx = "rx-f-#{run_id}"; tx = "rx-t-#{run_id}"
q = "rq-#{run_id}"

check("connect + channel") { conn.create_channel.close }

ch = conn.create_channel
ch.confirm_select

check("declare exchanges") do
  ch.exchange_declare(dx, "direct", durable: true)
  ch.exchange_declare(fx, "fanout", durable: true)
  ch.exchange_declare(tx, "topic", durable: true)
end

check("queue declare + bind") do
  ch.queue_declare(q, durable: true)
  ch.queue_bind(q, dx, routing_key: "k1")
end

check("publish confirmed") do
  ch.basic_publish("node-ruby", dx, "k1", content_type: "text/plain",
                   persistent: true, message_id: run_id.to_s)
  ch.wait_for_confirms
end

check("basic.get roundtrip + properties") do
  d = get_eventually(ch, q) or raise "no message"
  raise "body=#{body_of(d)}" unless body_of(d) == "node-ruby"
  raise "ct=#{prop(d, :content_type)}" unless prop(d, :content_type) == "text/plain"
  ch.ack(tag_of(d))
end

check("topic wildcards") do
  q_star = "rq-star-#{run_id}"; q_hash = "rq-hash-#{run_id}"
  ch.queue_declare(q_star, durable: true)
  ch.queue_declare(q_hash, durable: true)
  ch.queue_bind(q_star, tx, routing_key: "a.*")
  ch.queue_bind(q_hash, tx, routing_key: "a.#")
  ch.basic_publish("both", tx, "a.b")
  ch.basic_publish("deep", tx, "a.b.c")
  ch.wait_for_confirms
  sleep 0.3
  s1 = get_eventually(ch, q_star) or raise "star empty"
  h1 = get_eventually(ch, q_hash) or raise "hash empty 1"
  h2 = get_eventually(ch, q_hash) or raise "hash empty 2"
  raise "bodies" unless body_of(s1) == "both" && body_of(h1) == "both" && body_of(h2) == "deep"
  ch.ack(tag_of(s1)); ch.ack(tag_of(h1)); ch.ack(tag_of(h2))
end

check("consume push + ack") do
  ch.basic_publish("pushed", dx, "k1")
  ch.wait_for_confirms
  got = nil
  q_obj = ch.queue(q, durable: true)
  sub = q_obj.subscribe(manual_ack: true, block: false) do |delivery, _props, payload|
    got = payload
    ch.ack(delivery.delivery_tag)
  end
  deadline = Time.now + 15
  while got.nil? && Time.now < deadline
    sleep 0.1
  end
  sub.cancel
  raise "no push delivery" unless got == "pushed"
end

check("dead-letter via nack") do
  dlq = "rq-dlx-#{run_id}"; src = "rq-dlxs-#{run_id}"
  ch.queue_declare(dlq, durable: true)
  ch.queue_declare(src, durable: true, arguments: {
    "x-dead-letter-exchange" => "",
    "x-dead-letter-routing-key" => dlq,
  })
  ch.basic_publish("doomed", "", src, persistent: true)
  ch.wait_for_confirms
  d = get_eventually(ch, src) or raise "no delivery to nack"
  ch.nack(tag_of(d), false, false)
  dl = get_eventually(ch, dlq) or raise "dead-letter lost"
  raise "wrong body" unless body_of(dl) == "doomed"
  ch.ack(tag_of(dl))
end

check("cross-node publish (write node2, read node1)") do
  conn2 = Bunny.new(other_url).start
  ch2 = conn2.create_channel
  ch2.confirm_select
  ch2.queue_declare(q, durable: true)
  ch2.basic_publish("cross", "", q)
  ch2.wait_for_confirms
  d = get_eventually(ch, q) or raise "cross lost"
  raise "body" unless body_of(d) == "cross"
  ch.ack(tag_of(d))
  conn2.close
end

check("bad credentials rejected") do
  bad = urls[0].sub("guest:guest", "guest:wrongpass")
  raised = false
  begin
    Bunny.new(bad).start
  rescue StandardError
    raised = true
  end
  raise "wrong password accepted" unless raised
end

conn.close

puts "\nruby/amqp091: #{$checks}/#{$checks + $failed} checks passed"
exit($failed.zero? ? 0 : 1)
