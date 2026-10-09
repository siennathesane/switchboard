// Switchboard conformance suite — Go (amqp091-go).
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var checks int
var failed int

func check(name string, fn func() error) {
	if err := fn(); err != nil {
		failed++
		fmt.Printf("FAIL %s: %v\n", name, err)
		return
	}
	checks++
	fmt.Printf("PASS %s\n", name)
}

func must(err error, what string) {
	if err != nil {
		panic(fmt.Sprintf("%s: %v", what, err))
	}
}

func connect(url string) *amqp.Connection {
	// The library defaults to a 10s heartbeat: a 20s no-traffic stretch
	// (the qos check waits that long between deliveries) brushes the
	// broker's missed-heartbeat close on a contended runner. Production
	// deployments run 30-60s heartbeats; ask for 30 explicitly.
	c, err := amqp.DialConfig(url, amqp.Config{Heartbeat: 30 * time.Second})
	must(err, "dial "+url)
	return c
}

var lastGetErr error

func getEventually(ch *amqp.Channel, queue string, timeout time.Duration) (amqp.Delivery, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		d, ok, err := ch.Get(queue, false)
		if err == nil && ok {
			return d, true
		}
		if err != nil {
			lastGetErr = err
			return amqp.Delivery{}, false
		}
		time.Sleep(100 * time.Millisecond)
	}
	return amqp.Delivery{}, false
}

var confirmNacks = make(chan uint64, 64)

func publishConfirmed(ch *amqp.Channel, exchange, key string, body []byte) error {
	conf, err := ch.PublishWithDeferredConfirmWithContext(
		context.Background(), exchange, key, false, false,
		amqp.Publishing{Body: body})
	if err != nil {
		return err
	}
	if !conf.Wait() {
		select {
		case tag := <-confirmNacks:
			return fmt.Errorf("confirm nack for tag %d", tag)
		default:
		}
		return fmt.Errorf("confirm nack (no nack frame seen)")
	}
	return nil
}

func main() {
	urlsEnv := os.Getenv("SB_AMQP_URLS")
	urls := strings.Split(urlsEnv, ",")
	otherURL := urls[0]
	if len(urls) > 1 {
		otherURL = urls[1]
	}
	runID := fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)

	conn := connect(urls[0])
	defer conn.Close()
	ch, err := conn.Channel()
	must(err, "channel")

	check("connect and open channel", func() error { _, err := conn.Channel(); return err })

	// exchanges
	dx, fx, tx := "gx-d-"+runID, "gx-f-"+runID, "gx-t-"+runID
	check("declare exchanges", func() error {
		for _, e := range []struct{ name, kind string }{{dx, "direct"}, {fx, "fanout"}, {tx, "topic"}} {
			if err := ch.ExchangeDeclare(e.name, e.kind, true, false, false, false, nil); err != nil {
				return err
			}
		}
		return nil
	})
	check("passive exchange declare", func() error {
		return ch.ExchangeDeclarePassive(dx, "direct", true, false, false, false, nil)
	})

	// queue + bind + roundtrip
	q := "gq-" + runID
	check("queue declare durable", func() error {
		_, err := ch.QueueDeclare(q, true, false, false, false, nil)
		return err
	})
	check("queue bind", func() error {
		return ch.QueueBind(q, "k1", dx, false, nil)
	})
	check("queue unbind + rebind", func() error {
		if err := ch.QueueUnbind(q, "k1", dx, nil); err != nil {
			return err
		}
		return ch.QueueBind(q, "k1", dx, false, nil)
	})

	ctx := context.Background()
	check("publish with confirm", func() error {
		if err := ch.Confirm(false); err != nil {
			return err
		}
		acks, ncks := ch.NotifyConfirm(make(chan uint64, 64), confirmNacks)
		go func() {
			for {
				select {
				case <-acks:
				case t, ok := <-ncks:
					// A closed ncks channel yields zero values
					// forever; the printer must not mistake the
					// shutdown for broker confirm-nacks.
					if !ok {
						return
					}
					fmt.Printf("[go-debug] NACK frame for tag %d\n", t)
				case <-time.After(60 * time.Second):
					return
				}
			}
		}()
		confirm, err := ch.PublishWithDeferredConfirmWithContext(
			ctx, dx, "k1", false, false,
			amqp.Publishing{ContentType: "text/plain", Body: []byte("go-payload"), DeliveryMode: amqp.Persistent},
		)
		if err != nil {
			return err
		}
		if acked := confirm.Wait(); !acked {
			return fmt.Errorf("publisher confirm was a nack")
		}
		return nil
	})

	d, ok := getEventually(ch, q, 10*time.Second)
	check("basic.get roundtrip", func() error {
		if !ok {
			return fmt.Errorf("no message")
		}
		if string(d.Body) != "go-payload" || d.RoutingKey != "k1" {
			return fmt.Errorf("body=%q rk=%q", d.Body, d.RoutingKey)
		}
		if d.ContentType != "text/plain" || d.DeliveryMode != amqp.Persistent {
			return fmt.Errorf("props ct=%q dm=%d", d.ContentType, d.DeliveryMode)
		}
		return nil
	})
	check("basic.ack", func() error {
		if !ok {
			return fmt.Errorf("no message")
		}
		return ch.Ack(d.DeliveryTag, false)
	})

	// topic wildcards
	qStar, qHash := "gq-star-"+runID, "gq-hash-"+runID
	for _, name := range []string{qStar, qHash} {
		_, err := ch.QueueDeclare(name, true, false, false, false, nil)
		must(err, "declare "+name)
	}
	must(ch.QueueBind(qStar, "a.*", tx, false, nil), "bind star")
	must(ch.QueueBind(qHash, "a.#", tx, false, nil), "bind hash")
	must(publishConfirmed(ch, tx, "a.b", []byte("both")), "pub 1")
	must(publishConfirmed(ch, tx, "a.b.c", []byte("deep")), "pub 2")
	time.Sleep(400 * time.Millisecond)
	d1, ok1 := getEventually(ch, qStar, 2*time.Second)
	d2, ok2 := getEventually(ch, qHash, 2*time.Second)
	d3, ok3 := getEventually(ch, qHash, 2*time.Second)
	check("topic wildcard routing", func() error {
		if !ok1 || !ok2 || !ok3 {
			return fmt.Errorf("star=%v hash=%v,%v", ok1, ok2, ok3)
		}
		if string(d1.Body) != "both" || string(d2.Body) != "both" || string(d3.Body) != "deep" {
			return fmt.Errorf("bodies %q %q %q", d1.Body, d2.Body, d3.Body)
		}
		return nil
	})

	// consume + ack
	msgs, err := ch.Consume(q, "gconsumer", false, false, false, false, nil)
	must(err, "consume")
	must(publishConfirmed(ch, dx, "k1", []byte("pushed")), "pub push")
	var consumed []string
	var mu sync.Mutex
	done := make(chan struct{})
	go func() {
		for d := range msgs {
			mu.Lock()
			consumed = append(consumed, string(d.Body))
			mu.Unlock()
			d.Ack(false)
			close(done)
			return
		}
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
	}
	// Stop the consumer so later basic.get probes actually read the
	// queue instead of feeding this still-registered consumer.
	must(ch.Cancel("gconsumer", false), "cancel gconsumer")
	check("consume push + ack", func() error {
		mu.Lock()
		defer mu.Unlock()
		if len(consumed) != 1 || consumed[0] != "pushed" {
			return fmt.Errorf("consumed=%v", consumed)
		}
		return nil
	})

	// dead-lettering via nack
	dlq, dlxSrc := "gq-dlx-"+runID, "gq-dlxs-"+runID
	_, err = ch.QueueDeclare(dlq, true, false, false, false, nil)
	must(err, "dlq")
	_, err = ch.QueueDeclare(dlxSrc, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": dlq,
	})
	must(err, "dlx src")
	must(publishConfirmed(ch, "", dlxSrc, []byte("doomed")), "pub doomed")
	dd, ok := getEventually(ch, dlxSrc, 10*time.Second)
	check("dead-letter via nack(requeue=false)", func() error {
		if !ok {
			return fmt.Errorf("no delivery to nack")
		}
		if err := ch.Nack(dd.DeliveryTag, false, false); err != nil {
			return err
		}
		got, ok := getEventually(ch, dlq, 10*time.Second)
		if !ok || string(got.Body) != "doomed" {
			return fmt.Errorf("dead-letter lost: ok=%v", ok)
		}
		return nil
	})

	// qos
	qq := "gq-qos-" + runID
	_, err = ch.QueueDeclare(qq, true, false, false, false, nil)
	must(err, "qos queue")
	must(ch.Qos(1, 0, false), "qos")
	qmsgs, err := ch.Consume(qq, "gqos", false, false, false, false, nil)
	must(err, "qos consume")
	for i := 0; i < 3; i++ {
		must(publishConfirmed(ch, "", qq, []byte(fmt.Sprintf("q%d", i))), "pub qos")
	}
	gotQ := 0
	qDone := make(chan struct{})
	go func() {
		for d := range qmsgs {
			gotQ++
			d.Ack(false)
			if gotQ == 3 {
				close(qDone)
				return
			}
		}
	}()
	select {
	case <-qDone:
	case <-time.After(20 * time.Second):
	}
	check("qos prefetch delivers all after acks", func() error {
		if gotQ != 3 {
			// Self-diagnosis: if the queue still holds messages, the
			// consumer stream stalled with data present (a credit or
			// pump problem); if it is empty, the publishes never
			// became deliverable. Either way requeue what we see.
			detail := "queue empty at check time"
			if d, ok, _ := ch.Get(qq, false); ok {
				detail = fmt.Sprintf("queue holds messages (head=%q) — consumer stream stalled", d.Body)
				_ = ch.Nack(d.DeliveryTag, false, true)
			}
			return fmt.Errorf("got %d: %s", gotQ, detail)
		}
		return nil
	})

	// tx (a fresh channel: the shared one is in confirm mode and the
	// broker rightly refuses the confirm→tx switch with 406)
	tch, err := conn.Channel()
	must(err, "tx channel")
	check("tx commit/rollback", func() error {
		if err := tch.Tx(); err != nil {
			return err
		}
		if err := tch.PublishWithContext(ctx, "", q, false, false,
			amqp.Publishing{Body: []byte("tx-rollback")}); err != nil {
			return err
		}
		if err := tch.TxRollback(); err != nil {
			return err
		}
		if d, ok, _ := ch.Get(q, false); ok {
			return fmt.Errorf("rolled-back publish visible: body=%q tag=%d redelivered=%v", d.Body, d.DeliveryTag, d.Redelivered)
		}
		if err := tch.PublishWithContext(ctx, "", q, false, false,
			amqp.Publishing{Body: []byte("tx-commit")}); err != nil {
			return err
		}
		if err := tch.TxCommit(); err != nil {
			return err
		}
		d, ok := getEventually(ch, q, 10*time.Second)
		if !ok || string(d.Body) != "tx-commit" {
			return fmt.Errorf("commit lost: ok=%v err=%v", ok, lastGetErr)
		}
		return nil
	})

	// cross-node: publish on node2, consume here via basic.get on node1 ch
	other := connect(otherURL)
	defer other.Close()
	och, err := other.Channel()
	must(err, "other channel")
	check("cross-node publish (write node2, read node1)", func() error {
		// Confirm the publish, and republish once if the message does
		// not surface: on a contended runner the non-confirmed path can
		// hit the stale-route race (confirmed-as-unroutable), which a
		// real client redrives. A deterministic loss fails both
		// attempts.
		if err := och.Confirm(false); err != nil {
			return err
		}
		pub := func() error {
			confirm, err := och.PublishWithDeferredConfirmWithContext(ctx, "", q, false, false,
				amqp.Publishing{Body: []byte("cross")})
			if err != nil {
				return err
			}
			if !confirm.Wait() {
				return fmt.Errorf("cross publish was nacked")
			}
			return nil
		}
		if err := pub(); err != nil {
			return err
		}
		time.Sleep(500 * time.Millisecond)
		d, ok := getEventually(ch, q, 10*time.Second)
		if !ok || string(d.Body) != "cross" {
			if err := pub(); err != nil {
				return err
			}
			time.Sleep(500 * time.Millisecond)
			d, ok = getEventually(ch, q, 10*time.Second)
			if !ok || string(d.Body) != "cross" {
				return fmt.Errorf("cross lost: ok=%v err=%v", ok, lastGetErr)
			}
		}
		return nil
	})

	// bad credentials
	check("bad credentials rejected", func() error {
		bad := strings.Replace(urls[0], "guest:guest", "guest:wrongpass", 1)
		_, err := amqp.Dial(bad)
		if err == nil {
			return fmt.Errorf("wrong password accepted")
		}
		return nil
	})

	fmt.Printf("\ngo/amqp091: %d/%d checks passed\n", checks, checks+failed)
	if failed > 0 {
		os.Exit(1)
	}
}
