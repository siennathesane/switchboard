package main

import (
	"context"
	"fmt"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func must(err error, what string) {
	if err != nil {
		panic(fmt.Sprintf("%s: %v", what, err))
	}
}

func main() {
	url := os.Getenv("SB_AMQP_URLS")
	conn, err := amqp.Dial(url)
	must(err, "dial")
	defer conn.Close()
	ctx := context.Background()
	run := os.Args[1]
	ch, _ := conn.Channel()
	_ = ch.Confirm(false)
	src, dlq := "probe-src-"+run, "probe-dlq-"+run
	_, _ = ch.QueueDeclare(dlq, true, false, false, false, nil)
	_, err = ch.QueueDeclare(src, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange": "", "x-dead-letter-routing-key": dlq})
	must(err, "declare src")
	c, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", src, false, false,
		amqp.Publishing{Body: []byte("doomed"), DeliveryMode: amqp.Persistent})
	must(err, "pub")
	if !c.Wait() {
		fmt.Println("not confirmed")
		os.Exit(1)
	}
	d, ok, err := ch.Get(src, false)
	must(err, "get")
	fmt.Printf("run %s: got delivery tag %d ok=%v\n", run, d.DeliveryTag, ok)
	must(ch.Nack(d.DeliveryTag, false, false), "nack")
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, ok, err := ch.Get(dlq, true)
		if err == nil && ok {
			fmt.Printf("run %s: dead-lettered OK\n", run)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Printf("run %s: DEAD LETTER LOST\n", run)
	os.Exit(1)
}
