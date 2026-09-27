package main

import (
	"context"
	"fmt"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial(os.Getenv("SB_AMQP_URLS"))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	nacks := make(chan uint64, 64)
	acks := make(chan uint64, 64)
	ch.NotifyConfirm(acks, nacks)
	go func() {
		for {
			select {
			case a := <-acks:
				fmt.Println("  ACK", a)
			case n := <-nacks:
				fmt.Println("  NACK", n)
			case <-time.After(30 * time.Second):
				return
			}
		}
	}()

	closeCh := ch.NotifyClose(make(chan *amqp.Error, 1))
	go func() {
		for e := range closeCh {
			fmt.Println("!! CHANNEL CLOSE:", e)
		}
	}()
	if err := ch.Confirm(false); err != nil {
		panic(err)
	}
	_, err = ch.QueueDeclare("probe-nack", true, false, false, false, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println("-- publish 1 (confirm)")
	conf, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", "probe-nack", false, false,
		amqp.Publishing{Body: []byte("one")})
	if err != nil {
		panic(err)
	}
	fmt.Println("  pub1 wait:", conf.Wait())
	fmt.Println("-- get")
	d, ok, err := ch.Get("probe-nack", false)
	if err != nil || !ok {
		panic(fmt.Sprint("get:", err, ok))
	}
	fmt.Println("-- ack", d.DeliveryTag)
	if err := ch.Ack(d.DeliveryTag, false); err != nil {
		panic(err)
	}
	time.Sleep(2 * time.Second)
	fmt.Println("-- publish 2 (confirm)")
	conf, err = ch.PublishWithDeferredConfirmWithContext(ctx, "", "probe-nack", false, false,
		amqp.Publishing{Body: []byte("two")})
	if err != nil {
		panic(err)
	}
	fmt.Println("  pub2 wait:", conf.Wait())
	// Is the channel still alive after the lost confirm?
	_, err = ch.QueueDeclarePassive("probe-nack", true, false, false, false, nil)
	fmt.Println("  passive declare after lost confirm:", err)
	time.Sleep(1 * time.Second)
}
