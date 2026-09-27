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
	ctx := context.Background()
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	_, err = ch.QueueDeclare("probe-tx", true, false, false, false, nil)
	if err != nil {
		panic(err)
	}
	if err = ch.Tx(); err != nil {
		panic(err)
	}
	fmt.Println("tx selected")
	if err = ch.PublishWithContext(ctx, "", "probe-tx", false, false,
		amqp.Publishing{Body: []byte("rolled")}); err != nil {
		panic(err)
	}
	fmt.Println("published into tx")
	if err = ch.TxRollback(); err != nil {
		panic(err)
	}
	fmt.Println("rolled back")
	time.Sleep(300 * time.Millisecond)
	_, ok, err := ch.Get("probe-tx", true)
	if err != nil {
		panic(err)
	}
	if ok {
		fmt.Println("BUG: rolled-back publish is visible")
		os.Exit(1)
	}
	fmt.Println("rollback discarded the publish: OK")
}
