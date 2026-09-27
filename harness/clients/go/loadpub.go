package main

import (
    "context"
    "fmt"
    "os"
    "time"

    amqp "github.com/rabbitmq/amqp091-go"
)

func dial(url string) *amqp.Connection {
    for i := 0; i < 40; i++ {
        conn, err := amqp.Dial(url)
        if err == nil {
            return conn
        }
        time.Sleep(500 * time.Millisecond)
    }
    return nil
}

func main() {
    conn := dial(os.Args[1])
    if conn == nil { fmt.Println(0); return }
    defer conn.Close()
    ch, err := conn.Channel()
    if err != nil { fmt.Println(0); return }
    ctx := context.Background()
    ch.Confirm(false)
    n := 0
    deadline := time.Now().Add(8 * time.Second)
    for time.Now().Before(deadline) {
        conf, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", os.Args[2],
            false, false, amqp.Publishing{Body: []byte(fmt.Sprintf("go:%d", n)),
            DeliveryMode: amqp.Persistent})
        if err != nil { break }
        if !conf.Wait() { break }
        n++
    }
    fmt.Println(n)
}
