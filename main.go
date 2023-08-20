package main

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"math/rand"
	"time"
)

func main() {
	delayType := "x-delayed-message"
	//delayType := "direct"
	c, err := NewClient("amqp://root:123456@localhost:5672/",
		"my-exchange", delayType, "my-queue",
		"my-key", "my-tag")
	if err != nil {
		panic(err)
	}
	go sending(c)
	receiving(c)
	err = c.Shutdown()
	if err != nil {
		panic(err)
	}
}

func sending(c *Client) {
	for i := 0; ; i++ {
		msg := fmt.Sprintf("%v helo", time.Now().Format("2006-01-02 15:04:05"))
		ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
		pub := amqp.Publishing{Body: []byte(msg)}
		if i%2 == 0 {
			intn := rand.Intn(100)
			// 延迟多少ms发送
			pub.Headers = make(amqp.Table)
			pub.Headers["x-delay"] = intn * 1000
			appd := fmt.Sprintf(" delay: %vs", intn)
			pub.Body = append(pub.Body, []byte(appd)...)
		}
		err := c.channel.PublishWithContext(ctx, c.exchange, c.key,
			false, false, pub)
		if err != nil {
			fmt.Println(err)
			continue
		}
		cancelFunc()
		time.Sleep(2 * time.Second)
	}
}

func receiving(c *Client) {
	var err error
	for d := range c.delivery {
		select {
		case <-time.After(2 * time.Minute):
			break
		default:
		}
		fmt.Printf("now:%v,tag:%v,body:%s\n", time.Now().Format("2006-01-02 15:04:05"),
			d.DeliveryTag, string(d.Body))
		err = d.Ack(false)
		if err != nil {
			fmt.Println(err)
			continue
		}
	}
}

type Client struct {
	exchange string
	key      string
	channel  *amqp.Channel
	conn     *amqp.Connection
	delivery <-chan amqp.Delivery
	tag      string
}

func NewClient(amqpURI, exchange, exchangeType, queueName, key, tag string) (*Client, error) {
	config := amqp.Config{Properties: amqp.NewConnectionProperties()}
	config.Properties.SetClientConnectionName("my-consumer")
	conn, err := amqp.DialConfig(amqpURI, config)
	if err != nil {
		return nil, err
	}
	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	eargs := make(map[string]interface{})
	// 使用插件提供的交换类型才可以发送延迟消息
	if exchangeType == "x-delayed-message" {
		eargs["x-delayed-type"] = "direct"
	}
	err = channel.ExchangeDeclare(
		exchange,
		exchangeType,
		true,
		false,
		false,
		false,
		eargs,
	)
	if err != nil {
		return nil, err
	}
	queue, err := channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}
	err = channel.QueueBind(
		queue.Name,
		key,
		exchange,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}
	delivery, err := channel.Consume(
		queue.Name,
		tag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}
	return &Client{
		exchange: exchange,
		key:      key,
		channel:  channel,
		conn:     conn,
		delivery: delivery,
		tag:      tag,
	}, nil
}

func (c *Client) Shutdown() error {
	err := c.channel.Cancel(c.tag, true)
	if err != nil {
		return err
	}
	return c.conn.Close()
}
