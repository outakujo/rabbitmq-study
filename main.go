package main

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

func main() {
	c, err := NewClient("amqp://root:123456@localhost:5672/",
		"my-exchange", "direct", "my-queue",
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
	for {
		msg := fmt.Sprintf("%v helo", time.Now().Format("2006-01-02 15:04:05"))
		ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
		err := c.channel.PublishWithContext(ctx, c.exchange, c.key,
			false, false,
			amqp.Publishing{Body: []byte(msg)})
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
		case <-time.After(time.Minute):
			break
		default:
		}
		fmt.Printf("ctag:%s,tag:%v,body:%s\n", d.ConsumerTag,
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
	err = channel.ExchangeDeclare(
		exchange,     // name of the exchange
		exchangeType, // type
		true,         // durable
		false,        // delete when complete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	)
	if err != nil {
		return nil, err
	}
	queue, err := channel.QueueDeclare(
		queueName, // name of the queue
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		return nil, err
	}
	err = channel.QueueBind(
		queue.Name, // name of the queue
		key,        // bindingKey
		exchange,   // sourceExchange
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		return nil, err
	}
	delivery, err := channel.Consume(
		queue.Name, // name
		tag,        // consumerTag,
		false,      // autoAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
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
