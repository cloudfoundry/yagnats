package yagnats

import (
	"bufio"
	"errors"
	"fmt"
	"net"
)

type Callback func(*Message)

type Client struct {
	writer chan Packet

	pongs chan *PongPacket
	oks   chan *OKPacket
	errs  chan error

	subscriptions map[int]*Subscription
}

type Message struct {
	Subject string
	Payload string
	ReplyTo string
}

type Subscription struct {
	Subject  string
	Callback Callback
	ID       int
}

func Dial(addr string) (client *Client, err error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return NewClient(conn), nil
}

func NewClient(conn net.Conn) (client *Client) {
	client = &Client{
		writer:        make(chan Packet),
		pongs:         make(chan *PongPacket),
		oks:           make(chan *OKPacket),
		errs:          make(chan error),
		subscriptions: make(map[int]*Subscription),
	}

	go client.writePackets(conn)
	go client.handlePackets(bufio.NewReader(conn))

	return
}

func (c *Client) Ping() *PongPacket {
	c.sendPacket(&PingPacket{})
	return <-c.pongs
}

func (c *Client) Connect(user, pass string) error {
	c.sendPacket(&ConnectPacket{User: user, Pass: pass})

	select {
	case <-c.oks:
		return nil
	case err := <-c.errs:
		return err
	}
}

func (c *Client) Publish(subject, payload string) error {
	c.sendPacket(
		&PubPacket{
			Subject: subject,
			Payload: payload,
		},
	)

	select {
	case err := <-c.errs:
		return err
	case <-c.oks:
		return nil
	}
}

func (c *Client) PublishWithReplyTo(subject, payload, reply string) error {
	c.sendPacket(
		&PubPacket{
			Subject: subject,
			Payload: payload,
      ReplyTo: reply,
		},
	)

	select {
	case err := <-c.errs:
		return err
	case <-c.oks:
		return nil
	}
}

func (c *Client) Subscribe(subject string, callback Callback) (int, error) {
	id := len(c.subscriptions) + 1

	c.subscriptions[id] = &Subscription{
		Subject:  subject,
		ID:       id,
		Callback: callback,
	}

	c.sendPacket(
		&SubPacket{
			Subject: subject,
			ID:      id,
		},
	)

	select {
	case err := <-c.errs:
		return -1, err
	case <-c.oks:
		return id, nil
	}
}

func (c *Client) UnsubscribeAll(subject string) {
	for id, sub := range c.subscriptions {
		if sub.Subject == subject {
			c.Unsubscribe(id)
		}
	}
}

func (c *Client) Unsubscribe(sid int) error {
	c.sendPacket(&UnsubPacket{ID: sid})

	select {
	case err := <-c.errs:
		return err
	case <-c.oks:
		delete(c.subscriptions, sid)
		return nil
	}
}

func (c *Client) sendPacket(packet Packet) {
	c.writer <- packet
}

func (c *Client) writePackets(conn net.Conn) {
	for {
		packet := <-c.writer

		// TODO: check if written == packet length?
		written, err := conn.Write(packet.Encode())

		if err != nil {
			// TODO
			fmt.Printf("Error: %s (wrote %d)\n", err, written)
			return
		}
	}
}

func (c *Client) handlePackets(io *bufio.Reader) {
	for {
		packet, err := Parse(io)
		if err != nil {
			// TODO
			fmt.Printf("ERROR! %s\n", err)
			break
		}

		switch packet.(type) {
		case *PongPacket:
			c.pongs <- packet.(*PongPacket)

		case *OKPacket:
			c.oks <- packet.(*OKPacket)

		case *ERRPacket:
			c.errs <- errors.New(packet.(*ERRPacket).Message)

		case *InfoPacket:
			// noop

		case *PingPacket:
			c.sendPacket(&PongPacket{})

		case *MsgPacket:
			msg := packet.(*MsgPacket)
			sub := c.subscriptions[msg.SubID]
			if sub == nil {
				fmt.Printf("Warning: Message for unknown subscription (%s, %d): %#v\n", msg.Subject, msg.SubID, msg)
				break
			}

			go sub.Callback(
				&Message{
					Subject: msg.Subject,
					Payload: msg.Payload,
					ReplyTo: msg.ReplyTo,
				},
			)

		default:
			// TODO
			fmt.Printf("Unhandled packet: %#v\n", packet)
		}
	}
}
