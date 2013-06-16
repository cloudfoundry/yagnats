package yagnats

import (
	"fmt"
	"time"
)

type Callback func(*Message)

type Client struct {
	connection    chan *Connection
	subscriptions map[int]*Subscription
	disconnecting bool
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

func NewClient() *Client {
	return &Client{
		connection:    make(chan *Connection),
		subscriptions: make(map[int]*Subscription),
	}
}

func (c *Client) Ping() *PongPacket {
	conn := <-c.connection
	conn.Send(&PingPacket{})
	return <-conn.PONGs
}

func (c *Client) Connect(addr, user, pass string) error {
	conn, err := c.connect(addr, user, pass)
	if err != nil {
		return err
	}

	go c.serveConnections(conn, addr, user, pass)
	go c.dispatchMessages()

	return nil
}

func (c *Client) Disconnect() {
	if c.disconnecting {
		return
	}

	conn := <-c.connection
	c.disconnecting = true
	conn.Disconnect()
}

func (c *Client) Publish(subject, payload string) error {
	conn := <-c.connection

	conn.Send(
		&PubPacket{
			Subject: subject,
			Payload: payload,
		},
	)

	return conn.ErrOrOK()
}

func (c *Client) PublishWithReplyTo(subject, payload, reply string) error {
	conn := <-c.connection

	conn.Send(
		&PubPacket{
			Subject: subject,
			Payload: payload,
			ReplyTo: reply,
		},
	)

	return conn.ErrOrOK()
}

func (c *Client) Subscribe(subject string, callback Callback) (int, error) {
	conn := <-c.connection

	id := len(c.subscriptions) + 1

	c.subscriptions[id] = &Subscription{
		Subject:  subject,
		ID:       id,
		Callback: callback,
	}

	conn.Send(
		&SubPacket{
			Subject: subject,
			ID:      id,
		},
	)

	err := conn.ErrOrOK()
	if err != nil {
		return -1, err
	}

	return id, nil
}

func (c *Client) Unsubscribe(sid int) error {
	conn := <-c.connection

	conn.Send(&UnsubPacket{ID: sid})

	delete(c.subscriptions, sid)

	return conn.ErrOrOK()
}

func (c *Client) UnsubscribeAll(subject string) {
	for id, sub := range c.subscriptions {
		if sub.Subject == subject {
			c.Unsubscribe(id)
		}
	}
}

func (c *Client) connect(addr, user, pass string) (conn *Connection, err error) {
	conn = NewConnection(addr, user, pass)

	err = conn.Dial()
	if err != nil {
		return
	}

	err = conn.Handshake()
	if err != nil {
		return
	}

	return
}

func (c *Client) serveConnections(conn *Connection, addr, user, pass string) {
	var err error

	for {
		// serve connection until disconnected
		for stop := false; !stop; {
			select {
			case <-conn.Disconnected:
				stop = true

			case c.connection <- conn:
			}
		}

		// stop if client was told to disconnect
		if c.disconnecting {
			return
		}

		// acquire new connection
		for {
			conn, err = c.connect(addr, user, pass)
			if err == nil {
				c.resubscribe()
				break
			}

			time.Sleep(500 * time.Millisecond)
		}
	}
}

func (c *Client) resubscribe() error {
	conn := <-c.connection

	for id, sub := range c.subscriptions {
		conn.Send(
			&SubPacket{
				Subject: sub.Subject,
				ID:      id,
			},
		)

		err := conn.ErrOrOK()
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) dispatchMessages() {
	for {
		conn := <-c.connection
		msg := <-conn.MSGs

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
	}
}
