package yagnats

import (
	"time"
	"sync"
)

type Callback func(*Message)

type Client struct {
	sync.RWMutex
	connection    chan *Connection
	subscriptions map[int]*Subscription
	disconnecting bool

	ConnectedCallback func()

	Logger Logger
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
		Logger:        &DefaultLogger{},
	}
}

func (c *Client) Ping() bool {
	select {
	case conn := <-c.connection:
		return conn.Ping()
	case <-time.After(500 * time.Millisecond):
		return false
	}
}

func (c *Client) Connect(cp ConnectionProvider) error {
	conn, err := c.connect(cp)
	if err != nil {
		return err
	}

	go c.serveConnections(conn, cp)
	go c.dispatchMessages()

	if c.ConnectedCallback != nil {
		go c.ConnectedCallback()
	}

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

	c.Lock()
	c.subscriptions[id] = &Subscription{
		Subject:  subject,
		ID:       id,
		Callback: callback,
	}
	c.Unlock()

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

func (c *Client) connect(cp ConnectionProvider) (conn *Connection, err error) {
	conn, err = cp.ProvideConnection()
	if err != nil {
		return
	}

	conn.SetLogger(c.Logger)

	return
}

func (c *Client) serveConnections(conn *Connection, cp ConnectionProvider) {
	var err error

	// serve connection until disconnected
	for stop := false; !stop; {
		select {
		case <-conn.Disconnected:
			c.Logger.Warn("client.connection.disconnected")
			conn.Close()
			stop = true

		case c.connection <- conn:
			c.Logger.Debug("client.connection.served")
		}
	}

	// stop if client was told to disconnect
	if c.disconnecting {
		c.Logger.Info("client.disconnecting")
		return
	}

	// acquire new connection
	for {
		c.Logger.Debug("client.reconnect.starting")

		conn, err = c.connect(cp)
		if err == nil {
			go c.serveConnections(conn, cp)
			c.Logger.Debug("client.connection.resubscribing")
			c.resubscribe(conn)
			c.Logger.Debug("client.connection.resubscribed")

			if c.ConnectedCallback != nil {
				go c.ConnectedCallback()
			}
			break
		}

		c.Logger.Warnd(map[string]interface{}{"error": err.Error()}, "client.reconnect.failed")

		time.Sleep(500 * time.Millisecond)
	}
}

func (c *Client) resubscribe(conn *Connection) error {
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
		msg, ok := <-conn.MSGs
		if !ok {
			continue
		}

		c.Lock()
		sub := c.subscriptions[msg.SubID]
		c.Unlock()
		if sub == nil {
			continue
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
