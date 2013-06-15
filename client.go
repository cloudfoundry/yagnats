package yagnats

import (
	"bufio"
	"errors"
	"fmt"
	"net"
)

type Callback func(*Message)

type Client struct {
	writer net.Conn
	reader *bufio.Reader

	pongs chan *PongPacket
	oks   chan *OKPacket
	errs  chan *ERRPacket

	subscriptions map[int]Callback
}

type Message struct {
  Subject string
  Payload string
  ReplyTo string
}

func Dial(addr string) (client *Client, err error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	client = &Client{
		writer: conn,
		reader: bufio.NewReader(conn),
		pongs:  make(chan *PongPacket),
		oks:    make(chan *OKPacket),
		errs:   make(chan *ERRPacket),
    subscriptions: make(map[int]Callback),
	}

	return
}

func (c *Client) Ping() *PongPacket {
	c.sendPacket(&PingPacket{})
	return <-c.pongs
}

func (c *Client) Connect(user, pass string) error {
	go c.handlePackets()

	c.sendPacket(&ConnectPacket{User: user, Pass: pass})

	select {
	case <-c.oks:
		return nil
	case err := <-c.errs:
		return errors.New(err.Message)
	}
}

func (c *Client) Publish(subject, payload string) (err error) {
	_, err = c.sendPacket(
		&PubPacket{
			Subject: subject,
			Payload: payload,
		},
	)

	return
}

func (c *Client) Subscribe(subject string, callback Callback) (err error) {
  id := len(c.subscriptions)
  c.subscriptions[id] = callback

	_, err = c.sendPacket(
		&SubPacket{
			Subject: subject,
			ID:      id,
		},
	)

	return
}

func (c *Client) sendPacket(packet Packet) (int, error) {
	return c.writer.Write(packet.Encode())
}

func (c *Client) handlePackets() {
	for {
		packet, err := Parse(c.reader)
		if err != nil {
		  // TODO
			fmt.Printf("ERROR! %s\n", err)
			break
		}

		switch packet.(type) {
		// TODO: inelegant
		case *PongPacket:
			select {
			case c.pongs <- packet.(*PongPacket):
			default:
			}
		// TODO: inelegant
		case *OKPacket:
			select {
			case c.oks <- packet.(*OKPacket):
			default:
			}
		// TODO: inelegant
		case *ERRPacket:
			select {
			case c.errs <- packet.(*ERRPacket):
			default:
			}
		case *InfoPacket:
		case *MsgPacket:
      msg := packet.(*MsgPacket)
		  c.subscriptions[msg.SubID](
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
