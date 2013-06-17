package yagnats

import (
	"bufio"
	"errors"
	"net"
	"sync"
)

type Connection struct {
	conn net.Conn

	addr string
	user string
	pass string

	writeLock sync.Mutex

	PONGs chan *PongPacket
	OKs   chan *OKPacket
	MSGs  chan *MsgPacket
	Errs  chan error

	Disconnected chan bool
}

func NewConnection(addr, user, pass string) *Connection {
	return &Connection{
		addr: addr,
		user: user,
		pass: pass,

		PONGs: make(chan *PongPacket),
		OKs:   make(chan *OKPacket),
		MSGs:  make(chan *MsgPacket),
		Errs:  make(chan error),

		Disconnected: make(chan bool),
	}
}

func (c *Connection) Dial() error {
	conn, err := net.Dial("tcp", c.addr)
	if err != nil {
		return err
	}

	c.conn = conn

	go c.receivePackets()

	return nil
}

func (c *Connection) Handshake() error {
	c.Send(&ConnectPacket{User: c.user, Pass: c.pass})
	return c.ErrOrOK()
}

func (c *Connection) Disconnect() {
	c.conn.Close()
}

func (c *Connection) ErrOrOK() error {
	select {
	case err := <-c.Errs:
		return err
	case <-c.OKs:
		return nil
	}
}

func (c *Connection) Send(packet Packet) {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	_, err := c.conn.Write(packet.Encode())
	if err != nil {
		c.disconnected()
	}

	return
}

func (c *Connection) disconnected() {
	c.Disconnected <- true
	close(c.MSGs)
	close(c.PONGs)
}

func (c *Connection) receivePackets() {
	io := bufio.NewReader(c.conn)

	for {
		packet, err := Parse(io)
		if err != nil {
			c.disconnected()
			break
		}

		switch packet.(type) {
		case *PongPacket:
			c.PONGs <- packet.(*PongPacket)

		case *PingPacket:
			c.Send(&PongPacket{})

		case *OKPacket:
			c.OKs <- packet.(*OKPacket)

		case *ERRPacket:
			c.Errs <- errors.New(packet.(*ERRPacket).Message)

		case *InfoPacket:
			// noop

		case *MsgPacket:
			c.MSGs <- packet.(*MsgPacket)
		}
	}
}
