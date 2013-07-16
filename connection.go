package yagnats

import (
	"bufio"
	"errors"
	"net"
	"sync"
	"time"
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

	Logger Logger
}

type ConnectionProvider interface {
	ProvideConnection() (*Connection, error)
}

func NewConnection(addr, user, pass string) *Connection {
	return &Connection{
		addr: addr,
		user: user,
		pass: pass,

		PONGs: make(chan *PongPacket),
		OKs:   make(chan *OKPacket),
		MSGs:  make(chan *MsgPacket),

		Logger: &DefaultLogger{},

		// buffer size of 1 to account for fatal unexpected errors
		// from the server (i.e. slow consumer)
		Errs: make(chan error, 1),

		// buffer size of 1 so that read and write errors
		// can both send without blocking
		Disconnected: make(chan bool, 1),
	}
}

type ConnectionInfo struct {
	Addr     string
	Username string
	Password string
}

func (c *ConnectionInfo) ProvideConnection() (*Connection, error) {
	conn := NewConnection(c.Addr, c.Username, c.Password)

	var err error

	err = conn.Dial()
	if err != nil {
		return nil, err
	}

	err = conn.Handshake()
	if err != nil {
		return nil, err
	}

	return conn, nil
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
	c.Logger.Debug("connection.err-or-ok.wait")

	select {
	case err := <-c.Errs:
		c.Logger.Warnd(map[string]interface{}{"error": err.Error()}, "connection.err-or-ok.err")
		return err
	case <-c.OKs:
		c.Logger.Debug("connection.err-or-ok.ok")
		return nil
	}
}

func (c *Connection) Send(packet Packet) {
	c.Logger.Debugd(map[string]interface{}{"packet": packet}, "connection.packet.send")

	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	_, err := c.conn.Write(packet.Encode())
	if err != nil {
		c.Logger.Errord(map[string]interface{}{"error": err.Error()}, "connection.packet.write-error")
		c.disconnected()
	}

	return
}

func (c *Connection) Ping() bool {
	c.Send(&PingPacket{})

	select {
	case _, ok := <-c.PONGs:
		return ok
	case <-time.After(500 * time.Millisecond):
		return false
	}
}

func (c *Connection) disconnected() {
	c.Disconnected <- true
}

func (c *Connection) Close() {
	close(c.MSGs)
	close(c.PONGs)
}

func (c *Connection) receivePackets() {
	io := bufio.NewReader(c.conn)

	for {
		c.Logger.Debug("connection.packet.read")

		packet, err := Parse(io)
		if err != nil {
			c.Logger.Errord(map[string]interface{}{"error": err.Error()}, "connection.packet.read-error")
			c.disconnected()
			break
		}

		switch packet.(type) {
		case *PongPacket:
			c.Logger.Debug("connection.packet.pong-received")

			select {
			case c.PONGs <- packet.(*PongPacket):
				c.Logger.Debug("connection.packet.pong-served")
			default:
				c.Logger.Debug("connection.packet.pong-unhandled")
			}

		case *PingPacket:
			c.Logger.Debug("connection.packet.ping-received")
			c.Send(&PongPacket{})

		case *OKPacket:
			c.Logger.Debug("connection.packet.ok-received")
			c.OKs <- packet.(*OKPacket)

		case *ERRPacket:
			c.Logger.Debug("connection.packet.err-received")
			c.Errs <- errors.New(packet.(*ERRPacket).Message)

		case *InfoPacket:
			c.Logger.Debug("connection.packet.info-received")
			// noop

		case *MsgPacket:
			c.Logger.Debugd(
				map[string]interface{}{"packet": packet},
				"connection.packet.msg-received",
			)

			c.MSGs <- packet.(*MsgPacket)
		}
	}
}
