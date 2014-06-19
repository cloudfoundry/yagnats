package yagnats

import (
	"bytes"
	"errors"
	. "launchpad.net/gocheck"
	"net"
	"runtime"
	"time"
)

type CSuite struct {
	Connection *Connection
}

var _ = Suite(&CSuite{})

func (s *CSuite) SetUpTest(c *C) {
	s.Connection = NewConnection("foo", "bar", "baz")
}

func (s *CSuite) TestConnectionPong(c *C) {
	conn := &fakeConn{
		ReadBuffer:  bytes.NewBuffer([]byte("PING\r\n")),
		WriteBuffer: bytes.NewBuffer([]byte{}),
		WriteChan:   make(chan []byte),
	}

	// fill in a fake connection
	s.Connection.conn = conn
	go s.Connection.receivePackets()

	time.Sleep(1 * time.Second)

	waitReceive(c, "PONG\r\n", conn.WriteChan, 500)
}

func (s *CSuite) TestConnectionUnexpectedError(c *C) {
	conn := &fakeConn{
		ReadBuffer:  bytes.NewBuffer([]byte("-ERR 'foo'\r\nPING\r\n")),
		WriteBuffer: bytes.NewBuffer([]byte{}),
		WriteChan:   make(chan []byte),
	}

	// fill in a fake connection
	s.Connection.conn = conn
	go s.Connection.receivePackets()

	time.Sleep(1 * time.Second)

	waitReceive(c, "PONG\r\n", conn.WriteChan, 500)
}

func (s *CSuite) TestConnectionUnexpectedPong(c *C) {
	conn := &fakeConn{
		ReadBuffer:  bytes.NewBuffer([]byte("PONG\r\nPING\r\n")),
		WriteBuffer: bytes.NewBuffer([]byte{}),
		WriteChan:   make(chan []byte),
	}

	// fill in a fake connection
	s.Connection.conn = conn
	go s.Connection.receivePackets()

	time.Sleep(1 * time.Second)

	waitReceive(c, "PONG\r\n", conn.WriteChan, 500)
}

func (s *CSuite) TestConnectionDisconnect(c *C) {
	conn := &fakeConn{
		ReadBuffer:  bytes.NewBuffer([]byte{}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
		WriteChan:   make(chan []byte),
		Closed:      false,
	}

	// fill in a fake connection
	s.Connection.conn = conn
	go s.Connection.receivePackets()

	s.Connection.Disconnect()

	c.Assert(conn.Closed, Equals, true)
}

func (s *CSuite) TestConnectionErrOrOKReturnsErrorOnDisconnect(c *C) {
	conn := &fakeConn{
		ReadBuffer:  bytes.NewBuffer([]byte{}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
		WriteChan:   make(chan []byte),
	}

	// fill in a fake connection
	s.Connection.conn = conn

	errOrOK := make(chan error)

	go func() {
		errOrOK <- s.Connection.ErrOrOK()
	}()

	go s.Connection.receivePackets()

	select {
	case <-s.Connection.Disconnected:
	case <-time.After(1 * time.Second):
		c.Error("Connection never disconnected.")
	}

	select {
	case err := <-errOrOK:
		c.Assert(err, ErrorMatches, "disconnected")
	case <-time.After(1 * time.Second):
		c.Error("Never received result from ErrOrOK.")
	}
}

func (s *CSuite) TestConnectionOnMessageCallback(c *C) {
	conn := &fakeConn{
		ReadBuffer:  bytes.NewBuffer([]byte("MSG foo 1 5\r\nhello\r\n")),
		WriteBuffer: bytes.NewBuffer([]byte{}),
		WriteChan:   make(chan []byte),
		Closed:      false,
	}

	// fill in a fake connection
	s.Connection.conn = conn

	messages := make(chan *MsgPacket)

	s.Connection.OnMessage(func(msg *MsgPacket) {
		messages <- msg
	})

	go s.Connection.receivePackets()

	select {
	case msg := <-messages:
		c.Assert(msg.SubID, Equals, int64(1))
		c.Assert(string(msg.Payload), Equals, "hello")
	case <-time.After(1 * time.Second):
		c.Error("Did not receive message.")
	}
}

func (s *CSuite) TestConnectionClusterReconnectsToRandomNode(c *C) {
	hellos := 0
	goodbyes := 0

	for i := 0; i < 100; i++ {
		node1 := &FakeConnectionProvider{
			ReadBuffer:  "+OK\r\nMSG foo 1 5\r\nhello\r\n",
			WriteBuffer: []byte{},
		}

		node2 := &FakeConnectionProvider{
			ReadBuffer:  "+OK\r\nMSG foo 1 7\r\ngoodbye\r\n",
			WriteBuffer: []byte{},
		}

		cluster := &ConnectionCluster{[]ConnectionProvider{node1, node2}}

		conn, err := cluster.ProvideConnection()
		c.Assert(err, IsNil)

		conn.OnMessage(func(msg *MsgPacket) {
			if string(msg.Payload) == "hello" {
				hellos += 1
			}

			if string(msg.Payload) == "goodbye" {
				goodbyes += 1
			}
		})

		conn.ErrOrOK()
	}

	c.Assert(hellos, Not(Equals), 0)
	c.Assert(goodbyes, Not(Equals), 0)
}

//This hacky test was derived from http://golang.org/src/pkg/net/dial_test.go#TestDialTimeout
func (s *CSuite) TestConnectionTimeout(c *C) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	errc := make(chan error)
	switch runtime.GOOS {
	case "linux":
		for i := 0; i < 1000; i++ {
			go func() {
				conn := NewConnection(ln.Addr().String(), "nats", "nats")
				err := conn.Dial()
				errc <- err
			}()
		}
	case "darwin", "plan9", "windows":
		go func() {
			conn := NewConnection("127.0.71.111:49151", "nats", "nats")
			err := conn.Dial()
			if err == nil {
				err = errors.New("unexpected: connected to 127.0.71.111:49151")
				conn.Disconnect()
			}
			errc <- err
		}()
	default:
		c.Skip("skipping test on " + runtime.GOOS)
	}

	connected := 0
	for {
		select {
		case <-time.After(15 * time.Second):
			c.Fatal("too slow")
		case err := <-errc:
			if err == nil {
				connected++
				if connected == 1000 {
					c.Fatal("all connections connected; expected some to time out")
				}
			} else {
				timeout := err.(*net.OpError).Timeout()
				if !timeout {
					c.Fatalf("got error %q; not a timeout", err)
				}
				// Pass. We saw a timeout error.
				return
			}
		}
	}
}
