package yagnats

import (
	"bytes"
	. "launchpad.net/gocheck"
	"net"
	"testing"
	"time"
)

func Test(t *testing.T) { TestingT(t) }

type YSuite struct {
	Client *Client
}

var _ = Suite(&YSuite{})

func (s *YSuite) SetUpTest(c *C) {
	client, err := Dial("127.0.0.1:4222")
	if err != nil {
		c.Error("Cannot contact NATS at 127.0.0.1:4222")
	}

	s.Client = client
}

func (s *YSuite) TearDownTest(c *C) {
	s.Client = nil
}

func (s *YSuite) TestDialWithValidAddress(c *C) {
	c.Assert(s.Client, Not(Equals), nil)
}

func (s *YSuite) TestDialWithInvalidAddress(c *C) {
	_, err := Dial("")
	c.Assert(err, Not(Equals), nil)
	c.Assert(err.Error(), Equals, "dial tcp: missing address")
}

func (s *YSuite) TestClientConnect(c *C) {
	c.Assert(s.Client.Connect("foo", "bar"), Equals, nil)
}

func (s *YSuite) TestClientConnectWithInvalidAuth(c *C) {
	err := s.Client.Connect("foo", "invalid")

	c.Assert(err, Not(Equals), nil)
	c.Assert(err.Error(), Equals, "Authorization failed")
}

func (s *YSuite) TestClientPing(c *C) {
	s.Client.Connect("foo", "bar")
	c.Assert(s.Client.Ping(), Equals, &PongPacket{})
}

func (s *YSuite) TestClientSubscribe(c *C) {
	s.Client.Connect("foo", "bar")

	sub := s.Client.Subscribe("some.subject", func(msg *Message) {})
	c.Assert(sub, Equals, 1)

	sub2 := s.Client.Subscribe("some.subject", func(msg *Message) {})
	c.Assert(sub2, Equals, 2)
}

func (s *YSuite) TestClientUnsubscribe(c *C) {
	s.Client.Connect("foo", "bar")

	payload1 := make(chan string)
	payload2 := make(chan string)

	sid1 := s.Client.Subscribe("some.subject", func(msg *Message) {
		payload1 <- msg.Payload
	})

	s.Client.Subscribe("some.subject", func(msg *Message) {
		payload2 <- msg.Payload
	})

	s.Client.Publish("some.subject", "hello!")

	waitReceive(c, "hello!", payload1, 500)
	waitReceive(c, "hello!", payload2, 500)

	s.Client.Unsubscribe(sid1)

	s.Client.Publish("some.subject", "hello!")

	select {
	case <-payload1:
		c.Error("Should not have received message.")
	case <-time.After(500 * time.Millisecond):
	}

	waitReceive(c, "hello!", payload2, 500)
}

func (s *YSuite) TestClientUnsubscribeAll(c *C) {
	s.Client.Connect("foo", "bar")

	payload := make(chan string)

	s.Client.Subscribe("some.subject", func(msg *Message) {
		payload <- msg.Payload
	})

	s.Client.Publish("some.subject", "hello!")

	waitReceive(c, "hello!", payload, 500)

	s.Client.UnsubscribeAll("some.subject")

	s.Client.Publish("some.subject", "hello!")

	select {
	case <-payload:
		c.Error("Should not have received message.")
	case <-time.After(500 * time.Millisecond):
	}
}

func (s *YSuite) TestClientPubSub(c *C) {
	s.Client.Connect("foo", "bar")

	payload := make(chan string)

	s.Client.Subscribe("some.subject", func(msg *Message) {
		payload <- msg.Payload
	})

	s.Client.Publish("some.subject", "hello!")

	waitReceive(c, "hello!", payload, 500)
}

func (s *YSuite) TestClientPong(c *C) {
	conn := &fakeConn{
		Buffer:    bytes.NewBuffer([]byte("PING\r\n")),
		Received:  bytes.NewBuffer([]byte{}),
		WriteChan: make(chan string),
	}

	s.Client = NewClient(conn)

	time.Sleep(1 * time.Second)

	waitReceive(c, "PONG\r\n", conn.WriteChan, 500)
}

func waitReceive(c *C, expected string, from chan string, ms time.Duration) {
	select {
	case msg := <-from:
		c.Assert(msg, Equals, expected)
	case <-time.After(ms * time.Millisecond):
		c.Error("Timed out waiting for message.")
	}
}

type fakeConn struct {
	Buffer    *bytes.Buffer
	Received  *bytes.Buffer
	WriteChan chan string
}

func (f *fakeConn) Read(b []byte) (n int, err error) {
	return f.Buffer.Read(b)
}

func (f *fakeConn) Write(b []byte) (n int, err error) {
	f.WriteChan <- string(b)
	return f.Received.Write(b)
}

func (f *fakeConn) Close() error {
	return nil
}

func (f *fakeConn) SetDeadline(time.Time) error {
	return nil
}

func (f *fakeConn) SetReadDeadline(time.Time) error {
	return nil
}

func (f *fakeConn) SetWriteDeadline(time.Time) error {
	return nil
}

func (f *fakeConn) LocalAddr() net.Addr {
	addr, _ := net.ResolveTCPAddr("tcp", "0.0.0.0:4222")
	return addr
}

func (f *fakeConn) RemoteAddr() net.Addr {
	addr, _ := net.ResolveTCPAddr("tcp", "0.0.0.0:65525")
	return addr
}
