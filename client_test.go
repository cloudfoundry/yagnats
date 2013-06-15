package yagnats

import (
	. "launchpad.net/gocheck"
	"testing"
	"time"
)

func Test(t *testing.T) { TestingT(t) }

type YSuite struct{}

var _ = Suite(&YSuite{})

func (s *YSuite) SetUpSuite(c *C) {
}

func (s *YSuite) TestDialWithValidAddress(c *C) {
	client, err := Dial("127.0.0.1:4222")
	c.Assert(err, Equals, nil)
	c.Assert(client, Not(Equals), nil)
}

func (s *YSuite) TestDialWithInvalidAddress(c *C) {
	_, err := Dial("")
	c.Assert(err, Not(Equals), nil)
}

func (s *YSuite) TestClientConnect(c *C) {
	client, _ := Dial("127.0.0.1:4222")
	c.Assert(client.Connect("foo", "bar"), Equals, nil)
}

func (s *YSuite) TestClientConnectWithInvalidAuth(c *C) {
	client, _ := Dial("127.0.0.1:4222")

	err := client.Connect("foo", "invalid")

	c.Assert(err, Not(Equals), nil)
	c.Assert(err.Error(), Equals, "Authorization failed")
}

func (s *YSuite) TestClientPing(c *C) {
	client, _ := Dial("127.0.0.1:4222")
	client.Connect("foo", "bar")
	c.Assert(client.Ping(), Equals, &PongPacket{})
}

func (s *YSuite) TestClientPubSub(c *C) {
	client, _ := Dial("127.0.0.1:4222")
	client.Connect("foo", "bar")

	payload := make(chan string)

	client.Subscribe("some.subject", func(msg *Message) {
		payload <- msg.Payload
	})

	client.Publish("some.subject", "hello!")

	select {
	case msg := <-payload:
		c.Assert(msg, Equals, "hello!")
	case <-time.After(500 * time.Millisecond):
		c.Error("Timed out waiting for message.")
	}
}
