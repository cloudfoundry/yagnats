package yagnats

import (
	. "launchpad.net/gocheck"
	"os/exec"
	"testing"
	"time"
)

func Test(t *testing.T) { TestingT(t) }

type YSuite struct {
	Client  *Client
	NatsCmd *exec.Cmd
}

var _ = Suite(&YSuite{})

func (s *YSuite) SetUpSuite(c *C) {
	s.NatsCmd = startNats(4223)
	waitUntilNatsUp(4223)
}

func (s *YSuite) TearDownSuite(c *C) {
	stopNats(s.NatsCmd)
}

func (s *YSuite) SetUpTest(c *C) {
	client := NewClient()

	client.Connect("127.0.0.1:4223", "nats", "nats")

	s.Client = client
}

func (s *YSuite) TearDownTest(c *C) {
	s.Client.Disconnect()
	s.Client = nil
}

func (s *YSuite) TestConnectWithInvalidAddress(c *C) {
	badClient := NewClient()

	err := badClient.Connect("", "cats", "bats")

	c.Assert(err, Not(Equals), nil)
	c.Assert(err.Error(), Equals, "dial tcp: missing address")
}

func (s *YSuite) TestClientConnectWithInvalidAuth(c *C) {
	badClient := NewClient()

	err := badClient.Connect("127.0.0.1:4223", "cats", "bats")

	c.Assert(err, Not(Equals), nil)
	c.Assert(err.Error(), Equals, "Authorization failed")
}

func (s *YSuite) TestClientPing(c *C) {
	c.Assert(s.Client.Ping(), Equals, &PongPacket{})
}

func (s *YSuite) TestClientSubscribe(c *C) {
	sub, _ := s.Client.Subscribe("some.subject", func(msg *Message) {})
	c.Assert(sub, Equals, 1)

	sub2, _ := s.Client.Subscribe("some.subject", func(msg *Message) {})
	c.Assert(sub2, Equals, 2)
}

func (s *YSuite) TestClientUnsubscribe(c *C) {
	payload1 := make(chan string)
	payload2 := make(chan string)

	sid1, _ := s.Client.Subscribe("some.subject", func(msg *Message) {
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

func (s *YSuite) TestClientUnsubscribeInvalid(c *C) {
	err := s.Client.Unsubscribe(42)

	c.Assert(err, Not(Equals), nil)
	c.Assert(err.Error(), Equals, "Invalid Subject-Identifier (sid), no subscriber registered")
}

func (s *YSuite) TestClientSubscribeAndUnsubscribe(c *C) {
	payload := make(chan string)

	sid1, _ := s.Client.Subscribe("some.subject", func(msg *Message) {
		payload <- msg.Payload
	})

	s.Client.Publish("some.subject", "hello!")

	waitReceive(c, "hello!", payload, 500)

	s.Client.Unsubscribe(sid1)

	s.Client.Subscribe("some.subject", func(msg *Message) {
		payload <- msg.Payload
	})

	s.Client.Publish("some.subject", "hello!")

	waitReceive(c, "hello!", payload, 500)

	select {
	case <-payload:
		c.Error("Should not have received message.")
	case <-time.After(500 * time.Millisecond):
	}
}

func (s *YSuite) TestClientAutoResubscribe(c *C) {
	doomedNats := startNats(4213)
	defer stopNats(doomedNats)

	durableClient := NewClient()
	durableClient.Connect("127.0.0.1:4213", "nats", "nats")

	payload := make(chan string)

	durableClient.Subscribe("some.subject", func(msg *Message) {
		payload <- msg.Payload
	})

	stopNats(doomedNats)
	waitUntilNatsDown(4213)
	doomedNats = startNats(4213)
	waitUntilNatsUp(4213)

	durableClient.Publish("some.subject", "hello!")

	waitReceive(c, "hello!", payload, 500)
}

func (s *YSuite) TestClientPublishTooBig(c *C) {
	payload := make([]byte, 10240000)
	err := s.Client.Publish("foo", string(payload))

	c.Assert(err, Not(Equals), nil)
	c.Assert(err.Error(), Equals, "Payload size exceeded")
}

func (s *YSuite) TestClientPublishTooBigRecoverable(c *C) {
	payload := make([]byte, 10240000)

	err := s.Client.Publish("foo", string(payload))

	c.Assert(err, Not(Equals), nil)
	c.Assert(err.Error(), Equals, "Payload size exceeded")

	err = s.Client.Publish("some.publish", "bar")

	c.Assert(err, Equals, nil)
}

func (s *YSuite) TestClientSubscribeInvalidSubject(c *C) {
	sid, err := s.Client.Subscribe(">.a", func(msg *Message) {})

	c.Assert(err, Not(Equals), nil)
	c.Assert(err.Error(), Equals, "Invalid Subject")
	c.Assert(sid, Equals, -1)
}

func (s *YSuite) TestClientUnsubscribeAll(c *C) {
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
	payload := make(chan string)

	s.Client.Subscribe("some.subject", func(msg *Message) {
		payload <- msg.Payload
	})

	s.Client.Publish("some.subject", "hello!")

	waitReceive(c, "hello!", payload, 500)
}

func (s *YSuite) TestClientPublishWithReply(c *C) {
	payload := make(chan string)

	s.Client.Subscribe("some.request", func(msg *Message) {
		s.Client.Publish(msg.ReplyTo, "response!")
	})

	s.Client.Subscribe("some.reply", func(msg *Message) {
		payload <- msg.Payload
	})

	s.Client.PublishWithReplyTo("some.request", "hello!", "some.reply")

	waitReceive(c, "response!", payload, 500)
}

func (s *YSuite) TestClientDisconnect(c *C) {
	payload := make(chan string)

	s.Client.Subscribe("some.subject", func(msg *Message) {
		payload <- msg.Payload
	})

	s.Client.Disconnect()

	otherClient := NewClient()
	otherClient.Connect("127.0.0.1:4223", "nats", "nats")
	otherClient.Publish("some.subject", "hello!")

	select {
	case <-payload:
		c.Error("Should not have received message.")
	case <-time.After(500 * time.Millisecond):
	}
}

func waitReceive(c *C, expected string, from chan string, ms time.Duration) {
	select {
	case msg := <-from:
		c.Assert(msg, Equals, expected)
	case <-time.After(ms * time.Millisecond):
		c.Error("Timed out waiting for message.")
	}
}
