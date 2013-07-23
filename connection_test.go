package yagnats

import (
	"bytes"
	. "launchpad.net/gocheck"
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
		WriteChan:   make(chan string),
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
		WriteChan:   make(chan string),
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
		WriteChan:   make(chan string),
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
		WriteChan:   make(chan string),
		Closed:      false,
	}

	// fill in a fake connection
	s.Connection.conn = conn
	go s.Connection.receivePackets()

	s.Connection.Disconnect()

	c.Assert(conn.Closed, Equals, true)
}

func (s *CSuite) TestConnectionLogging(c *C) {
	logger := &DefaultLogger{}
	s.Connection.SetLogger(logger)
	c.Assert(s.Connection.GetLogger(), Equals, logger)
}

func (s *CSuite) TestLockingOfGetLogger(c *C) {
	resultChan := make(chan Logger)

	s.Connection.Lock()
	defer s.Connection.Unlock()

	go func() {
		resultChan <- s.Connection.GetLogger()
	}()

	select {
	case <-resultChan:
		c.Error("There should be a lock, but there is none!")
	case <-time.After(1*time.Second):
	}
}

func (s *CSuite) TestReadLockingOfGetLogger(c *C) {
	resultChan := make(chan Logger)

	s.Connection.RLock()
	defer s.Connection.RUnlock()

	go func() {
		resultChan <- s.Connection.GetLogger()
	}()

	select {
	case <-resultChan:
	case <-time.After(1*time.Second):
		c.Error("We should be able to read the logger, but are not!")
	}
}
