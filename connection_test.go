package yagnats

import (
	"bytes"
	. "launchpad.net/gocheck"
	"net"
	"time"
)

type CSuite struct {
	Connection *Connection
}

var _ = Suite(&CSuite{})

func (s *CSuite) TestConnectionPong(c *C) {
	conn := &fakeConn{
		Buffer:    bytes.NewBuffer([]byte("PING\r\n")),
		Received:  bytes.NewBuffer([]byte{}),
		WriteChan: make(chan string),
	}

	s.Connection = &Connection{}

	// fill in a fake connection
	s.Connection.conn = conn
	go s.Connection.receivePackets()

	time.Sleep(1 * time.Second)

	waitReceive(c, "PONG\r\n", conn.WriteChan, 500)
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
