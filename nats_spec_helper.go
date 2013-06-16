package yagnats

import (
	"errors"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"time"
)

func startNats(port int) *exec.Cmd {
	cmd := exec.Command("nats-server", "-p", strconv.Itoa(port), "--user", "nats", "--pass", "nats")
	err := cmd.Start()
	if err != nil {
		fmt.Printf("NATS failed to start: %v\n", err)
	}
	err = waitUntilNatsUp(port)
	if err != nil {
		panic("Cannot connect to NATS")
	}
	return cmd
}

func stopNats(cmd *exec.Cmd) {
	cmd.Process.Kill()
	cmd.Wait()
}

func waitUntilNatsUp(port int) error {
	maxWait := 10
	for i := 0; i < maxWait; i++ {
		time.Sleep(500 * time.Millisecond)
		_, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			return nil
		}
	}
	return errors.New("Waited too long for NATS to start")
}

func waitUntilNatsDown(port int) error {
	maxWait := 10
	for i := 0; i < maxWait; i++ {
		time.Sleep(500 * time.Millisecond)
		_, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			return nil
		}
	}
	return errors.New("Waited too long for NATS to stop")
}
