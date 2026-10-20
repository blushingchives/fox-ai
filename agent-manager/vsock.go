package main

// Host->guest invocation transport over Firecracker vsock, adapted from
// pool-manager. The key change: no fixed 30s deadline — an agent turn can run
// for minutes. The timeout is configurable (InvokeTimeout).

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"
)

const bootstrapPort = 8080

type InvocationEvent struct {
	Method  string              `json:"method"`
	Path    string              `json:"path"`
	Query   string              `json:"query"`
	Headers map[string][]string `json:"headers"`
	Body    string              `json:"body"`
}

type InvocationResponse struct {
	Status     int                 `json:"status"`
	Headers    map[string][]string `json:"headers"`
	Body       string              `json:"body"`
	InfraError bool                `json:"infra_error,omitempty"`
}

// connectVsock opens a connection to the bootstrap inside a VM via the
// Firecracker proxy protocol: CONNECT <port>\n then expect OK <port>\n.
func connectVsock(vsockPath string) (net.Conn, error) {
	conn, err := net.Dial("unix", vsockPath)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(conn, "CONNECT %d\n", bootstrapPort)
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("vsock handshake read: %w", err)
	}
	if !strings.HasPrefix(line, "OK") {
		conn.Close()
		return nil, fmt.Errorf("vsock handshake failed: %q", line)
	}
	return conn, nil
}

func waitForVsock(vsockPath string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := connectVsock(vsockPath)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for bootstrap on %s", vsockPath)
}

func invokeViaVsock(vsockPath string, event InvocationEvent, timeout time.Duration) (*InvocationResponse, error) {
	conn, err := connectVsock(vsockPath)
	if err != nil {
		return nil, fmt.Errorf("connect vsock: %w", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(timeout))
	if err := json.NewEncoder(conn).Encode(event); err != nil {
		return nil, fmt.Errorf("send event: %w", err)
	}
	var resp InvocationResponse
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	return &resp, nil
}
