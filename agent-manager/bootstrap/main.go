package main

// Agent VM init (PID 1), adapted from pool-manager/bootstrap. Differences:
//   - mounts the writable data drive (/dev/vdc) read-write at /data
//   - runs a localhost->vsock egress proxy so the Pi runtime can reach the host
//     agent-broker for model calls (the key never enters the VM)
// The invocation path (host->guest vsock on port 8080) is unchanged.

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
	"unsafe"
)

const (
	appPort        = 3000
	vsockPort      = 8080 // host -> guest invocations
	egressTCPPort  = 8081 // guest-local HTTP endpoint Pi points at
	brokerVsockPort = 9000 // guest -> host broker
	afVsock        = 40
	vmaddrCIDAny   = 0xFFFFFFFF
	vmaddrCIDHost  = 2
)

type sockaddrVM struct {
	Family    uint16
	Reserved1 uint16
	Port      uint32
	CID       uint32
	Zero      [4]byte
}

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

func main() {
	log.SetPrefix("[agent-bootstrap] ")
	log.SetFlags(log.Ltime)

	mustMount("proc", "/proc", "proc")
	mustMount("sysfs", "/sys", "sysfs")
	mustMount("devtmpfs", "/dev", "devtmpfs")
	mustMount("tmpfs", "/tmp", "tmpfs")

	if err := bringUpLoopback(); err != nil {
		log.Printf("loopback: %v (continuing)", err)
	}

	// Code drive (Pi runtime image) — read-only at /var/task.
	os.MkdirAll("/var/task", 0755)
	if err := syscall.Mount("/dev/vdb", "/var/task", "ext4", syscall.MS_RDONLY, "noload"); err != nil {
		log.Fatalf("mount code drive: %v", err)
	}
	log.Println("code drive mounted at /var/task")

	// Data drive (per-agent writable ext4) — read-write at /data.
	os.MkdirAll("/data", 0755)
	if err := syscall.Mount("/dev/vdc", "/data", "ext4", 0, ""); err != nil {
		log.Fatalf("mount data drive: %v", err)
	}
	log.Println("data drive mounted at /data")

	// Egress proxy: localhost:8081 -> host broker over vsock. Pi points its
	// Anthropic base URL here; the real key lives on the host side.
	go serveEgressProxy()

	// Start the runtime app.
	app := exec.Command("/usr/local/bin/node", "/var/task/index.js")
	// Point BOTH provider base URLs at the egress proxy. The base rootfs is
	// shared across engine builds, so it can't know which provider this image
	// uses; whichever one Pi calls, it funnels through the host broker, which
	// injects the real key.
	app.Env = append(os.Environ(),
		fmt.Sprintf("PORT=%d", appPort),
		fmt.Sprintf("ANTHROPIC_BASE_URL=http://127.0.0.1:%d", egressTCPPort),
		fmt.Sprintf("OPENAI_BASE_URL=http://127.0.0.1:%d/v1", egressTCPPort),
		"DATA_DIR=/data",
		"PATH=/usr/local/bin:/usr/bin:/bin",
	)
	app.Stdout = os.Stdout
	app.Stderr = os.Stderr
	if err := app.Start(); err != nil {
		log.Fatalf("start app: %v", err)
	}
	log.Printf("app started (pid %d), waiting for ready...", app.Process.Pid)
	if err := waitForApp(appPort, 30*time.Second); err != nil {
		log.Fatalf("app not ready: %v", err)
	}
	log.Println("app ready")

	fd, err := listenVsock(vsockPort)
	if err != nil {
		log.Fatalf("listen vsock port %d: %v", vsockPort, err)
	}
	log.Printf("listening for invocations on vsock port %d", vsockPort)

	go func() {
		app.Wait()
		log.Printf("app process died — closing vsock listener")
		syscall.Close(fd)
	}()

	for {
		connFd, _, errno := syscall.Syscall(syscall.SYS_ACCEPT, uintptr(fd), 0, 0)
		if errno != 0 {
			log.Printf("accept: %v", errno)
			continue
		}
		go handleInvocation(int(connFd))
	}
}

// serveEgressProxy accepts local TCP connections and relays each to the host
// broker over vsock (CID 2, port 9000). Raw byte relay works for both streaming
// and non-streaming HTTP.
func serveEgressProxy() {
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", egressTCPPort))
	if err != nil {
		log.Fatalf("egress proxy listen: %v", err)
	}
	log.Printf("egress proxy on 127.0.0.1:%d -> host vsock %d", egressTCPPort, brokerVsockPort)
	for {
		c, err := ln.Accept()
		if err != nil {
			log.Printf("egress accept: %v", err)
			continue
		}
		go proxyToBroker(c)
	}
}

func proxyToBroker(tcpConn net.Conn) {
	defer tcpConn.Close()
	vfd, err := dialVsockHost(brokerVsockPort)
	if err != nil {
		log.Printf("dial broker vsock: %v", err)
		return
	}
	vconn := fdToConn(vfd)
	defer vconn.Close()

	done := make(chan struct{}, 2)
	go func() { io.Copy(vconn, tcpConn); done <- struct{}{} }()
	go func() { io.Copy(tcpConn, vconn); done <- struct{}{} }()
	<-done
}

// dialVsockHost opens an AF_VSOCK connection to the host (CID 2) on port.
func dialVsockHost(port uint32) (int, error) {
	fd, _, errno := syscall.RawSyscall(syscall.SYS_SOCKET, afVsock, syscall.SOCK_STREAM, 0)
	if errno != 0 {
		return 0, fmt.Errorf("socket: %w", errno)
	}
	sa := sockaddrVM{Family: afVsock, Port: port, CID: vmaddrCIDHost}
	_, _, errno = syscall.RawSyscall(syscall.SYS_CONNECT, fd,
		uintptr(unsafe.Pointer(&sa)), unsafe.Sizeof(sa))
	if errno != 0 {
		syscall.Close(int(fd))
		return 0, fmt.Errorf("connect: %w", errno)
	}
	return int(fd), nil
}

func handleInvocation(fd int) {
	conn := fdToConn(fd)
	defer conn.Close()

	var event InvocationEvent
	if err := json.NewDecoder(conn).Decode(&event); err != nil {
		log.Printf("decode event: %v", err)
		return
	}

	url := fmt.Sprintf("http://127.0.0.1:%d%s", appPort, event.Path)
	if event.Query != "" {
		url += "?" + event.Query
	}
	var bodyReader io.Reader
	if event.Body != "" {
		bodyReader = strings.NewReader(event.Body)
	}
	req, err := http.NewRequest(event.Method, url, bodyReader)
	if err != nil {
		sendError(conn, 500, err.Error())
		return
	}
	for k, vals := range event.Headers {
		for _, v := range vals {
			req.Header.Add(k, v)
		}
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		sendError(conn, 502, err.Error())
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	json.NewEncoder(conn).Encode(InvocationResponse{
		Status:  resp.StatusCode,
		Headers: map[string][]string(resp.Header),
		Body:    string(body),
	})
}

func sendError(conn net.Conn, status int, msg string) {
	log.Printf("error %d: %s", status, msg)
	json.NewEncoder(conn).Encode(InvocationResponse{Status: status, Body: msg, InfraError: true})
}

func listenVsock(port uint32) (int, error) {
	fd, _, errno := syscall.RawSyscall(syscall.SYS_SOCKET, afVsock, syscall.SOCK_STREAM, 0)
	if errno != 0 {
		return 0, fmt.Errorf("socket: %w", errno)
	}
	sa := sockaddrVM{Family: afVsock, Port: port, CID: vmaddrCIDAny}
	_, _, errno = syscall.RawSyscall(syscall.SYS_BIND, fd,
		uintptr(unsafe.Pointer(&sa)), unsafe.Sizeof(sa))
	if errno != 0 {
		syscall.Close(int(fd))
		return 0, fmt.Errorf("bind: %w", errno)
	}
	_, _, errno = syscall.RawSyscall(syscall.SYS_LISTEN, fd, 128, 0)
	if errno != 0 {
		syscall.Close(int(fd))
		return 0, fmt.Errorf("listen: %w", errno)
	}
	return int(fd), nil
}

func fdToConn(fd int) net.Conn { return &rawConn{fd: fd} }

type rawConn struct{ fd int }

func (c *rawConn) Read(b []byte) (int, error) {
	n, err := syscall.Read(c.fd, b)
	if n < 0 {
		n = 0
	}
	if n == 0 && err == nil {
		return 0, io.EOF
	}
	return n, err
}

func (c *rawConn) Write(b []byte) (int, error) {
	n, err := syscall.Write(c.fd, b)
	if n < 0 {
		n = 0
	}
	return n, err
}

func (c *rawConn) Close() error                       { return syscall.Close(c.fd) }
func (c *rawConn) LocalAddr() net.Addr                { return &net.UnixAddr{} }
func (c *rawConn) RemoteAddr() net.Addr               { return &net.UnixAddr{} }
func (c *rawConn) SetDeadline(t time.Time) error      { return nil }
func (c *rawConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *rawConn) SetWriteDeadline(t time.Time) error { return nil }

func waitForApp(port int, timeout time.Duration) error {
	url := fmt.Sprintf("http://127.0.0.1:%d/", port)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("app did not respond within %s", timeout)
}

func bringUpLoopback() error {
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, syscall.IPPROTO_IP)
	if err != nil {
		return fmt.Errorf("socket: %w", err)
	}
	defer syscall.Close(fd)

	const (
		SIOCSIFFLAGS = 0x8914
		SIOCGIFFLAGS = 0x8913
		SIOCSIFADDR  = 0x8916
	)
	type ifreqFlags struct {
		Name  [syscall.IFNAMSIZ]byte
		Flags uint16
		_     [22]byte
	}
	type ifreqAddr struct {
		Name [syscall.IFNAMSIZ]byte
		Addr syscall.RawSockaddrInet4
		_    [8]byte
	}
	flags := ifreqFlags{}
	copy(flags.Name[:], "lo")
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), SIOCGIFFLAGS, uintptr(unsafe.Pointer(&flags))); errno != 0 {
		return fmt.Errorf("SIOCGIFFLAGS: %w", errno)
	}
	flags.Flags |= syscall.IFF_UP | syscall.IFF_RUNNING
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), SIOCSIFFLAGS, uintptr(unsafe.Pointer(&flags))); errno != 0 {
		return fmt.Errorf("SIOCSIFFLAGS: %w", errno)
	}
	addr := ifreqAddr{}
	copy(addr.Name[:], "lo")
	addr.Addr.Family = syscall.AF_INET
	addr.Addr.Addr = [4]byte{127, 0, 0, 1}
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), SIOCSIFADDR, uintptr(unsafe.Pointer(&addr))); errno != 0 {
		return fmt.Errorf("SIOCSIFADDR: %w", errno)
	}
	return nil
}

func mustMount(source, target, fstype string) {
	if err := syscall.Mount(source, target, fstype, 0, ""); err != nil {
		log.Printf("mount %s → %s: %v (continuing)", source, target, err)
	}
}
