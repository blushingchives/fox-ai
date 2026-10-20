package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

var ErrAgentUnknown = errors.New("agent not found")

type Config struct {
	KernelPath      string
	BaseRootfs      string // agent base rootfs (OS + Node + agent bootstrap)
	RuntimeDir      string // dir holding engine build images: <runtime>.ext4
	DataDir         string // where per-storage writable ext4 drives live
	FirecrackerBin  string
	BrokerControlURL string // e.g. http://127.0.0.1:8090
	DataDriveSizeMB int
	VcpuCount       int
	MemMiB          int
	IdleTimeout     time.Duration
	InvokeTimeout   time.Duration
	LeaseTTL        time.Duration
	Host            string // this host's id, for the lease
}

// agentVM is the single sticky VM for one agent. Unlike the function pool, it
// accepts CONCURRENT invocations (a /message can steer an in-flight /tick) —
// the in-VM runtime serializes turns, not the manager.
type agentVM struct {
	agentID    string
	userID     string
	storageID  string // the leased storage this VM has mounted
	runtime    string // engine build booted (e.g. "claude", "chatgpt")
	pid        int
	socketPath string
	vsockPath  string
	dataPath   string
	lastUsedNs atomic.Int64
}

func (vm *agentVM) touch() { vm.lastUsedNs.Store(time.Now().UnixNano()) }

type Manager struct {
	mu        sync.Mutex
	vms       map[string]*agentVM
	bootLocks map[string]*sync.Mutex
	store     *Store
	cfg       Config
}

func NewManager(store *Store, cfg Config) *Manager {
	m := &Manager{
		vms:       make(map[string]*agentVM),
		bootLocks: make(map[string]*sync.Mutex),
		store:     store,
		cfg:       cfg,
	}
	os.MkdirAll(cfg.DataDir, 0755)
	go m.idleSweeper()
	return m
}

func (m *Manager) bootLock(agentID string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	if l, ok := m.bootLocks[agentID]; ok {
		return l
	}
	l := &sync.Mutex{}
	m.bootLocks[agentID] = l
	return l
}

// getVM returns the live VM for an agent, booting one if needed. Concurrent
// callers for the same agent serialize on the per-agent boot lock so only one
// boot happens; callers for different agents don't contend.
func (m *Manager) getVM(agentID string) (*agentVM, error) {
	m.mu.Lock()
	if vm, ok := m.vms[agentID]; ok {
		m.mu.Unlock()
		return vm, nil
	}
	m.mu.Unlock()

	bl := m.bootLock(agentID)
	bl.Lock()
	defer bl.Unlock()

	m.mu.Lock()
	if vm, ok := m.vms[agentID]; ok {
		m.mu.Unlock()
		return vm, nil
	}
	m.mu.Unlock()

	vm, err := m.bootVM(agentID)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.vms[agentID] = vm
	m.mu.Unlock()
	return vm, nil
}

func (m *Manager) bootVM(agentID string) (*agentVM, error) {
	info, found, err := m.store.LookupAgent(agentID)
	if err != nil {
		return nil, fmt.Errorf("lookup agent: %w", err)
	}
	if !found {
		return nil, ErrAgentUnknown
	}
	if info.storageID == "" {
		return nil, fmt.Errorf("agent %s has no storage attached", agentID)
	}
	runtime := info.runtime
	if runtime == "" {
		runtime = "claude"
	}
	image := filepath.Join(m.cfg.RuntimeDir, runtime+".ext4")
	if _, err := os.Stat(image); err != nil {
		return nil, fmt.Errorf("runtime image %q: %w", image, err)
	}

	id := newID()
	vm := &agentVM{
		agentID:    agentID,
		userID:     info.userID,
		storageID:  info.storageID,
		runtime:    runtime,
		socketPath: fmt.Sprintf("/tmp/fc-agt-%s.sock", id),
		vsockPath:  fmt.Sprintf("/tmp/fc-agt-%s-vsock.sock", id),
		// ext4 is keyed on STORAGE id, not agent id — that's what makes it
		// portable across engine builds.
		dataPath: filepath.Join(m.cfg.DataDir, info.storageID+".ext4"),
	}

	// Lease the STORAGE before booting. Keyed on storage so no two VMs — even
	// different engine builds pointed at the same storage — mount it at once.
	if err := m.store.AcquireStorageLease(info.storageID, m.cfg.Host, vm.vsockPath, m.cfg.LeaseTTL); err != nil {
		return nil, fmt.Errorf("acquire storage lease: %w", err)
	}
	release := func() { m.store.ReleaseStorageLease(info.storageID, m.cfg.Host) }

	if err := m.ensureDataDrive(vm.dataPath); err != nil {
		release()
		return nil, fmt.Errorf("data drive: %w", err)
	}
	pid, err := spawnFirecracker(m.cfg.FirecrackerBin, vm.socketPath)
	if err != nil {
		release()
		return nil, fmt.Errorf("spawn firecracker: %w", err)
	}
	vm.pid = pid

	if err := configureVM(vm.socketPath, m.cfg, image, vm.dataPath, vm.vsockPath); err != nil {
		killVM(vm)
		release()
		return nil, fmt.Errorf("configure VM: %w", err)
	}
	if err := startVM(vm.socketPath); err != nil {
		killVM(vm)
		release()
		return nil, fmt.Errorf("start VM: %w", err)
	}
	if err := waitForVsock(vm.vsockPath, 30*time.Second); err != nil {
		killVM(vm)
		release()
		return nil, fmt.Errorf("VM not ready: %w", err)
	}

	// Register with the broker so the agent's model calls are attributed and
	// capped. If this fails the VM can't make model calls — abort the boot.
	if err := m.brokerRegister(vm); err != nil {
		killVM(vm)
		release()
		return nil, fmt.Errorf("broker register: %w", err)
	}

	vm.touch()
	log.Printf("[%s] VM ready (pid %d, runtime=%s, storage=%s)", agentID, vm.pid, runtime, info.storageID)
	return vm, nil
}

// ensureDataDrive creates a fresh writable ext4 for a new agent. Existing drives
// are reused as-is, preserving the agent's session + workspace across evictions.
func (m *Manager) ensureDataDrive(path string) error {
	if _, err := os.Stat(path); err == nil {
		return nil // reuse existing state
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := f.Truncate(int64(m.cfg.DataDriveSizeMB) * 1024 * 1024); err != nil {
		f.Close()
		return err
	}
	f.Close()
	if out, err := exec.Command("mkfs.ext4", "-F", "-q", path).CombinedOutput(); err != nil {
		os.Remove(path)
		return fmt.Errorf("mkfs.ext4: %v: %s", err, out)
	}
	log.Printf("created data drive %s (%d MB)", path, m.cfg.DataDriveSizeMB)
	return nil
}

// Invoke sends an event to the agent's VM. It allows concurrent invocations to
// the same VM. On a transport error the VM is presumed dead and discarded so the
// next call reboots it.
func (m *Manager) Invoke(agentID string, event InvocationEvent) (*InvocationResponse, error) {
	vm, err := m.getVM(agentID)
	if err != nil {
		return nil, err
	}
	resp, err := invokeViaVsock(vm.vsockPath, event, m.cfg.InvokeTimeout)
	if err != nil {
		log.Printf("[%s] invoke error: %v — discarding VM", agentID, err)
		m.discard(vm)
		return nil, err
	}
	vm.touch()
	go m.store.RenewStorageLease(vm.storageID, m.cfg.Host, m.cfg.LeaseTTL)
	return resp, nil
}

// discard tears down a VM (dead or evicted) but keeps its data drive.
func (m *Manager) discard(vm *agentVM) {
	m.mu.Lock()
	if cur, ok := m.vms[vm.agentID]; ok && cur == vm {
		delete(m.vms, vm.agentID)
	}
	m.mu.Unlock()

	m.brokerDeregister(vm)
	killVM(vm)
	m.store.ReleaseStorageLease(vm.storageID, m.cfg.Host)
}

// idleSweeper evicts VMs idle longer than IdleTimeout. State survives on the
// host data drive, so re-invocation reboots and re-attaches it.
func (m *Manager) idleSweeper() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		m.mu.Lock()
		var stale []*agentVM
		for _, vm := range m.vms {
			if now.Sub(time.Unix(0, vm.lastUsedNs.Load())) > m.cfg.IdleTimeout {
				stale = append(stale, vm)
			}
		}
		m.mu.Unlock()

		for _, vm := range stale {
			log.Printf("[%s] idle, evicting VM", vm.agentID)
			m.discard(vm)
		}
	}
}

func (m *Manager) brokerRegister(vm *agentVM) error {
	return m.brokerCall("/register", vm)
}

func (m *Manager) brokerDeregister(vm *agentVM) {
	if err := m.brokerCall("/deregister", vm); err != nil {
		log.Printf("[%s] broker deregister: %v", vm.agentID, err)
	}
}

func (m *Manager) brokerCall(path string, vm *agentVM) error {
	body, _ := json.Marshal(map[string]string{
		"agent_id":   vm.agentID,
		"user_id":    vm.userID,
		"vsock_path": vm.vsockPath,
		"runtime":    vm.runtime,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, m.cfg.BrokerControlURL+path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("broker %s -> %d", path, resp.StatusCode)
	}
	return nil
}

func newID() string {
	b := make([]byte, 6)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}
