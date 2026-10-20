package main

// Firecracker helpers, adapted from pool-manager. The agent difference is a
// third drive: a per-agent WRITABLE ext4 mounted read-write, holding the Pi
// session + workspace. The base rootfs and runtime code drive stay read-only.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"syscall"
	"time"
)

func spawnFirecracker(bin, socketPath string) (int, error) {
	cmd := exec.Command(bin, "--api-sock", socketPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("exec firecracker: %w", err)
	}
	go cmd.Wait() // reap on exit so it doesn't zombie

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(socketPath); err == nil {
			return cmd.Process.Pid, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	cmd.Process.Kill()
	return 0, fmt.Errorf("timeout waiting for firecracker socket at %s", socketPath)
}

func killVM(vm *agentVM) {
	if vm.pid > 0 {
		if p, err := os.FindProcess(vm.pid); err == nil {
			p.Signal(syscall.SIGTERM)
		}
	}
	os.Remove(vm.socketPath)
	os.Remove(vm.vsockPath)
	// NOTE: vm.dataPath (the writable ext4) is intentionally NOT removed — it
	// persists on the host across evictions so the agent keeps its state.
}

func fcRequest(method, socketPath, path string, body any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", socketPath)
			},
		},
	}
	req, err := http.NewRequest(method, "http://localhost"+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		var msg map[string]any
		json.NewDecoder(resp.Body).Decode(&msg)
		return fmt.Errorf("firecracker %s → %d: %v", path, resp.StatusCode, msg)
	}
	return nil
}

func fcPut(socketPath, path string, body any) error {
	return fcRequest(http.MethodPut, socketPath, path, body)
}

// configureVM sets up an agent VM: read-only base rootfs + read-only runtime
// code drive + a writable per-agent data drive, plus the vsock device.
func configureVM(socketPath string, cfg Config, codeRootfs, dataDrive, vsockPath string) error {
	if err := fcPut(socketPath, "/boot-source", map[string]any{
		"kernel_image_path": cfg.KernelPath,
		"boot_args":         "console=ttyS0 reboot=k panic=1 pci=off init=/var/runtime/bootstrap",
	}); err != nil {
		return fmt.Errorf("boot-source: %w", err)
	}

	// Base rootfs (OS + Node runtime) — read-only.
	if err := fcPut(socketPath, "/drives/rootfs", map[string]any{
		"drive_id":       "rootfs",
		"path_on_host":   cfg.BaseRootfs,
		"is_root_device": true,
		"is_read_only":   true,
	}); err != nil {
		return fmt.Errorf("base rootfs: %w", err)
	}

	// Runtime code drive (the single shared Pi agent image) — read-only, /dev/vdb.
	if err := fcPut(socketPath, "/drives/code", map[string]any{
		"drive_id":       "code",
		"path_on_host":   codeRootfs,
		"is_root_device": false,
		"is_read_only":   true,
	}); err != nil {
		return fmt.Errorf("code drive: %w", err)
	}

	// Writable per-agent data drive — /dev/vdc, mounted rw at /data by bootstrap.
	if err := fcPut(socketPath, "/drives/data", map[string]any{
		"drive_id":       "data",
		"path_on_host":   dataDrive,
		"is_root_device": false,
		"is_read_only":   false,
	}); err != nil {
		return fmt.Errorf("data drive: %w", err)
	}

	if err := fcPut(socketPath, "/machine-config", map[string]any{
		"vcpu_count":   cfg.VcpuCount,
		"mem_size_mib": cfg.MemMiB,
	}); err != nil {
		return fmt.Errorf("machine-config: %w", err)
	}

	// Vsock: bootstrap listens on 8080 (invocations); the egress proxy dials
	// host CID 2 for model calls. uds_path is the broker's identity anchor.
	if err := fcPut(socketPath, "/vsock", map[string]any{
		"guest_cid": 3,
		"uds_path":  vsockPath,
	}); err != nil {
		return fmt.Errorf("vsock: %w", err)
	}
	return nil
}

func startVM(socketPath string) error {
	return fcPut(socketPath, "/actions", map[string]any{
		"action_type": "InstanceStart",
	})
}
