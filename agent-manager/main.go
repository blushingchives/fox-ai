package main

// agent-manager owns the sticky per-agent Firecracker VMs. It exposes:
//   POST /tick/<agentID>     — autonomous turn (called by agent-dispatcher)
//   POST /message/<agentID>  — user turn (called by the app/controller)
// Both forward into the VM's runtime over vsock. Unlike pool-manager, the
// response is unwrapped (runtime body returned directly) so the dispatcher can
// read next_run_seconds without unpacking an envelope.

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	loadEnvFile()
	store := NewStore(getEnv("DATABASE_URL", ""))

	cfg := Config{
		KernelPath:       getEnv("KERNEL_PATH", "/tmp/fc/vmlinux.bin"),
		BaseRootfs:       getEnv("AGENT_BASE_ROOTFS", "/tmp/agent-base.ext4"),
		RuntimeDir:       getEnv("AGENT_RUNTIME_DIR", "/tmp/agent-runtimes"),
		DataDir:          getEnv("AGENT_DATA_DIR", "/tmp/agent-data"),
		FirecrackerBin:   getEnv("FIRECRACKER_BIN", "firecracker"),
		BrokerControlURL: strings.TrimRight(getEnv("BROKER_CONTROL_URL", "http://127.0.0.1:8090"), "/"),
		DataDriveSizeMB:  getEnvInt("DATA_DRIVE_SIZE_MB", 2048),
		VcpuCount:        getEnvInt("VCPU_COUNT", 1),
		MemMiB:           getEnvInt("MEM_MIB", 1024),
		IdleTimeout:      time.Duration(getEnvInt("IDLE_TIMEOUT_SECONDS", 300)) * time.Second,
		InvokeTimeout:    time.Duration(getEnvInt("INVOKE_TIMEOUT_SECONDS", 600)) * time.Second,
		LeaseTTL:         time.Duration(getEnvInt("LEASE_TTL_SECONDS", 120)) * time.Second,
		Host:             getEnv("HOST_ID", hostname()),
	}
	mgr := NewManager(store, cfg)

	http.HandleFunc("/tick/", turnHandler(mgr, "/tick"))
	http.HandleFunc("/message/", turnHandler(mgr, "/message"))
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"database": store.Ready()})
	})

	addr := getEnv("LISTEN_ADDR", ":8080")
	log.Printf("agent-manager listening on %s (host=%s)", addr, cfg.Host)
	log.Fatal(http.ListenAndServe(addr, nil))
}

// turnHandler builds an invocation for the given runtime path (/tick or
// /message) from the incoming request and returns the runtime's response.
func turnHandler(mgr *Manager, runtimePath string) http.HandlerFunc {
	prefix := strings.TrimSuffix(runtimePath, "/") + "/" // "/tick/"
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		agentID := strings.TrimPrefix(r.URL.Path, prefix)
		if agentID == "" {
			http.Error(w, "agent id required", http.StatusBadRequest)
			return
		}
		body, _ := io.ReadAll(r.Body)

		event := InvocationEvent{
			Method:  http.MethodPost,
			Path:    runtimePath,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    string(body),
		}
		resp, err := mgr.Invoke(agentID, event)
		if err != nil {
			switch {
			case errors.Is(err, ErrAgentUnknown):
				http.Error(w, "agent not found", http.StatusNotFound)
			case errors.Is(err, errLeaseHeld):
				http.Error(w, "agent is running on another host", http.StatusConflict)
			default:
				log.Printf("[%s] invoke: %v", agentID, err)
				http.Error(w, "invocation failed", http.StatusBadGateway)
			}
			return
		}
		if resp.InfraError {
			http.Error(w, "runtime error: "+resp.Body, http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.Status)
		io.WriteString(w, resp.Body)
	}
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown-host"
	}
	return h
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

// loadEnvFile reads KEY=VALUE lines from ./.env; externally-set vars win.
func loadEnvFile() {
	data, err := os.ReadFile(".env")
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		if key != "" && os.Getenv(key) == "" {
			os.Setenv(key, value)
		}
	}
}
