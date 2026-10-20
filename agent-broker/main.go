package main

// agent-broker runs on each host and is the ONLY component that holds the
// Anthropic API key. Agent VMs reach it over vsock (never the network), so the
// key never enters a VM. Identity is host-asserted: the agent-manager registers
// each VM's vsock UDS path with the broker; the broker opens a per-VM listener
// at "<vsock_path>_<port>" (the path Firecracker connects to for guest->host
// vsock), so any request arriving on that socket is provably from that VM.
//
// Per request: resolve agent/user from the socket -> reserve against the user's
// 5h cap -> forward to Anthropic -> reconcile actual usage -> write audit.

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
)

// registration ties a live VM's vsock socket to its agent/user identity and the
// model provider its engine build uses.
type registration struct {
	agentID  string
	userID   string
	sockPath string // "<vsock_path>_<vsockPort>"
	provider Provider
	listener net.Listener
}

type Broker struct {
	mu        sync.Mutex
	regs      map[string]*registration // keyed by agentID
	store     *Store
	cfg       Config
	anthropic Provider
	openai    Provider
}

type Config struct {
	AnthropicBaseURL string
	AnthropicKey     string
	AnthropicVersion string
	OpenAIBaseURL    string
	OpenAIKey        string
	VsockPort        int     // guest connects to CID 2 on this port
	ReserveUSD       float64 // flat per-call reservation
}

func main() {
	loadEnvFile()

	cfg := Config{
		AnthropicBaseURL: strings.TrimRight(getEnv("ANTHROPIC_BASE_URL", "https://api.anthropic.com"), "/"),
		AnthropicKey:     getEnv("ANTHROPIC_API_KEY", ""),
		AnthropicVersion: getEnv("ANTHROPIC_VERSION", "2023-06-01"),
		OpenAIBaseURL:    strings.TrimRight(getEnv("OPENAI_BASE_URL", "https://api.openai.com"), "/"),
		OpenAIKey:        getEnv("OPENAI_API_KEY", ""),
		VsockPort:        getEnvInt("AGENT_BROKER_VSOCK_PORT", 9000),
		ReserveUSD:       getEnvFloat("RESERVE_USD", 0.10),
	}
	b := &Broker{
		regs:      make(map[string]*registration),
		store:     NewStore(getEnv("DATABASE_URL", "")),
		cfg:       cfg,
		anthropic: &anthropicProvider{baseURL: cfg.AnthropicBaseURL, key: cfg.AnthropicKey, version: cfg.AnthropicVersion},
		openai:    &openaiProvider{baseURL: cfg.OpenAIBaseURL, key: cfg.OpenAIKey},
	}
	if cfg.AnthropicKey == "" {
		log.Println("WARNING: ANTHROPIC_API_KEY not set — claude runtimes will 401")
	}
	if cfg.OpenAIKey == "" {
		log.Println("WARNING: OPENAI_API_KEY not set — chatgpt runtimes will 401")
	}
	loadPrices(getEnv("PRICES_PATH", "prices.json"))

	// Host-local control plane. Only the agent-manager (same host) calls this.
	http.HandleFunc("/register", b.handleRegister)
	http.HandleFunc("/deregister", b.handleDeregister)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		b.mu.Lock()
		n := len(b.regs)
		b.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{
			"database": b.store.Ready(),
			"agents":   n,
		})
	})

	addr := getEnv("CONTROL_ADDR", "127.0.0.1:8090")
	log.Printf("agent-broker control plane listening on %s (vsock port %d)", addr, b.cfg.VsockPort)
	log.Fatal(http.ListenAndServe(addr, nil))
}

type registerReq struct {
	AgentID   string `json:"agent_id"`
	UserID    string `json:"user_id"`
	VsockPath string `json:"vsock_path"`
	Runtime   string `json:"runtime"` // engine build -> selects the provider
}

// handleRegister opens a per-VM unix listener at the path Firecracker uses for
// guest->host vsock, and serves model calls from that VM with its identity.
func (b *Broker) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req registerReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if req.AgentID == "" || req.UserID == "" || req.VsockPath == "" {
		http.Error(w, "agent_id, user_id, vsock_path required", http.StatusBadRequest)
		return
	}

	sockPath := fmt.Sprintf("%s_%d", req.VsockPath, b.cfg.VsockPort)

	b.mu.Lock()
	if existing, ok := b.regs[req.AgentID]; ok {
		existing.listener.Close()
		delete(b.regs, req.AgentID)
	}
	b.mu.Unlock()

	os.Remove(sockPath) // clear any stale socket from a prior VM
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		log.Printf("[%s] listen %s: %v", req.AgentID, sockPath, err)
		http.Error(w, "listen failed", http.StatusInternalServerError)
		return
	}

	provider := b.providerForRuntime(req.Runtime)
	reg := &registration{agentID: req.AgentID, userID: req.UserID, sockPath: sockPath, provider: provider, listener: ln}
	b.mu.Lock()
	b.regs[req.AgentID] = reg
	b.mu.Unlock()

	go b.serve(reg)
	log.Printf("[%s] registered (user=%s, runtime=%s -> %s) on %s",
		req.AgentID, req.UserID, req.Runtime, provider.Name(), sockPath)
	w.WriteHeader(http.StatusNoContent)
}

func (b *Broker) handleDeregister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req registerReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	b.mu.Lock()
	if reg, ok := b.regs[req.AgentID]; ok {
		reg.listener.Close()
		os.Remove(reg.sockPath)
		delete(b.regs, req.AgentID)
	}
	b.mu.Unlock()
	log.Printf("[%s] deregistered", req.AgentID)
	w.WriteHeader(http.StatusNoContent)
}

// serve runs an HTTP server over one VM's unix socket. Every connection here is
// from that VM (host-asserted identity), so the handler can trust reg's ids.
func (b *Broker) serve(reg *registration) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.handleModelCall(reg, w, r)
	})
	if err := http.Serve(reg.listener, handler); err != nil {
		// Serve returns when the listener is closed on deregister — expected.
		log.Printf("[%s] serve stopped: %v", reg.agentID, err)
	}
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

func getEnvFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// loadEnvFile mirrors the other services: KEY=VALUE lines from ./.env, with
// externally-set vars winning.
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
