package main

// Provider-agnostic hot path: identity -> reserve -> forward -> meter -> settle
// -> audit. The vendor-specific bits are delegated to reg.provider (see
// providers.go, anthropic.go, openai.go).

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
)

func (b *Broker) handleModelCall(reg *registration, w http.ResponseWriter, r *http.Request) {
	provider := reg.provider
	if provider == nil {
		writeError(w, nil, http.StatusBadGateway, "no provider configured for this agent")
		return
	}
	if !b.store.Ready() {
		writeError(w, provider, http.StatusServiceUnavailable, "broker database unavailable")
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, provider, http.StatusBadRequest, "read body: "+err.Error())
		return
	}
	path := r.URL.Path

	// Non-metered endpoints (token counting, model listing) still get the key
	// and upstream routing, but no reservation.
	if !provider.Meters(path) {
		b.passthrough(provider, w, r, body)
		return
	}

	// model + stream live at the same top-level fields for both vendors.
	var meta struct {
		Model  string `json:"model"`
		Stream bool   `json:"stream"`
	}
	json.Unmarshal(body, &meta)

	// Reserve against the user's cap before spending anything upstream.
	ok, err := b.store.Reserve(reg.userID, b.cfg.ReserveUSD)
	if err != nil {
		log.Printf("[%s] reserve: %v", reg.agentID, err)
		writeError(w, provider, http.StatusServiceUnavailable, "reserve failed")
		return
	}
	if !ok {
		writeError(w, provider, http.StatusTooManyRequests, "usage cap reached for this 5-hour window")
		return
	}

	// From here we MUST settle the reservation, even on error, or the user's
	// reserved balance leaks until the window rolls.
	var usage Usage
	settled := false
	settle := func() {
		if settled {
			return
		}
		settled = true
		cost := costOf(meta.Model, usage)
		if err := b.store.RecordCompletion(reg.userID, reg.agentID, meta.Model, b.cfg.ReserveUSD, usage, cost); err != nil {
			log.Printf("[%s] settle: %v", reg.agentID, err)
		}
	}
	defer settle()

	body = provider.RewriteBody(path, body, meta.Stream)

	upstream, err := b.forward(provider, r, body)
	if err != nil {
		log.Printf("[%s] upstream: %v", reg.agentID, err)
		writeError(w, provider, http.StatusBadGateway, "upstream error")
		return
	}
	defer upstream.Body.Close()

	copyHeaders(w.Header(), upstream.Header)
	w.WriteHeader(upstream.StatusCode)

	if meta.Stream {
		usage = streamThrough(w, upstream.Body, provider)
	} else {
		usage = bufferThrough(w, upstream.Body, provider)
	}
}

// forward builds the upstream request with the provider's key + headers injected.
func (b *Broker) forward(provider Provider, r *http.Request, body []byte) (*http.Response, error) {
	req, err := http.NewRequest(r.Method, provider.BaseURL()+r.URL.Path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	// Forward vendor beta/org opt-ins the client asked for; auth is set by the
	// provider (and thus overrides any placeholder the guest sent).
	for _, h := range []string{"anthropic-beta", "openai-beta", "openai-organization"} {
		if v := r.Header.Get(h); v != "" {
			req.Header.Set(h, v)
		}
	}
	provider.Auth(req)
	return http.DefaultClient.Do(req)
}

func (b *Broker) passthrough(provider Provider, w http.ResponseWriter, r *http.Request, body []byte) {
	upstream, err := b.forward(provider, r, body)
	if err != nil {
		writeError(w, provider, http.StatusBadGateway, "upstream error")
		return
	}
	defer upstream.Body.Close()
	copyHeaders(w.Header(), upstream.Header)
	w.WriteHeader(upstream.StatusCode)
	io.Copy(w, upstream.Body)
}

func bufferThrough(w http.ResponseWriter, body io.Reader, provider Provider) Usage {
	data, _ := io.ReadAll(body)
	w.Write(data)
	return provider.UsageBuffered(data)
}

// streamThrough relays an SSE stream to the guest line-by-line while handing each
// "data:" payload to the provider to accumulate usage.
func streamThrough(w http.ResponseWriter, body io.Reader, provider Provider) Usage {
	var usage Usage
	flusher, _ := w.(http.Flusher)

	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		w.Write(line)
		w.Write([]byte("\n"))
		if flusher != nil {
			flusher.Flush()
		}
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		payload := bytes.TrimSpace(line[len("data:"):])
		if len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) {
			continue
		}
		provider.UsageStreamData(payload, &usage)
	}
	if err := scanner.Err(); err != nil {
		log.Printf("stream scan: %v", err)
	}
	return usage
}

func copyHeaders(dst, src http.Header) {
	for k, vals := range src {
		switch strings.ToLower(k) {
		case "connection", "transfer-encoding", "content-length":
			continue // managed by the Go server itself
		}
		for _, v := range vals {
			dst.Add(k, v)
		}
	}
}

// writeError renders an error in the provider's vendor shape (or a generic one
// when no provider is resolved) so Pi surfaces it as an API error.
func writeError(w http.ResponseWriter, provider Provider, code int, msg string) {
	var body any
	if provider != nil {
		body = provider.ErrorBody(msg)
	} else {
		body = map[string]any{"type": "error", "error": map[string]string{"type": "broker_error", "message": msg}}
	}
	writeJSON(w, code, body)
}
