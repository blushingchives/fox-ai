package main

import "net/http"

// Usage is the normalized token breakdown across providers. Each provider maps
// its own wire fields into these four classes so pricing (costOf) is uniform.
// Invariant the adapters must hold: the four classes are DISJOINT (additive),
// i.e. InputTokens must exclude cached tokens — Anthropic already reports them
// separately, OpenAI does not, so the OpenAI adapter subtracts (see openai.go).
type Usage struct {
	InputTokens      int
	OutputTokens     int
	CacheReadTokens  int
	CacheWriteTokens int
}

// Provider isolates everything that differs between model vendors. The broker's
// flow — identity, reserve, forward, meter, settle, audit — is provider-agnostic
// (see proxy.go); only these methods are vendor-specific.
type Provider interface {
	Name() string
	BaseURL() string
	// Auth sets the upstream auth + version headers on the outbound request.
	Auth(req *http.Request)
	// Meters reports whether a request to this path should be reserved/metered
	// (vs a cheap passthrough like token counting or model listing).
	Meters(path string) bool
	// RewriteBody may adjust the request body before forwarding (e.g. OpenAI
	// needs stream_options.include_usage to report usage on streamed calls).
	RewriteBody(path string, body []byte, stream bool) []byte
	// UsageBuffered extracts usage from a full non-streaming response body.
	UsageBuffered(body []byte) Usage
	// UsageStreamData updates usage from a single SSE "data:" payload.
	UsageStreamData(data []byte, u *Usage)
	// ErrorBody renders an error in this vendor's shape so the client (Pi)
	// surfaces it as a provider API error rather than a parse failure.
	ErrorBody(msg string) any
}

// providerForRuntime maps an agent's engine build to its model vendor. The
// agent-manager sends the runtime at registration; unknown/claude default to
// Anthropic.
func (b *Broker) providerForRuntime(runtime string) Provider {
	switch runtime {
	case "chatgpt", "openai", "gpt":
		return b.openai
	default:
		return b.anthropic
	}
}
