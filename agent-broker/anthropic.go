package main

import (
	"encoding/json"
	"net/http"
	"strings"
)

// anthropicProvider adapts the Anthropic Messages API. Anthropic reports the
// four token classes separately, so its mapping into Usage is 1:1 (no subtraction).
type anthropicProvider struct {
	baseURL string
	key     string
	version string
}

func (p *anthropicProvider) Name() string   { return "anthropic" }
func (p *anthropicProvider) BaseURL() string { return p.baseURL }

func (p *anthropicProvider) Auth(req *http.Request) {
	req.Header.Set("x-api-key", p.key)
	req.Header.Set("anthropic-version", p.version)
}

func (p *anthropicProvider) Meters(path string) bool {
	return strings.HasPrefix(path, "/v1/messages") && !strings.Contains(path, "count_tokens")
}

func (p *anthropicProvider) RewriteBody(path string, body []byte, stream bool) []byte {
	return body // Anthropic reports stream usage without opt-in
}

// anthropicUsage is the shared shape across buffered responses and the
// message_start / message_delta stream events.
type anthropicUsage struct {
	InputTokens              int `json:"input_tokens"`
	OutputTokens             int `json:"output_tokens"`
	CacheReadInputTokens     int `json:"cache_read_input_tokens"`
	CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
}

func (u anthropicUsage) toUsage() Usage {
	return Usage{
		InputTokens:      u.InputTokens,
		OutputTokens:     u.OutputTokens,
		CacheReadTokens:  u.CacheReadInputTokens,
		CacheWriteTokens: u.CacheCreationInputTokens,
	}
}

func (p *anthropicProvider) UsageBuffered(body []byte) Usage {
	var parsed struct {
		Usage anthropicUsage `json:"usage"`
	}
	json.Unmarshal(body, &parsed)
	return parsed.Usage.toUsage()
}

// UsageStreamData reads Anthropic SSE data payloads: input/cache tokens arrive in
// message_start; output_tokens accumulates across message_delta (last is final).
func (p *anthropicProvider) UsageStreamData(data []byte, u *Usage) {
	var ev struct {
		Type    string `json:"type"`
		Message struct {
			Usage anthropicUsage `json:"usage"`
		} `json:"message"`
		Usage anthropicUsage `json:"usage"`
	}
	if json.Unmarshal(data, &ev) != nil {
		return
	}
	switch ev.Type {
	case "message_start":
		u.InputTokens = ev.Message.Usage.InputTokens
		u.CacheReadTokens = ev.Message.Usage.CacheReadInputTokens
		u.CacheWriteTokens = ev.Message.Usage.CacheCreationInputTokens
		if ev.Message.Usage.OutputTokens > 0 {
			u.OutputTokens = ev.Message.Usage.OutputTokens
		}
	case "message_delta":
		if ev.Usage.OutputTokens > 0 {
			u.OutputTokens = ev.Usage.OutputTokens
		}
	}
}

func (p *anthropicProvider) ErrorBody(msg string) any {
	return map[string]any{
		"type":  "error",
		"error": map[string]string{"type": "broker_error", "message": msg},
	}
}
