package main

import (
	"encoding/json"
	"net/http"
	"strings"
)

// openaiProvider adapts the OpenAI Chat Completions API. Pi, configured for
// OpenAI, calls this through the same egress proxy; the bootstrap points
// OPENAI_BASE_URL at it.
type openaiProvider struct {
	baseURL string
	key     string
}

func (p *openaiProvider) Name() string   { return "openai" }
func (p *openaiProvider) BaseURL() string { return p.baseURL }

func (p *openaiProvider) Auth(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+p.key)
}

func (p *openaiProvider) Meters(path string) bool {
	return strings.HasPrefix(path, "/v1/chat/completions") ||
		strings.HasPrefix(path, "/v1/responses") ||
		strings.HasPrefix(path, "/v1/completions")
}

// RewriteBody forces usage reporting on streaming Chat Completions. OpenAI omits
// usage from a stream unless stream_options.include_usage is set — without this,
// a streamed call would settle as 0 tokens and never charge the cap.
func (p *openaiProvider) RewriteBody(path string, body []byte, stream bool) []byte {
	if !stream || !strings.HasPrefix(path, "/v1/chat/completions") {
		return body
	}
	var m map[string]any
	if json.Unmarshal(body, &m) != nil {
		return body
	}
	so, _ := m["stream_options"].(map[string]any)
	if so == nil {
		so = map[string]any{}
	}
	so["include_usage"] = true
	m["stream_options"] = so
	out, err := json.Marshal(m)
	if err != nil {
		return body
	}
	return out
}

// openaiUsage is the shared shape of OpenAI's usage object across buffered and
// streamed responses.
type openaiUsage struct {
	PromptTokens        int `json:"prompt_tokens"`
	CompletionTokens    int `json:"completion_tokens"`
	PromptTokensDetails struct {
		CachedTokens int `json:"cached_tokens"`
	} `json:"prompt_tokens_details"`
}

// normalize maps OpenAI usage into our disjoint classes. OpenAI's prompt_tokens
// INCLUDES cached tokens, so input becomes the non-cached remainder to keep
// additive pricing correct; OpenAI has no separate cache-write charge.
func (u openaiUsage) normalize() Usage {
	input := u.PromptTokens - u.PromptTokensDetails.CachedTokens
	if input < 0 {
		input = 0
	}
	return Usage{
		InputTokens:      input,
		OutputTokens:     u.CompletionTokens,
		CacheReadTokens:  u.PromptTokensDetails.CachedTokens,
		CacheWriteTokens: 0,
	}
}

func (p *openaiProvider) UsageBuffered(body []byte) Usage {
	var parsed struct {
		Usage *openaiUsage `json:"usage"`
	}
	if json.Unmarshal(body, &parsed) != nil || parsed.Usage == nil {
		return Usage{}
	}
	return parsed.Usage.normalize()
}

func (p *openaiProvider) UsageStreamData(data []byte, u *Usage) {
	// In a stream, usage is null on every chunk except the final one (only
	// emitted when include_usage is set), which carries the totals.
	var chunk struct {
		Usage *openaiUsage `json:"usage"`
	}
	if json.Unmarshal(data, &chunk) != nil || chunk.Usage == nil {
		return
	}
	*u = chunk.Usage.normalize()
}

func (p *openaiProvider) ErrorBody(msg string) any {
	return map[string]any{
		"error": map[string]any{"message": msg, "type": "broker_error", "code": nil},
	}
}
