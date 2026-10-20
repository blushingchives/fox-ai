package main

import (
	"encoding/json"
	"log"
	"os"
	"strings"
)

// ModelPrice is USD per MILLION tokens for each token class — the units
// Anthropic publishes prices in, so prices.json reads like the pricing page.
type ModelPrice struct {
	Input      float64 `json:"input"`
	Output     float64 `json:"output"`
	CacheWrite float64 `json:"cache_write"`
	CacheRead  float64 `json:"cache_read"`
}

// prices is loaded at startup from PRICES_PATH. Deliberately config-driven
// rather than hardcoded so published price changes are a config edit, not a
// code change (and so we never bake in a stale number). Keys are matched by
// longest prefix, so "claude-opus-4" covers dated ids like
// "claude-opus-4-8-20260101".
var prices map[string]ModelPrice

func loadPrices(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Printf("prices: %v — cost will be recorded as 0 until %s exists", err, path)
		prices = map[string]ModelPrice{}
		return
	}
	if err := json.Unmarshal(data, &prices); err != nil {
		log.Printf("prices: parse %s: %v", path, err)
		prices = map[string]ModelPrice{}
		return
	}
	log.Printf("prices: loaded %d models from %s", len(prices), path)
}

func priceFor(model string) (ModelPrice, bool) {
	if p, ok := prices[model]; ok {
		return p, true
	}
	// longest-prefix match so dated model ids resolve to their family price.
	var best string
	for k := range prices {
		if strings.HasPrefix(model, k) && len(k) > len(best) {
			best = k
		}
	}
	if best != "" {
		return prices[best], true
	}
	return ModelPrice{}, false
}

// costOf converts a token breakdown into USD. Unknown models cost 0 (and log),
// which keeps the cap from silently over-charging on a misconfiguration — the
// missing-price case is visible in the audit as $0 rows.
func costOf(model string, u Usage) float64 {
	p, ok := priceFor(model)
	if !ok {
		log.Printf("pricing: no price for model %q — recording cost 0", model)
		return 0
	}
	const perToken = 1.0 / 1_000_000.0
	return perToken * (float64(u.InputTokens)*p.Input +
		float64(u.OutputTokens)*p.Output +
		float64(u.CacheWriteTokens)*p.CacheWrite +
		float64(u.CacheReadTokens)*p.CacheRead)
}
