package main

// agent-dispatcher drives agent autonomy. Every poll interval it claims agents
// whose next_run_at is due and asks the agent-manager to /tick them. The agent
// itself decides when it next wants to run (returned as next_run_seconds), which
// the dispatcher writes back — so idle agents cost nothing and cadence is
// adaptive rather than a fixed blanket sweep.

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

type Config struct {
	AgentManagerURL string
	PollInterval    time.Duration
	DefaultBackoff  time.Duration // provisional next_run on claim / on failure
	Batch           int
}

type dueAgent struct {
	ID     string
	UserID string
}

func main() {
	loadEnvFile()
	cfg := Config{
		AgentManagerURL: strings.TrimRight(getEnv("AGENT_MANAGER_URL", "http://localhost:8080"), "/"),
		PollInterval:    time.Duration(getEnvInt("POLL_INTERVAL_SECONDS", 60)) * time.Second,
		DefaultBackoff:  time.Duration(getEnvInt("DEFAULT_BACKOFF_SECONDS", 600)) * time.Second,
		Batch:           getEnvInt("BATCH", 50),
	}

	dbURL := getEnv("DATABASE_URL", "")
	if dbURL == "" {
		log.Fatal("DATABASE_URL is required")
	}
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	for db.Ping() != nil {
		log.Println("database not ready, retrying in 5s")
		time.Sleep(5 * time.Second)
	}
	log.Printf("agent-dispatcher started (poll=%s, manager=%s)", cfg.PollInterval, cfg.AgentManagerURL)

	client := &http.Client{Timeout: 5 * time.Minute} // an agent turn can be long
	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()
	for range ticker.C {
		agents, err := claimDue(db, cfg)
		if err != nil {
			log.Printf("claim due: %v", err)
			continue
		}
		for _, a := range agents {
			go dispatch(db, client, cfg, a)
		}
	}
}

// claimDue atomically selects due agents and pushes their next_run_at forward by
// DefaultBackoff, so a slow tick isn't re-dispatched next poll. A successful
// tick overwrites next_run_at precisely; a failed one falls back to the backoff.
// SKIP LOCKED lets multiple dispatchers coexist without double-claiming.
func claimDue(db *sql.DB, cfg Config) ([]dueAgent, error) {
	rows, err := db.Query(`
		UPDATE agents SET next_run_at = now() + make_interval(secs => $1)
		WHERE id IN (
			SELECT id FROM agents
			WHERE next_run_at <= now() AND deleted_at IS NULL
			ORDER BY next_run_at
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, user_id`,
		cfg.DefaultBackoff.Seconds(), cfg.Batch)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []dueAgent
	for rows.Next() {
		var a dueAgent
		var userID sql.NullString
		if err := rows.Scan(&a.ID, &userID); err != nil {
			return nil, err
		}
		a.UserID = userID.String
		out = append(out, a)
	}
	return out, rows.Err()
}

type tickResponse struct {
	NextRunSeconds int `json:"next_run_seconds"`
}

func dispatch(db *sql.DB, client *http.Client, cfg Config, a dueAgent) {
	url := cfg.AgentManagerURL + "/tick/" + a.ID
	resp, err := client.Post(url, "application/json", bytes.NewReader([]byte("{}")))
	if err != nil {
		log.Printf("[%s] tick: %v (backing off %s)", a.ID, err, cfg.DefaultBackoff)
		return // provisional backoff from claimDue stands
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		log.Printf("[%s] tick -> %d (backing off)", a.ID, resp.StatusCode)
		return
	}

	var tr tickResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil || tr.NextRunSeconds <= 0 {
		// Agent didn't specify — keep the provisional backoff.
		return
	}
	if _, err := db.Exec(
		`UPDATE agents SET next_run_at = now() + make_interval(secs => $1) WHERE id = $2`,
		tr.NextRunSeconds, a.ID); err != nil {
		log.Printf("[%s] set next_run: %v", a.ID, err)
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
