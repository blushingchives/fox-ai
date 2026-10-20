package main

import (
	"database/sql"
	"log"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"
)

// Store wraps Postgres. Like the metrics service it degrades gracefully when no
// DATABASE_URL is set. Schema (incl. reserve_usage/settle_usage) is owned by
// database/migrations.
type Store struct {
	db    *sql.DB
	ready atomic.Bool
}

func NewStore(databaseURL string) *Store {
	s := &Store{}
	if databaseURL == "" {
		log.Println("DATABASE_URL not set — broker cannot enforce caps, refusing all calls")
		return s
	}
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		log.Printf("open database: %v", err)
		return s
	}
	s.db = db
	go s.waitReadyLoop()
	return s
}

func (s *Store) Ready() bool { return s.db != nil && s.ready.Load() }

func (s *Store) waitReadyLoop() {
	for {
		if err := s.db.Ping(); err != nil {
			log.Printf("database not ready: %v (retrying in 30s)", err)
			time.Sleep(30 * time.Second)
			continue
		}
		s.ready.Store(true)
		log.Println("database ready")
		return
	}
}

// Reserve atomically rolls the 5h window if needed and reserves est against the
// user's cap. Returns false when the cap is hit.
func (s *Store) Reserve(userID string, est float64) (bool, error) {
	var ok bool
	err := s.db.QueryRow(`SELECT reserve_usage($1, $2)`, userID, est).Scan(&ok)
	return ok, err
}

// RecordCompletion replaces the reservation with the real cost and appends the
// permanent audit row, in one transaction.
func (s *Store) RecordCompletion(userID, agentID, model string, est float64, u Usage, cost float64) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`SELECT settle_usage($1, $2, $3)`, userID, est, cost); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO agent_usage
		(agent_id, user_id, model, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, cost)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		agentID, userID, model,
		u.InputTokens, u.OutputTokens, u.CacheReadTokens, u.CacheWriteTokens, cost); err != nil {
		return err
	}
	return tx.Commit()
}
