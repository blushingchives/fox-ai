package main

import (
	"database/sql"
	"errors"
	"log"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"
)

var errLeaseHeld = errors.New("storage lease held by another host")

type Store struct {
	db    *sql.DB
	ready atomic.Bool
}

// agentInfo is what the manager needs to boot a VM: which storage to mount,
// which engine build to boot, and who to attribute usage to.
type agentInfo struct {
	storageID string
	runtime   string
	userID    string
}

func NewStore(databaseURL string) *Store {
	s := &Store{}
	if databaseURL == "" {
		log.Println("DATABASE_URL not set — agent-manager requires it")
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

// LookupAgent resolves a live agent's storage attachment, engine build, and
// owner. found=false means the agent doesn't exist or was deleted.
func (s *Store) LookupAgent(agentID string) (info agentInfo, found bool, err error) {
	var storageID, runtime, userID sql.NullString
	err = s.db.QueryRow(
		`SELECT storage_id, runtime, user_id FROM agents WHERE id = $1 AND deleted_at IS NULL`,
		agentID).Scan(&storageID, &runtime, &userID)
	if err == sql.ErrNoRows {
		return agentInfo{}, false, nil
	}
	if err != nil {
		return agentInfo{}, false, err
	}
	return agentInfo{storageID: storageID.String, runtime: runtime.String, userID: userID.String}, true, nil
}

// AcquireStorageLease claims a storage for this host, recording the live VM's
// vsock path. Keyed on STORAGE, not agent: two agents (e.g. a claude build and a
// chatgpt build) may reference the same storage, and this is what stops them
// mounting the same ext4 at once. Fails with errLeaseHeld if held elsewhere.
func (s *Store) AcquireStorageLease(storageID, host, vsockPath string, ttl time.Duration) error {
	res, err := s.db.Exec(`
		UPDATE storage
		   SET owner_host = $2, vsock_path = $3, lease_until = now() + make_interval(secs => $4)
		 WHERE id = $1 AND deleted_at IS NULL
		   AND (lease_until IS NULL OR lease_until < now() OR owner_host = $2)`,
		storageID, host, vsockPath, ttl.Seconds())
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errLeaseHeld
	}
	return nil
}

func (s *Store) RenewStorageLease(storageID, host string, ttl time.Duration) error {
	_, err := s.db.Exec(`
		UPDATE storage SET lease_until = now() + make_interval(secs => $3)
		 WHERE id = $1 AND owner_host = $2`, storageID, host, ttl.Seconds())
	return err
}

func (s *Store) ReleaseStorageLease(storageID, host string) error {
	_, err := s.db.Exec(`
		UPDATE storage SET lease_until = NULL, owner_host = NULL, vsock_path = NULL
		 WHERE id = $1 AND owner_host = $2`, storageID, host)
	return err
}
