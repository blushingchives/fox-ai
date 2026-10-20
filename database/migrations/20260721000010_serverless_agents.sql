-- Serverless autonomous agents (Pi runtime on Firecracker). State and compute
-- are separate entities:
--   storage — the durable, user-facing thing (an agent's memory + workspace),
--             backed by a writable ext4 keyed by storage id. The MOUNT LEASE
--             lives here because storage is the single-writer resource.
--   agents  — a runnable bot: which engine build (runtime) to boot, attached to
--             a storage, plus its autonomous schedule. Switching provider means
--             changing runtime, or pointing a different agent at the same
--             storage; the storage lease guarantees only one runs at a time.
-- Ids are stor-/agt-prefixed, mirroring fn-/srv-. See docs/serverless-agents.md.

-- migrate:up

-- Storage: state + mount lease. lease_until unexpired => attached to a live VM
-- right now (owner_host/vsock_path identify it). Self-expiring so a crashed
-- host's storage frees itself after the TTL with no cleanup write.
CREATE TABLE IF NOT EXISTS storage (
    id           TEXT PRIMARY KEY DEFAULT 'stor-' || gen_random_uuid(),
    name         TEXT        NOT NULL,
    user_id      TEXT        REFERENCES users(id),
    owner_host   TEXT,        -- host whose VM currently has it mounted
    vsock_path   TEXT,        -- that VM's host UDS (broker identity anchor)
    lease_until  TIMESTAMPTZ, -- mount lease; unexpired => attached right now
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at   TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_storage_active_name
    ON storage (COALESCE(user_id, ''), name) WHERE deleted_at IS NULL;

-- Agents: engine + attachment + schedule. storage_id is the attachment (which
-- storage this bot uses); runtime selects the engine build image.
CREATE TABLE IF NOT EXISTS agents (
    id           TEXT PRIMARY KEY DEFAULT 'agt-' || gen_random_uuid(),
    name         TEXT        NOT NULL,
    user_id      TEXT        REFERENCES users(id),
    storage_id   TEXT        REFERENCES storage(id),
    runtime      TEXT        NOT NULL DEFAULT 'claude', -- engine build to boot
    config       JSONB       NOT NULL DEFAULT '{}',
    next_run_at  TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at   TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_agents_active_name
    ON agents (COALESCE(user_id, ''), name) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_agents_due
    ON agents (next_run_at) WHERE next_run_at IS NOT NULL AND deleted_at IS NULL;

-- Live per-user spend counter over an anchored 5-hour window. One row per user;
-- the window resets lazily in place (see reserve_usage). cap is USD; $2 for now.
CREATE TABLE IF NOT EXISTS usage_ledger (
    user_id      TEXT PRIMARY KEY,
    window_start TIMESTAMPTZ   NOT NULL DEFAULT now(),
    spent        NUMERIC(12,6) NOT NULL DEFAULT 0,
    reserved     NUMERIC(12,6) NOT NULL DEFAULT 0,
    cap          NUMERIC(12,6) NOT NULL DEFAULT 2.00
);

-- Permanent per-call record: billing history + audit. usage_ledger is only the
-- live counter (it resets); this table is the durable ledger.
CREATE TABLE IF NOT EXISTS agent_usage (
    id                 BIGSERIAL PRIMARY KEY,
    agent_id           TEXT        NOT NULL,
    user_id            TEXT        NOT NULL,
    model              TEXT        NOT NULL,
    input_tokens       INT         NOT NULL DEFAULT 0,
    output_tokens      INT         NOT NULL DEFAULT 0,
    cache_read_tokens  INT         NOT NULL DEFAULT 0,
    cache_write_tokens INT         NOT NULL DEFAULT 0,
    cost               NUMERIC(12,6) NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_agent_usage_user_time
    ON agent_usage (user_id, created_at DESC);

-- reserve_usage: atomically roll the 5h window if expired, then reserve p_est
-- against the cap. Returns TRUE if admitted, FALSE if the cap would be exceeded.
-- FOR UPDATE serializes concurrent callers for the same user (the hard-cap
-- guarantee).
CREATE OR REPLACE FUNCTION reserve_usage(p_user TEXT, p_est NUMERIC)
RETURNS BOOLEAN AS $$
DECLARE
    v_expired BOOLEAN;
BEGIN
    INSERT INTO usage_ledger (user_id) VALUES (p_user)
        ON CONFLICT (user_id) DO NOTHING;

    SELECT now() >= window_start + interval '5 hours'
      INTO v_expired
      FROM usage_ledger
     WHERE user_id = p_user
       FOR UPDATE;

    IF v_expired THEN
        UPDATE usage_ledger
           SET window_start = now(), spent = 0, reserved = p_est
         WHERE user_id = p_user;
        RETURN TRUE;
    END IF;

    UPDATE usage_ledger
       SET reserved = reserved + p_est
     WHERE user_id = p_user
       AND spent + reserved + p_est <= cap;

    RETURN FOUND;  -- FALSE => cap hit
END;
$$ LANGUAGE plpgsql;

-- settle_usage: after a call completes, replace the reservation with the actual
-- cost. greatest(...,0) guards the case where the window rolled between reserve
-- and settle.
CREATE OR REPLACE FUNCTION settle_usage(p_user TEXT, p_est NUMERIC, p_actual NUMERIC)
RETURNS VOID AS $$
BEGIN
    UPDATE usage_ledger
       SET spent    = spent + p_actual,
           reserved = greatest(reserved - p_est, 0)
     WHERE user_id = p_user;
END;
$$ LANGUAGE plpgsql;

-- migrate:down
DROP FUNCTION IF EXISTS settle_usage(TEXT, NUMERIC, NUMERIC);
DROP FUNCTION IF EXISTS reserve_usage(TEXT, NUMERIC);
DROP TABLE IF EXISTS agent_usage;
DROP TABLE IF EXISTS usage_ledger;
DROP TABLE IF EXISTS agents;
DROP TABLE IF EXISTS storage;
