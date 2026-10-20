# Serverless Autonomous Agents — Design

Status: **design, not yet built** · Last updated: 2026-07-20

## Goal

Run autonomous agents (openclaw/hermes-style) on a **serverless** substrate instead of
long-lived servers. Agents wake on a schedule *and* on user messages, keep durable memory
across invocations, and cost is capped per user. Reuses the Firecracker machinery already
in [pool-manager](../pool-manager) and [instance-manager](../instance-manager).

The agent runtime is **Pi** (`@earendil-works/pi-coding-agent`, https://pi.dev) — a coding
agent harness with a headless Node SDK. Pi is *coding-shaped* (operates on a working
directory); this design assumes the target agents are coding-shaped too.

## Core decision: Model A — sticky stateful VM

An agent is inherently a single stateful actor, so each agent gets **one sticky Firecracker
VM** rather than being spread across stateless workers:

- One VM per agent, `MaxVM=1` per agent pool → the pool serializes invocations for free
  (no distributed lock needed for state safety).
- A **writable** ext4 data drive holds the agent's workspace (functions use read-only
  drives; see [firecracker.go](../pool-manager/firecracker.go)).
- The in-VM runtime keeps one live Pi `AgentSession`; concurrency is handled *inside* the
  VM (see below), not by running parallel VMs for one agent.

The alternative (stateless step workers + external state + per-step DB lease) was rejected:
it reloads full state every step and gives up warm in-memory context, for scaling we don't
need yet (one agent = one actor).

## Components

```
                    ┌──────────────── host ─────────────────┐
  cron dispatcher   │                                        │
  (due agents)      │   agent-manager ──boots/owns──► VM     │
        │           │        │                     ┌───────┐ │
        └──POST /tick────────┘   invoke (vsock)    │ Pi    │ │
  user ──POST /message──────────────────────────► │ agent │ │
                    │                              │ +      │ │
                    │   agent-broker ◄──model calls│ express│ │
                    │   (vsock, = proxy)  (vsock)  └───┬───┘ │
                    │        │                    writable   │
                    └────────┼───────────────────── ext4 ───┘
                             │
                     ┌───────┴────────┐
              Anthropic API      Postgres (control plane)
              (single key)       usage_ledger / audit / schedule / agents
```

### 1. agent-manager (Go — fork of pool-manager)

Fork [pool-manager](../pool-manager) into `agent-manager`, same architecture, different
knobs. Changes from the function pool:

- Pools keyed by `agent_id`; `MaxVM=1` per agent (sticky).
- Attach a **writable** ext4 data drive per agent (created on first invocation from a
  template; see State below). Base rootfs + code drive stay read-only.
- Remove/raise the 30s vsock invocation deadline in [vsock.go](../pool-manager/vsock.go#L80)
  — agent turns run longer than a function.
- Eviction hook on the idle sweep ([manager.go](../pool-manager/manager.go#L452)): persist
  session, then detach + remove the ext4.
- Place VMs on the isolated **agent bridge** (extend the socket-prefix class scheme in
  [main.go](../pool-manager/main.go#L17); agent VMs get their own prefix + network).
- Endpoints forwarded into the VM: `POST /tick` (cron turn), `POST /message` (user turn).

### 2. In-VM runtime (Node + Pi SDK)

Single agent image, deployed as one code drive; agents differ only by state/config.

- On boot: `SessionManager.open('/data/session.jsonl')` (the writable ext4), create one live
  `AgentSession`.
- `POST /tick` → `session.prompt(<autonomous tick instructions>)`, then report the next
  `next_run_at` back to the control plane.
- `POST /message` → if idle, `session.prompt(userMsg)`; if a turn is in flight,
  `session.steer(userMsg)` so the user interjects into the running turn instead of blocking.
- Serialization: Pi's `followUp()` queues turns; `steer()` injects mid-turn. This **is** the
  in-VM queue — no manager-side lock.
- Streaming out via `session.subscribe(...)` → SSE.
- Persist `session.jsonl` after **every** turn.
- **Model calls go to the agent-broker over vsock**, never directly to Anthropic. The
  provider key is never present in the VM.

### 3. agent-broker (Go — one per host, *is* the proxy)

The broker runs on the host and is the only thing that talks to the model providers.

- **Multi-provider.** A `Provider` interface (providers.go) isolates the vendor-specific bits —
  upstream URL, auth header, metering, usage parsing, error shape. `anthropic.go` and `openai.go`
  implement it; the provider-agnostic flow lives in `proxy.go`. Each registered VM is routed to a
  provider by its `runtime` (claude→anthropic, chatgpt→openai), sent at registration. Usage is
  normalized into four disjoint token classes so the USD cap spans providers unchanged (the OpenAI
  adapter subtracts cached from prompt tokens and injects `stream_options.include_usage`).
- **Host-asserted identity**: the broker knows which VM opened the vsock connection (it owns
  the per-VM socket). The guest asserts nothing and cannot impersonate another VM.
- Holds the **single Anthropic API key**.
- Per request: resolve `VM → agent → user` (Postgres) → atomic cap reserve → forward to
  Anthropic → parse usage → reconcile → write audit. See lifecycle below.

### 4. Control plane (Postgres)

Migration `database/migrations/20260721000010_serverless_agents.sql`.

**State and compute are separate entities.** `storage` is the durable, user-facing thing (an
agent's memory + workspace) and holds the **mount lease**, because storage is the single-writer
resource. `agents` is a runnable bot: which engine build (`runtime`) to boot, attached to a
`storage`, plus its schedule. Switching provider = change `runtime`, or point a different agent
at the same storage — the storage lease guarantees only one runs at a time.

```sql
-- Durable state + MOUNT LEASE. lease_until unexpired => attached to a live VM
-- right now; self-expiring, so a crashed host's storage frees itself after TTL.
CREATE TABLE storage (
  id            TEXT PRIMARY KEY,          -- stor-<uuid>
  name          TEXT NOT NULL,
  user_id       TEXT,
  owner_host    TEXT,                      -- host whose VM has it mounted
  vsock_path    TEXT,                      -- that VM's UDS (broker identity)
  lease_until   TIMESTAMPTZ,               -- mount lease; keyed on STORAGE
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted_at    TIMESTAMPTZ
);

-- Runnable bot: engine build + storage attachment + schedule.
CREATE TABLE agents (
  id            TEXT PRIMARY KEY,          -- agt-<uuid>
  name          TEXT NOT NULL,
  user_id       TEXT,
  storage_id    TEXT REFERENCES storage(id),  -- the attachment
  runtime       TEXT NOT NULL DEFAULT 'claude', -- engine build image to boot
  config        JSONB NOT NULL DEFAULT '{}',
  next_run_at   TIMESTAMPTZ,              -- when the dispatcher should tick it
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted_at    TIMESTAMPTZ
);
CREATE INDEX agents_due_idx ON agents (next_run_at) WHERE next_run_at IS NOT NULL;

-- Live per-user spend counter (anchored 5h window, resets lazily in place)
CREATE TABLE usage_ledger (
  user_id       TEXT PRIMARY KEY,
  window_start  TIMESTAMPTZ NOT NULL DEFAULT now(),
  spent         NUMERIC(12,6) NOT NULL DEFAULT 0,   -- USD
  reserved      NUMERIC(12,6) NOT NULL DEFAULT 0,   -- in-flight reservations
  cap           NUMERIC(12,6) NOT NULL DEFAULT 2.00 -- $2 for now
);

-- Permanent per-call record (billing history + audit)
CREATE TABLE agent_usage (
  id            BIGSERIAL PRIMARY KEY,
  agent_id      TEXT NOT NULL,
  user_id       TEXT NOT NULL,
  model         TEXT NOT NULL,
  input_tokens        INT NOT NULL DEFAULT 0,
  output_tokens       INT NOT NULL DEFAULT 0,
  cache_read_tokens   INT NOT NULL DEFAULT 0,
  cache_write_tokens  INT NOT NULL DEFAULT 0,
  cost          NUMERIC(12,6) NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX agent_usage_user_time_idx ON agent_usage (user_id, created_at);
```

### 5. Cron dispatcher

Runs ~once/minute: `SELECT id FROM agents WHERE next_run_at <= now()` and `POST /tick` to
each. The agent sets its own next `next_run_at` (adaptive — seconds to hours), so idle agents
cost nothing and the cadence isn't a fixed blanket sweep.

## State & persistence

Two parts with different durability needs:

| Part            | Where                | Durability                         |
|-----------------|----------------------|------------------------------------|
| Session (memory)| `session.jsonl`      | **Source of truth**, persisted every turn (→ B2/Postgres) |
| Workspace (files)| writable ext4       | Reconstructible **cache** (git clone + replay) |

- **First invocation:** create the ext4 from a template, attach as the data drive.
- **While warm:** session held in the live `AgentSession`, flushed to disk each turn.
- **Eviction (idle sweep):** persist session JSONL, then remove the ext4.
- **Cold start:** restore session JSONL if present, else fresh; workspace re-hydrated from
  git if needed.
- **B2 backup of the workspace is deferred** (maybe never — it's reconstructible). Only the
  small session JSONL must never be lost.

Note: the deploy-time snapshot ([manager.go](../pool-manager/manager.go#L316)) warms the
*runtime*, not per-agent state — the agent always loads its own state from the writable drive.

## Model-call lifecycle (agent-broker)

1. **Identify** — VM id from the vsock connection → `agent → user` (Postgres).
2. **Reserve** — atomic roll-then-reserve (see cap function). No admit → return 429 over
   vsock; the VM surfaces "capped" to the caller.
3. **Forward** — call Anthropic with the single key. If streaming, parse the SSE stream —
   usage is in the final `message_delta`/`message_stop` event, not blind-piped.
4. **Reconcile** — compute actual cost from all token classes (input, output, cache read,
   cache write) via a per-model price table; `spent += actual; reserved -= est`.
5. **Audit** — insert the `agent_usage` row.

## Spend cap

- **Per user**, **$2**, over an **anchored 5-hour window**.
- Window anchors on first use and resets **lazily at check time** (no cron reset job). A user
  whose window expired gets `spent`/`reserved` zeroed and `window_start = now()` on their next
  request.
- Enforcement is **DB-per-request** (no Redis until the DB is a hot spot), done as a single
  atomic function so the window-roll and the cap check can't interleave:

```sql
CREATE FUNCTION reserve_usage(p_user TEXT, p_est NUMERIC)
RETURNS BOOLEAN AS $$
DECLARE rolled BOOLEAN;
BEGIN
  -- upsert row for new users
  INSERT INTO usage_ledger (user_id) VALUES (p_user)
    ON CONFLICT (user_id) DO NOTHING;

  SELECT (now() >= window_start + interval '5 hours')
    INTO rolled
    FROM usage_ledger WHERE user_id = p_user FOR UPDATE;

  IF rolled THEN
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
```

- **Reserve estimate:** flat conservative amount per call (~$0.10) to stop concurrent calls
  slipping past $2 before actuals land; reconcile to the real cost after the response.
- **Rejection UX:** user turn → tell the user they're capped; cron tick → agent no-ops and the
  dispatcher backs off `next_run_at`.

## Security model

- **Provider key never enters the VM.** The broker holds it and makes the outbound call, so a
  compromised agent cannot read or exfiltrate it (no `env`, no file, no process memory dump
  reaches it).
- **Identity is host-asserted** via the vsock connection — a guest cannot claim another VM's
  identity, so cap attribution and the audit log can't be forged.
- **Agent VMs get full internet** (for tool use). This is safe for the key (never in the VM),
  but means: **put no other secrets in an agent VM** — DB creds, B2 keys, etc. stay host-side
  or come through the broker. Cross-tenant isolation still holds via Firecracker + one VM per
  agent.
- Single Anthropic key is fine because it lives only in the broker. Optional defense-in-depth:
  an Anthropic **Workspace spend limit** as a dumb org-wide ceiling behind the proxy, so an
  accounting bug can't run the master key away.

## Networking

- Agent VMs on their own bridge, separate from function VMs (pool-manager) and instance VMs
  (`fc-inst-`, [main.go](../pool-manager/main.go#L17)).
- The broker listens on **vsock only** — not reachable over the network, from the internet, or
  from function/instance VMs.
- Model-call egress: none needed from the VM (the host broker makes the call). Tool egress:
  full internet, per the decision above.

## Deferred / future

- B2 backup of the workspace (reconstructible cache — only add if re-hydration proves costly).
- Redis hot counter + background DB reconcile (only if DB-per-request becomes a hot spot; see
  the LiteLLM pattern).
- Per-agent sub-caps under the per-user cap (so one runaway agent can't burn a user's budget).
- Configurable caps/windows per user (hardcoded $2 / 5h for now).
- Higher-assurance token isolation is already covered by the broker (chosen over per-VM
  token+IP).

## Build status

Implemented (needs remote `go build` + image builds to validate — no Go locally):

1. ✅ Migration — [`database/migrations/20260721000010_serverless_agents.sql`](../database/migrations/20260721000010_serverless_agents.sql):
   `agents`, `usage_ledger`, `agent_usage`, `reserve_usage()`, `settle_usage()`.
2. ✅ [`agent-broker/`](../agent-broker) — per-VM vsock listener → host-asserted identity →
   `reserve_usage` → forward to the provider (streaming + buffered usage capture) → `settle_usage`
   + `agent_usage` audit. **Multi-provider**: `anthropic.go` + `openai.go` behind a `Provider`
   interface, routed by the agent's `runtime`; provider-agnostic flow in `proxy.go`. Config-driven
   pricing (`prices.example.json`, Anthropic + OpenAI examples).
3. ✅ [`agent-manager/`](../agent-manager) — sticky one-VM-per-agent, **concurrent** invocations
   (so `/message` can steer a running `/tick`), writable ext4 keyed by **storage id** and kept
   on host across evictions, **storage-keyed mount lease** (so a claude build and a chatgpt
   build can't mount the same storage at once), engine image chosen by the agent's `runtime`,
   broker (de)register. Forked firecracker/vsock helpers (no 30s cap). Includes the
   agent [`bootstrap/`](../agent-manager/bootstrap) (mounts `/data`, runs the localhost→vsock
   egress proxy).
4. ✅ [`agent-runtime/`](../agent-runtime) — express + Pi SDK: `/tick`, `/message` (steer),
   session persistence to `/data/session.jsonl`, `next_run_seconds`.
5. ✅ [`agent-dispatcher/`](../agent-dispatcher) — claims due agents (`FOR UPDATE SKIP LOCKED`),
   POSTs `/tick`, writes back the agent's chosen `next_run_at`.

Not yet done:

6. ⬜ Frontend: per-user usage view (group `agent_usage` by user).
7. ⬜ Ops: build the agent base rootfs (embed the bootstrap) + the per-runtime engine images
   (`<AGENT_RUNTIME_DIR>/claude.ext4`, `chatgpt.ext4` — same source, `PROVIDER`/`MODEL` baked
   per build), agent network bridge, systemd units. B2 workspace backup remains deferred (state
   currently lives on the host data drive across evictions).
8. ⬜ Streaming `/message` responses to end users (the vsock invocation path is buffered; SSE
   to users is a v2).
```
