# agent-runtime

The in-VM Node process for serverless agents — one live Pi `AgentSession` per
agent. Deployed as the single shared **runtime code image** (`AGENT_RUNTIME_IMAGE`
in agent-manager), mounted read-only at `/var/task`; the bootstrap runs
`/var/task/index.js`.

## Contract (called by agent-manager over vsock)

- `GET  /` — readiness probe (bootstrap waits on this).
- `POST /tick` — autonomous turn. Returns `{ next_run_seconds }`.
- `POST /message` `{ "message": "..." }` — user turn. If a turn is in flight the
  message is `steer()`ed into it; otherwise it runs as a new turn. Returns
  `{ reply, next_run_seconds }`.

## State

- Session persists to `/data/session.jsonl` on the writable data drive (survives
  VM eviction on the host).
- The agent sets its own cadence by writing seconds to `/data/next_run`; absent
  that, `DEFAULT_NEXT_RUN_SECONDS` (600) is used.

## Model calls

`ANTHROPIC_BASE_URL` is set by the bootstrap to the localhost→vsock egress proxy
(`http://127.0.0.1:8081`). The host broker injects the real key, so the API key
here is a placeholder. No provider key ever lives in the VM.

## Building the image

Bundle this directory **with `node_modules`** into an ext4 image the same way
functions are built (see artifact-store), then point `AGENT_RUNTIME_IMAGE` at it:

```
npm ci --omit=dev        # install express + @earendil-works/pi-coding-agent
# package dir -> ext4 (mkfs.ext4 + copy), as the artifact-store does for functions
```

> The Pi SDK calls (`createAgentSession`, `SessionManager.open/create`,
> `ModelRuntime`, `session.prompt/steer/subscribe`) follow https://pi.dev/docs.
> Confirm them against the installed `@earendil-works/pi-coding-agent` version —
> they're isolated in `initSession()` and `runTurn()` in index.js.
