// In-VM agent runtime. One process = one agent = one live Pi AgentSession.
//
// - Model calls go to ANTHROPIC_BASE_URL / OPENAI_BASE_URL, which the agent
//   bootstrap points at the localhost->vsock egress proxy; the host broker
//   injects the real key, so no provider key lives here (a placeholder works).
// - Session state persists under DATA_DIR on the writable ext4, which survives
//   VM eviction on the host.
// - Concurrency: exactly one turn runs at a time. A /message arriving mid-turn
//   is steered into the running turn instead of blocking (Pi's steer()).
//
// Pi 1.3.0 is pure ESM and ships no CommonJS entry point, so this module is ESM
// and runs under Node's native type stripping (node >= 22.19, see package.json
// engines). There is no build step: the file is shipped as-is into the image.

import express from "express";
import type { Request, Response } from "express";
import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import {
	createAgentSession,
	ModelRuntime,
	SessionManager,
	type AgentSession,
	type AgentSessionEvent,
} from "@earendil-works/pi-coding-agent";

const PORT = parseInt(process.env.PORT || "3000", 10);
const DATA_DIR = process.env.DATA_DIR || "/data";

// The coding agent's working directory — what its read/bash/edit tools act on.
// Kept separate from session storage so the transcript is never a file the
// agent can clobber with its own tools.
const WORKSPACE_DIR = process.env.WORKSPACE_DIR || path.join(DATA_DIR, "workspace");
// Explicit session dir. Without this Pi derives a path by encoding cwd, which
// we neither control nor want to depend on across engine builds.
const SESSION_DIR = process.env.SESSION_DIR || path.join(DATA_DIR, "sessions");
const NEXT_RUN_PATH = path.join(DATA_DIR, "next_run"); // agent writes desired cadence here

const DEFAULT_NEXT_RUN = parseInt(process.env.DEFAULT_NEXT_RUN_SECONDS || "600", 10);
const TICK_PROMPT =
	process.env.TICK_PROMPT ||
	"Autonomous tick. Review your goals and any pending work, then take the next " +
		"useful action. If there is nothing to do right now, stop without acting.";

// PROVIDER is baked into the engine build image (claude.ext4 vs chatgpt.ext4)
// and doubles as Pi's provider id. MODEL must be baked alongside it: the broker
// prices each call from the `model` field in the request body, so leaving the
// choice to Pi's default would bill against a model that may not be in
// prices.json.
const PROVIDER = process.env.PROVIDER || "anthropic";
const MODEL = process.env.MODEL || "";

let session: AgentSession | null = null;
let busy = false;

async function initSession(): Promise<void> {
	fs.mkdirSync(WORKSPACE_DIR, { recursive: true });
	fs.mkdirSync(SESSION_DIR, { recursive: true });

	// Keep auth/model config on the data drive; $HOME in the VM is not writable
	// and Pi would otherwise default to ~/.pi/agent.
	const modelRuntime = await ModelRuntime.create({
		authPath: path.join(DATA_DIR, "auth.json"),
		modelsPath: path.join(DATA_DIR, "models.json"),
	});

	// Credential resolution, in order:
	//   1. AGENT_API_KEY      — provider-agnostic, so a local run can't miss by
	//                           naming the wrong vendor's variable.
	//   2. <PROVIDER>_API_KEY — the conventional name, matching the broker's env.
	//   3. "proxied"          — placeholder. This is the CORRECT value in the VM:
	//                           the broker injects real auth and no provider key
	//                           ever lives in the guest.
	// Because 3 is right in production and always wrong locally, the source is
	// logged below — otherwise "no key set" reaches the provider as a 401 that
	// reads like "bad key".
	const keyEnv = `${PROVIDER.toUpperCase()}_API_KEY`;
	const apiKey = process.env.AGENT_API_KEY || process.env[keyEnv] || "proxied";
	const keySource = process.env.AGENT_API_KEY
		? "AGENT_API_KEY"
		: process.env[keyEnv]
			? keyEnv
			: "placeholder (expects broker injection)";

	// Returns a promise: the credential write must land before session creation.
	await modelRuntime.setRuntimeApiKey(PROVIDER, apiKey);

	// MODEL is mandatory. Left unset, Pi resolves "from settings, else first
	// available", which ignores PROVIDER entirely — it will happily pick a
	// vendor we hold no key for, and bill against a model id the broker has no
	// price for. Requiring it here turns that silent misroute into a boot error.
	if (!MODEL) {
		throw new Error("MODEL is required (e.g. PROVIDER=openai MODEL=gpt-5.4)");
	}
	const model = modelRuntime.getModel(PROVIDER, MODEL);
	if (!model) {
		throw new Error(`MODEL ${PROVIDER}/${MODEL} not found in Pi's model registry`);
	}

	// continueRecent resumes the newest transcript in SESSION_DIR, or starts a
	// fresh one when the drive is new — the resume-or-create branch we need,
	// without probing for a session file whose path Pi owns.
	const sessionManager = SessionManager.continueRecent(WORKSPACE_DIR, SESSION_DIR);

	const { session: created, modelFallbackMessage } = await createAgentSession({
		cwd: WORKSPACE_DIR,
		sessionManager,
		modelRuntime,
		model,
	});
	session = created;

	if (modelFallbackMessage) {
		console.warn(`[runtime] model fallback: ${modelFallbackMessage}`);
	}
	console.log(
		`[runtime] session ready (provider=${PROVIDER} model=${MODEL} key=${keySource} ` +
			`file=${created.sessionFile ?? "none"})`,
	);
}

// runTurn drives one prompt to completion, collecting the assistant text.
//
// prompt() resolves even when the turn failed: a 401, a rate limit or an
// overload is recorded on the assistant message as stopReason "error" rather
// than thrown. Without the agent_end check below a dead turn looks identical to
// a silent one — HTTP 200, empty reply — and the dispatcher would keep
// rescheduling an agent that never actually ran.
async function runTurn(promptText: string): Promise<string> {
	if (!session) throw new Error("session not initialized");
	let text = "";
	let turnError: string | undefined;

	const unsubscribe = session.subscribe((event: AgentSessionEvent) => {
		if (event.type === "message_update" && event.assistantMessageEvent.type === "text_delta") {
			text += event.assistantMessageEvent.delta;
			return;
		}
		if (event.type === "agent_end") {
			for (const msg of event.messages) {
				if (msg.role === "assistant" && msg.stopReason === "error") {
					turnError = msg.errorMessage ?? "model call failed";
				}
			}
		}
	});

	try {
		await session.prompt(promptText);
	} finally {
		unsubscribe();
	}

	if (turnError) throw new Error(turnError);
	return text;
}

// readNextRun lets the agent set its own cadence by writing seconds to
// DATA_DIR/next_run (via its file tools). Falls back to the default.
function readNextRun(): number {
	try {
		const n = parseInt(fs.readFileSync(NEXT_RUN_PATH, "utf8").trim(), 10);
		if (Number.isFinite(n) && n > 0) return n;
	} catch {
		/* not set */
	}
	return DEFAULT_NEXT_RUN;
}

const app = express();
app.use(express.json({ limit: "1mb" }));

// Health probe used by the bootstrap's waitForApp.
app.get("/", (_req: Request, res: Response) => {
	res.json({ ok: true, ready: session !== null, provider: PROVIDER, model: MODEL || null });
});

// Autonomous turn from the dispatcher.
app.post("/tick", async (_req: Request, res: Response) => {
	if (!session) return res.status(503).json({ error: "session not ready" });
	if (busy) return res.json({ status: "busy", next_run_seconds: readNextRun() });
	busy = true;
	try {
		await runTurn(TICK_PROMPT);
		res.json({ status: "ok", next_run_seconds: readNextRun() });
	} catch (err) {
		console.error("[runtime] tick error:", err);
		res.status(500).json({ error: String(err), next_run_seconds: readNextRun() });
	} finally {
		busy = false;
	}
});

// User turn. If a turn is already running, steer into it rather than block.
app.post("/message", async (req: Request, res: Response) => {
	if (!session) return res.status(503).json({ error: "session not ready" });
	const message = req.body?.message;
	if (!message) return res.status(400).json({ error: "message required" });

	if (busy) {
		try {
			await session.steer(message);
			return res.json({ status: "steered" });
		} catch (err) {
			return res.status(500).json({ error: String(err) });
		}
	}

	busy = true;
	try {
		const reply = await runTurn(message);
		res.json({ status: "ok", reply, next_run_seconds: readNextRun() });
	} catch (err) {
		console.error("[runtime] message error:", err);
		res.status(500).json({ error: String(err) });
	} finally {
		busy = false;
	}
});

// Listen first so the bootstrap's readiness probe passes immediately; the Pi
// session initializes in the background and turns 503 until it's ready.
app.listen(PORT, "127.0.0.1", () => console.log(`[runtime] listening on :${PORT}`));

initSession().catch((err) => {
	console.error("[runtime] failed to init session:", err);
	// Leave the server up so the failure is observable via /tick 503s rather than
	// a boot loop; the VM will be evicted and retried.
});

// Flush session state on eviction — the manager stops VMs with SIGTERM.
for (const signal of ["SIGTERM", "SIGINT"] as const) {
	process.on(signal, () => {
		session?.dispose();
		process.exit(0);
	});
}
