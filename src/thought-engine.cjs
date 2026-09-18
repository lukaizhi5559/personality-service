'use strict';

/**
 * ThoughtEngine — "Artificial Alive AI" engine for ThinkDrop.
 *
 * Persistent scored Thoughts accumulate cross-modal evidence (prompts, screen
 * dwell, memory, silence) as decaying activation traces; when a thought's score
 * crosses THOUGHT_TRIGGER_SCORE it becomes a Trigger and a constrained LLM
 * assigns an action from a closed vocabulary (notify|question|prompt|skill|
 * remember|watch|skip). Delivery is gated on user presence — actions taken
 * while the user is inactive are held and delivered on wake.
 *
 * Silence is a first-class input with its own taxonomy: attributable silence
 * (assistant's question unanswered) may nudge with escalating tone; held-turn
 * pauses ("one moment", "brb") are suppressed entirely; post-answer lapses are
 * weak; busy silence (no prompts but screen active) never produces silence
 * thoughts; true idle runs an insight scan over episodic/memory.
 *
 * Built on research: ACT-R activation (traces + power-law decay + retrieval
 * threshold), Horvitz attention-sensitive alerting (interruption vs deferral
 * cost), Sacks/Schegloff/Jefferson turn-taking (silence taxonomy), Generative
 * Agents (reflection on idle), JITIR (proactive info must be accessible yet
 * ignorable — card-first delivery).
 */

const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');
const heartbeat = require('./heartbeat.cjs');
const { getOverlay, getMoodContext } = require('./personality-doc.cjs');

// ---------------------------------------------------------------------------
// Config (all env-tunable)
// ---------------------------------------------------------------------------
const ENABLED = process.env.THOUGHT_ENGINE_ENABLED !== 'false';
const SHADOW = process.env.THOUGHT_SHADOW === 'true'; // log would-be triggers, never act
// Whitelist of action types that execute for real even under shadow mode —
// e.g. THOUGHT_LIVE_ACTIONS=notify,question delivers alerts while prompt/skill
// stay shadowed. Approval gating still applies on top.
const LIVE_ACTIONS = new Set(
  (process.env.THOUGHT_LIVE_ACTIONS || '').split(',').map(s => s.trim()).filter(Boolean));
const USER_ID = 'local_user';

const TICK_MS = parseInt(process.env.THOUGHT_TICK_MS, 10) || 60000;
const TRIGGER_SCORE = parseFloat(process.env.THOUGHT_TRIGGER_SCORE || '1.0');
const TRIGGER_COOLDOWN_MS = parseInt(process.env.THOUGHT_TRIGGER_COOLDOWN_MS, 10) || 15 * 60 * 1000;
const MAX_TRIGGERS_PER_TICK = 1;
// Shadow mode never updates a thought's status, so without a per-thought
// re-arm the highest-score thought re-fires every tick and starves the rest.
const SHADOW_REARM_MS = parseInt(process.env.THOUGHT_SHADOW_REARM_MS, 10) || 60 * 60 * 1000;

// Silence taxonomy windows
const SILENCE_MS = parseInt(process.env.THOUGHT_SILENCE_MS, 10) || 3 * 60 * 1000;   // quiet since last prompt
const IDLE_MS = parseInt(process.env.THOUGHT_IDLE_MS, 10) || 15 * 60 * 1000;        // no prompt AND no monitor events
const PAUSE_SUPPRESS_MS = parseInt(process.env.THOUGHT_PAUSE_SUPPRESS_MS, 10) || 10 * 60 * 1000;
const MAX_SILENCE_EPISODES = 3; // escalation cap — humans stop saying "hello?" too

// Presence: user considered active if any input/monitor event within this window
const PRESENCE_MS = parseInt(process.env.THOUGHT_PRESENCE_MS, 10) || 7 * 60 * 1000;

// Initial trace weights by producing input
const TRACE_WEIGHT = {
  prompt: parseFloat(process.env.THOUGHT_W_PROMPT || '0.5'),
  screen_capture: parseFloat(process.env.THOUGHT_W_SCREEN || '0.35'),
  memory: parseFloat(process.env.THOUGHT_W_MEMORY || '0.4'),
  queue: parseFloat(process.env.THOUGHT_W_QUEUE || '0.4'),
  silence: parseFloat(process.env.THOUGHT_W_SILENCE || '0.4'),
  judgment: 0.15,
};
// High time-sensitivity candidates jump the queue — a download watched too late
// is worthless (Horvitz: cost of deferral). Large initial trace so a second
// observation crosses τ within minutes.
const TIME_SENSITIVE_WEIGHT = parseFloat(process.env.THOUGHT_W_URGENT || '0.7');

// Idle insight scan: how much episodic history to sample
const IDLE_SCAN_LIMIT = parseInt(process.env.THOUGHT_IDLE_SCAN_LIMIT, 10) || 40;
const IDLE_SCAN_MAX_CANDIDATES = 3;

const MEM_PORT = process.env.THOUGHT_MEM_PORT || 3001;
const CONV_PORT = process.env.THOUGHT_CONV_PORT || 3004;

// Watches — conditions monitored until they resolve or expire
const WATCH_MAX = parseInt(process.env.THOUGHT_WATCH_MAX, 10) || 5;
const WATCH_TTL_MS = parseInt(process.env.THOUGHT_WATCH_TTL_MS, 10) || 2 * 60 * 60 * 1000;
const WATCH_STILL_MS = parseInt(process.env.THOUGHT_WATCH_STILL_MS, 10) || 2 * 60 * 1000;
// Prompt phrasing that means "watch this for me" — explicit intent, no scoring.
const WATCH_INTENT_RE = /\b(watch|monitor|keep an eye on)\b|\b(let me know|tell me|ping me|notify me|remind me)\b[^.]*\b(when|in\s+\d|at\s+\d)/i;

// Quiet hours "23-7": voice deliveries held in-window; cards still emit.
const QUIET_HOURS = process.env.THOUGHT_QUIET_HOURS || '';
function inQuietHours() {
  const m = QUIET_HOURS.match(/^(\d{1,2})\s*-\s*(\d{1,2})$/);
  if (!m) return false;
  const h = new Date().getHours(), s = +m[1], e = +m[2];
  return s <= e ? (h >= s && h < e) : (h >= s || h < e);
}

// Per-input hourly caps bound producer storms (dwell/queue/memory fire-and-forget)
const INPUT_CAP_HOURLY = parseInt(process.env.THOUGHT_INPUT_CAP_HOURLY, 10) || 30;
const CAPPED_INPUTS = new Set(['dwell', 'queue', 'memory']);
const VOICE_PORT = process.env.THOUGHT_VOICE_PORT || 3006;
const COMMS_PORT = process.env.THOUGHT_COMMS_PORT || 3015;
const MAIN_PORT = process.env.THOUGHT_MAIN_PORT || 3010;

// Held-turn pause cues — silence after these is suppressed entirely (S2)
const PAUSE_RE = /\b(one moment|just a (sec|second|moment|min)|be right back|brb|hold on|give me a (sec|second|minute|moment)|gimme a (sec|second|minute)|wait a (sec|second|minute|moment)|back in a (sec|minute|bit|flash)|afk)\b/i;

// OCR noise to strip before extraction/embedding — volatile UI text would
// otherwise make the same error dialog embed differently day to day.
const OCR_NOISE_RE = /\b\d{1,2}:\d{2}(:\d{2})?(\s?[AP]M)?\b|\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b|\b0x[0-9a-f]+\b|\b\d{5,}\b/gi;

const ACTION_TYPES = ['notify', 'question', 'prompt', 'skill', 'remember', 'watch', 'skip'];
// Phase-1 auto-allowed actions — no side effects beyond talking/storing memory.
// prompt/skill/watch require approval until Phase-2 tiering is tuned.
const AUTO_ALLOWED = new Set(['notify', 'question', 'remember', 'skip']);

// ---------------------------------------------------------------------------
// Engine state
// ---------------------------------------------------------------------------
const state = {
  started: false,
  tickTimer: null,
  lastUserPromptAt: 0,
  lastUserText: '',
  lastSessionId: null,
  lastMonitorEventAt: 0,
  lastIdleScanAt: 0,
  lastTriggerAt: 0,
  silenceEpisode: 0,        // consecutive unanswered-silence episodes
  nextSilenceEvalAt: 0,     // re-arm cadence — continued quiet re-evaluates each window
  pauseSuppressedUntil: 0,
  pendingDeliveries: [],    // held notify/question payloads (deliver-on-wake)
  recentNudges: [],         // phrasing history for escalation
  shadowLog: [],
  shadowFiredAt: new Map(), // thoughtId → last shadow-eval ms (re-trigger cooldown)
  taskToThought: new Map(), // comms taskId → {thoughtId, outDir} for artifact correlation
  watches: new Map(),       // thoughtId → watch spec (hydrated from 'watching' rows)
  inputTimes: new Map(),    // input type → [timestamps] for hourly rate caps
};

function now() { return Date.now(); }

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------
function postJson(port, path, body, timeoutMs = 30000) {
  return new Promise((resolve) => {
    const data = JSON.stringify(body || {});
    const req = http.request({
      hostname: 'localhost', port, path, method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) },
    }, (res) => {
      let raw = '';
      res.on('data', c => (raw += c));
      res.on('end', () => {
        try { resolve(JSON.parse(raw)); } catch (_) { resolve(null); }
      });
    });
    req.setTimeout(timeoutMs, () => { try { req.destroy(); } catch (_) {} resolve(null); });
    req.on('error', () => resolve(null));
    req.write(data);
    req.end();
  });
}

const convPost = (action, payload) => postJson(CONV_PORT, `/${action}`, {
  version: 'mcp.v1', service: 'conversation', action, payload, requestId: `thought-${Date.now()}`,
});
const commsPost = (path, body) => postJson(COMMS_PORT, path, body);
const mainPost = (path, body) => postJson(MAIN_PORT, path, body, 5000);

function commsGet(path, timeoutMs = 8000) {
  return new Promise((resolve) => {
    const req = http.get({ hostname: '127.0.0.1', port: COMMS_PORT, path }, (res) => {
      let raw = '';
      res.on('data', (c) => { raw += c; });
      res.on('end', () => { try { resolve(JSON.parse(raw)); } catch (_) { resolve(null); } });
    });
    req.setTimeout(timeoutMs, () => { try { req.destroy(); } catch (_) {} resolve(null); });
    req.on('error', () => resolve(null));
  });
}

// ---------------------------------------------------------------------------
// Presence + event emission
// ---------------------------------------------------------------------------
function userActive() {
  return now() - Math.max(state.lastUserPromptAt, state.lastMonitorEventAt) < PRESENCE_MS;
}

/** Forward thought lifecycle events to the Electron overlay (Brain tab). */
function emitThoughtEvent(kind, thought, extra) {
  mainPost('/thought.event', { kind, thought, ...(extra || {}) }).catch(() => {});
}

// ---------------------------------------------------------------------------
// Input handling — POST /thought.input
// ---------------------------------------------------------------------------

/**
 * Unified input entrypoint.
 *   { type: 'prompt', text, sessionId? }
 *   { type: 'dwell',  app, filePath?, windowTitle?, url?, dwellMs, startedAt }
 *   { type: 'monitor', eventType, info }   — presence heartbeat from monitor
 *   { type: 'queue',  ... }                — Phase 2
 */
async function handleInput(body) {
  if (!ENABLED) return { ok: false, reason: 'disabled' };
  const type = body?.type;
  try {
    // Rate-cap high-frequency producers before spending an extraction call
    if (CAPPED_INPUTS.has(type)) {
      const arr = state.inputTimes.get(type) || [];
      const cutoff = now() - 3600000;
      while (arr.length && arr[0] < cutoff) arr.shift();
      if (arr.length >= INPUT_CAP_HOURLY) return { ok: false, reason: 'rate_capped' };
      arr.push(now());
      state.inputTimes.set(type, arr);
    }
    if (type === 'prompt') return await onPrompt(body);
    if (type === 'dwell') return await onDwell(body);
    if (type === 'monitor') return onMonitor(body);
    if (type === 'queue') return await onQueue(body);
    if (type === 'memory') return await onMemory(body);
    return { ok: false, reason: 'unknown_type' };
  } catch (e) {
    console.warn(`[ThoughtEngine] handleInput(${type}) failed:`, e.message);
    return { ok: false, reason: e.message };
  }
}

async function onPrompt({ text, sessionId }) {
  if (!text || !text.trim()) return { ok: false, reason: 'empty' };
  state.lastUserPromptAt = now();
  state.lastUserText = text;
  if (sessionId) state.lastSessionId = sessionId;
  // Any user input ends the quiet period and resets the escalation chain
  state.silenceEpisode = 0;
  state.nextSilenceEvalAt = 0;
  state.recentNudges = [];
  if (PAUSE_RE.test(text)) {
    state.pauseSuppressedUntil = now() + PAUSE_SUPPRESS_MS;
  }
  await flushPending();

  // Explicit watch request — a user command, not a proactive trigger, so it
  // runs even in shadow mode (shadow only suppresses autonomous actions).
  if (WATCH_INTENT_RE.test(text)) {
    const watchId = await createExplicitWatch(text);
    if (watchId) return { ok: true, watch: true, thoughtId: watchId };
    // parse failed or cap reached → fall through to normal candidate path
  }

  const cand = await extractCandidate('prompt', text);
  if (!cand) return { ok: false, reason: 'extract_null' };
  const result = await upsertCandidate('prompt', cand, { srcIds: [sessionId].filter(Boolean) });
  return { ok: true, ...result };
}

/** Fetch recent episodic memories. {ok:true, memories} or {ok:false} — an API
 *  failure is NEVER the same as "no captures" (pixel_still must not false-resolve). */
async function episodicRecent({ limit = 100, maxAgeDays = 1 } = {}) {
  const res = await heartbeat.memPost('episodic.recent', { limit, maxAgeDays });
  if (!res || (res.status && res.status !== 'ok')) return { ok: false };
  const memories = res.data?.memories || res.memories;
  return Array.isArray(memories) ? { ok: true, memories } : { ok: false };
}

/** Client-side filter: screen captures at/after sinceMs, optionally app-scoped. */
function capturesSince(memories, sinceMs, app) {
  return memories.filter(m => {
    if (m.type && m.type !== 'screen_capture') return false;
    if (app) {
      const mApp = m.metadata?.appName || m.metadata?.app;
      if (!mApp) return false;
      const a = String(app).toLowerCase(), b = String(mApp).toLowerCase();
      if (!b.includes(a) && !a.includes(b)) return false;
    }
    if (sinceMs) {
      const t = Date.parse(m.created_at || '');
      if (Number.isFinite(t) && t < sinceMs) return false;
    }
    return true;
  });
}

async function onDwell({ app, filePath, windowTitle, url, dwellMs, startedAt }) {
  state.lastMonitorEventAt = now();
  await flushPending();

  // Sample episodic captures from this app during the dwell window
  const r = await episodicRecent({ limit: 100 });
  if (!r.ok) return { ok: false, reason: 'episodic_unavailable' };
  const memories = capturesSince(r.memories, startedAt || (now() - (dwellMs || 0)), app).slice(0, 8);
  if (!memories.length) return { ok: false, reason: 'no_episodic' };

  const ocrBlob = memories
    .map(m => (m.metadata && (m.metadata.ocrText || m.metadata.ocr_text)) || m.text || '')
    .join('\n')
    .slice(0, 4000);
  const normalized = ocrBlob.replace(OCR_NOISE_RE, ' ').replace(/\s+/g, ' ').trim();
  if (normalized.length < 40) return { ok: false, reason: 'ocr_thin' };

  const contextText = [
    `The user has been dwelling in the app "${app}" for ${Math.round((dwellMs || 0) / 60000)}+ minutes.`,
    filePath ? `File: ${filePath}` : '',
    windowTitle ? `Window title: ${windowTitle}` : '',
    url ? `URL: ${url}` : '',
    `Recent on-screen text:\n${normalized}`,
  ].filter(Boolean).join('\n');

  const cand = await extractCandidate('screen', contextText);
  if (!cand) return { ok: false, reason: 'extract_null' };
  const srcIds = memories.map(m => m.id).filter(Boolean);
  const result = await upsertCandidate('screen_capture', cand, { srcIds });
  return { ok: true, ...result };
}

/** Task terminal states from comms-graph taskJournal — awareness of own work. */
async function onQueue({ text, taskId, status, result }) {
  if (!text) return { ok: false, reason: 'empty' };
  // Task-completion correlation: if this task was dispatched by a thought's
  // prompt action, stamp its outcome/artifacts back onto the thought.
  if (taskId) await correlateTaskResult(taskId, status, result);

  const cand = await extractCandidate('queue', `${text}${result ? `\nResult: ${result}` : ''}`);
  if (!cand) return { ok: false, reason: 'extract_null' };
  return { ok: true, ...(await upsertCandidate('queue', cand, { srcIds: [taskId].filter(Boolean) })) };
}

/** Newly stored user memories — things worth remembering feed accumulation. */
async function onMemory({ text, id }) {
  if (!text || !text.trim()) return { ok: false, reason: 'empty' };
  const cand = await extractCandidate('memory', text);
  if (!cand) return { ok: false, reason: 'extract_null' };
  return { ok: true, ...(await upsertCandidate('memory', cand, { srcIds: [id].filter(Boolean) })) };
}

function onMonitor({ eventType, info }) {
  state.lastMonitorEventAt = now();
  // A monitor event while deliveries are pending means the user is back at the
  // machine — flush held notifications (deliver-on-wake).
  flushPending().catch(() => {});
  return { ok: true, eventType };
}

// ---------------------------------------------------------------------------
// Candidate extraction — askLLM over the raw context
// ---------------------------------------------------------------------------

/**
 * One compact LLM call per candidate: summary + entities + actions + context
 * judgment ({contextScore, timeSensitivity}). contextScore≈0 suppresses
 * meaningless input (pause acknowledgements, OCR noise); high timeSensitivity
 * produces a large initial trace so urgent thoughts trigger within minutes.
 */
async function extractCandidate(sourceKind, text) {
  const truncated = String(text).slice(0, 6000);
  const prompt = `You extract a concise "thought candidate" from ${sourceKind} input observed on the user's computer.
Return JSON ONLY:
{
  "summary": "one sentence: what the user is doing or interested in",
  "entities": ["key nouns — people, orgs, products, projects, topics (max 6)"],
  "actions": ["verbs — what the user is doing (max 4)"],
  "contextScore": 0.0-1.0,
  "timeSensitive": true|false
}
Rules:
- contextScore: 0 if the input is trivial/noise (acknowledgements like "ok", "one moment"; idle screens; login screens). 1.0 = clearly meaningful activity worth remembering.
- timeSensitive: true ONLY if the state is transient and resolves within minutes (download/build/generation in progress, payment processing, error dialog, live call). Interests and research are NOT time-sensitive.
- summary must be specific, never generic ("user browsing a page" → score 0).`;

  const out = await heartbeat.askLLMJson(prompt, truncated);
  if (!out || typeof out !== 'object') return null;
  if (!out.summary || typeof out.summary !== 'string') return null;
  return {
    summary: out.summary.trim(),
    entityNames: Array.isArray(out.entities) ? out.entities.slice(0, 8).map(String) : [],
    actionNames: Array.isArray(out.actions) ? out.actions.slice(0, 6).map(String) : [],
    contextScore: Math.max(0, Math.min(1, Number(out.contextScore ?? 0.5))),
    timeSensitive: out.timeSensitive === true,
  };
}

async function upsertCandidate(input, cand, { srcIds = [], silenceEpisode = 0, forceNew = false } = {}) {
  // Time-critical candidates enter with a big trace; ordinary ones scale by
  // context judgment. contextScore≈0 (pauses, noise) makes the trace ~0.
  let w = TRACE_WEIGHT[input] ?? 0.3;
  w *= Math.max(0.05, cand.contextScore);
  if (cand.timeSensitive) w = Math.max(w, TIME_SENSITIVE_WEIGHT * cand.contextScore);
  // Silence escalation: each continued unanswered episode weighs more — a
  // single stretch stays a watched thought, sustained silence crosses τ.
  if (input === 'silence' && silenceEpisode > 1) {
    w *= Math.min(2, 1 + 0.5 * (silenceEpisode - 1));
  }

  const res = await heartbeat.memPost('thought.upsert', {
    input,
    summary: cand.summary,
    entityNames: cand.entityNames,
    actionNames: cand.actionNames,
    sourceIds: srcIds,
    userId: USER_ID,
    traceWeight: w,
    silenceEpisode,
    forceNew,
  });
  const data = res?.data || res;
  if (data?.thought) {
    emitThoughtEvent(data.matched ? 'reinforced' : 'created', data.thought);
  }
  return data || { matched: false };
}

// ---------------------------------------------------------------------------
// Silence producer — first-class input with a turn-taking taxonomy
// ---------------------------------------------------------------------------

/**
 * Six silence states (Sacks/Schegloff/Jefferson taxonomy):
 *   attributable      assistant asked a question, user didn't answer → nudge-eligible
 *   held-turn pause   "one moment"/"brb" → suppressed (the only correct behavior: wait)
 *   post-answer lapse natural conversation close → weak, quickly-decaying thought at most
 *   busy silence      no prompts but monitor shows active work → NEVER a silence thought
 *   true idle         no prompts AND no monitor events → reflection/insight scan
 *   extended absence  user away/asleep → triggered actions execute, delivery held
 */
async function silenceTick() {
  const t = now();
  const sincePrompt = t - state.lastUserPromptAt;
  const sinceMonitor = t - state.lastMonitorEventAt;

  // ── Held-turn pause: user said "one moment" — wait, do not interpret ───
  if (t < state.pauseSuppressedUntil) return;

  // ── Busy silence: comms quiet but the machine is active — heads-down
  //    work. Screen input owns this; never nudge a focused user. ──────────
  const busyWorking = sincePrompt > SILENCE_MS && sinceMonitor < SILENCE_MS;
  if (busyWorking) return;

  // ── True idle / extended absence: no comms AND no screen activity →
  //    silence-as-occasion: run the reflection/insight scan (rate-limited) ─
  if (sincePrompt > IDLE_MS && sinceMonitor > IDLE_MS) {
    if (t - state.lastIdleScanAt > IDLE_MS) {
      state.lastIdleScanAt = t;
      await idleInsightScan();
    }
    return;
  }

  // ── Conversational silence: quiet since last prompt, not held, user
  //    plausibly still around. Re-arm cadence: each SILENCE_MS window of
  //    continued quiet advances the episode so silence can escalate. ─────
  if (!state.lastUserPromptAt || sincePrompt < SILENCE_MS || t < state.nextSilenceEvalAt) return;
  if (state.silenceEpisode >= MAX_SILENCE_EPISODES) return; // chain capped → lapse

  // Attributable check: did the assistant's last turn end in a question?
  const lastAssistant = await fetchLastAssistantTurn();
  const attributable = /\?\s*$/.test(lastAssistant || '');
  // Post-answer lapse (assistant gave info, no question) is weak — context
  // scorer also damps it, but bias low from the start.
  const baseContext = attributable ? 0.7 : 0.25;

  state.nextSilenceEvalAt = t + SILENCE_MS;
  state.silenceEpisode += 1;
  const episode = state.silenceEpisode;

  const contextText = `The user's last message was "${state.lastUserText.slice(0, 300)}". ` +
    (lastAssistant ? `The assistant's last reply ended with: "${lastAssistant.slice(-300)}". ` : '') +
    `${Math.round(sincePrompt / 60000)}+ minutes of silence have passed. ` +
    `This is silence episode #${episode} (1 = first unanswered stretch, higher = repeated). ` +
    (episode > 1 ? `Previous nudges already sent: ${state.recentNudges.join(' | ') || 'none'}. ` : '') +
    `Is this silence meaningful — an unanswered question the user may want followed up on (score high), ` +
    `or a natural end of conversation (score ~0)?`;

  const cand = await extractCandidate('silence', contextText);
  if (!cand) return;
  // Blended context: attributable silence is the only kind with real weight
  cand.contextScore = Math.min(1, cand.contextScore * 0.5 + baseContext * 0.5);
  const summary = episode > 1
    ? `User still hasn't responded (silence episode ${episode})${attributable ? ' — assistant question unanswered' : ''}`
    : `User hasn't responded${attributable ? ' — assistant question unanswered' : ''}`;

  await upsertCandidate('silence', {
    ...cand,
    summary,
    // Silence quotes the user's last message, so extraction inherits topic
    // entities — drop them or silence evidence cross-merges into topic thoughts.
    entityNames: [],
    actionNames: cand.actionNames,
  }, { silenceEpisode: episode, forceNew: state.recentNudges.length > 0 });
}

async function fetchLastAssistantTurn() {
  try {
    if (!state.lastSessionId) return '';
    const res = await convPost('message.list', {
      sessionId: state.lastSessionId, limit: 6, direction: 'DESC',
    });
    const msgs = res?.data?.messages || res?.result?.messages || [];
    // direction DESC → newest first; find the most recent assistant turn
    const lastAssistant = msgs.find(m =>
      m.role === 'assistant' || m.author === 'assistant' || m.sender === 'assistant');
    return lastAssistant?.content || lastAssistant?.text || '';
  } catch (_) {
    return '';
  }
}

/**
 * Silence-as-occasion: on true idle, reflect over recent episodic memory and
 * produce topical candidates ("user keeps circling Tokyo flights"). The
 * thought's subject is the *content*, not the silence itself.
 */
async function idleInsightScan() {
  const res = await heartbeat.memPost('episodic.recent', { limit: IDLE_SCAN_LIMIT });
  const memories = res?.data?.memories || res?.memories || [];
  if (!memories.length) return;

  const digest = memories.slice(0, IDLE_SCAN_LIMIT).map(m => {
    const md = m.metadata || {};
    return `- [${md.appName || 'app'}] ${(m.text || '').slice(0, 160)}`;
  }).join('\n');

  const prompt = `You are scanning a user's recent screen activity while they are idle, looking for sustained interests or unfinished business worth remembering.
Recent activity:
${digest}

Identify up to ${IDLE_SCAN_MAX_CANDIDATES} recurring themes — things appearing MULTIPLE times or suggesting an ongoing project/interest. One-off items are noise.
Return JSON ONLY: {"candidates":[{"summary":"...","entities":[...],"actions":[...],"contextScore":0.0-1.0,"timeSensitive":false}]}
If nothing recurs meaningfully, return {"candidates":[]}.`;

  const out = await heartbeat.askLLMJson(prompt, '');
  const candidates = Array.isArray(out?.candidates) ? out.candidates.slice(0, IDLE_SCAN_MAX_CANDIDATES) : [];
  for (const c of candidates) {
    if (!c?.summary) continue;
    await upsertCandidate('silence', {
      summary: c.summary,
      entityNames: Array.isArray(c.entities) ? c.entities.slice(0, 8).map(String) : [],
      actionNames: Array.isArray(c.actions) ? c.actions.slice(0, 6).map(String) : [],
      contextScore: Math.max(0, Math.min(1, Number(c.contextScore ?? 0.5))),
      timeSensitive: c.timeSensitive === true,
    });
  }
}

// ---------------------------------------------------------------------------
// Trigger evaluation + constrained action assignment
// ---------------------------------------------------------------------------

async function evaluateTriggers() {
  const t = now();
  if (t - state.lastTriggerAt < TRIGGER_COOLDOWN_MS) return;

  const res = await heartbeat.memPost('thought.list', { userId: USER_ID, limit: 50 });
  const thoughts = res?.data?.thoughts || res?.thoughts || [];
  const ready = thoughts
    .filter(th => th.status === 'thought' && th.score >= TRIGGER_SCORE)
    .filter(th => !(th.snoozedUntil && new Date(th.snoozedUntil).getTime() > t))
    .filter(th => !(SHADOW && t - (state.shadowFiredAt.get(th.id) || 0) < SHADOW_REARM_MS))
    .slice(0, MAX_TRIGGERS_PER_TICK);
  if (!ready.length) return;

  for (const thought of ready) {
    state.lastTriggerAt = t;
    if (SHADOW) {
      // Run action assignment (no gates, no execution) so the shadow log shows
      // WHAT would happen — not just that it would trigger.
      const action = await assignAction(thought);
      if (LIVE_ACTIONS.has(action.type)) {
        // Whitelisted action — executes for real through the normal gated path.
        console.log(`[ThoughtEngine:SHADOW] Live action ${action.type} whitelisted → executing for ${thought.id}`);
        await trigger(thought, action);
        continue;
      }
      state.shadowFiredAt.set(thought.id, t);
      const entry = { ts: t, thoughtId: thought.id, score: thought.score, summary: thought.summary, action };
      state.shadowLog.push(entry);
      if (state.shadowLog.length > 200) state.shadowLog.shift();
      console.log(`[ThoughtEngine:SHADOW] Would trigger ${thought.id} score=${thought.score.toFixed(2)} → ${action.type} (${action.reason || 'no reason'}) "${thought.summary}"`);
      // UI preview: Brain card shows what it would have done (in-memory, no DB).
      emitThoughtEvent('shadow', {
        ...thought,
        shadowAction: {
          type: action.type,
          text: action.payload?.text || action.payload?.prompt || action.payload?.memory || '',
          reason: action.reason || '',
        },
      });
      continue;
    }
    await trigger(thought);
  }
}

/** Constrained LLM action assignment — shared by trigger() and shadow eval. */
async function assignAction(thought) {
  const overlayText = (await getOverlay()) || '';
  const { mood_label } = await getMoodContext();
  const active = userActive();
  const trail = (thought.reinforcements || [])
    .map(tr => `${tr.input}@${new Date(tr.ts).toLocaleDateString('en-US', { weekday: 'short' })}`)
    .join(' · ');

  const prompt = `A proactive desktop assistant's internal "thought" just crossed its trigger threshold. Choose ONE action from the closed vocabulary.

THOUGHT: "${thought.summary}"
Entities: ${(thought.entityNames || []).join(', ') || 'none'}
Evidence trail: ${trail || 'none'}
Age: ${Math.round((now() - new Date(thought.createdAt).getTime()) / 3600000)}h; sources: ${(thought.reinforcements || []).length} traces
User presence: ${active ? 'ACTIVE at machine' : 'INACTIVE (away or idle)'}
Time now: ${new Date().toLocaleString()}
Current mood: ${mood_label || 'content'}
Persona context:
${overlayText.slice(0, 1200)}

ACTION VOCABULARY (choose exactly one):
- "notify": tell the user something useful/timely. payload: {"text": "what to say"}
- "question": ask the user a decision question. payload: {"text": "what to ask"}
- "prompt": autonomously do work (research, write a doc, compile a summary). payload: {"prompt": "the task instruction"}
- "skill": run a deterministic capability. payload: {"skill": "name", "args": {}}
- "remember": quietly save an insight to long-term memory (optional short text to also show the user). payload: {"memory": "what to store", "text": "optional user-facing note"}
- "watch": monitor a condition and notify when it resolves. payload: {"condition": "what to watch for", "checkType": "pixel_still|task_state|episodic_query|ocr_contains|time", "target": "app/window name", "marker": "OCR text to appear (ocr_contains)", "minutes": 30, "notifyPayload": "what to say when it resolves", "text": "optional spoken offer like 'I'll watch it'"}. Use pixel_still for generation/download finishing (screen stops changing); task_state for dispatched tasks; time for deadlines.
- "skip": triggered but no useful action right now — let it expire quietly. payload: {"reason": "why"}

Rules:
- Prefer "skip" if acting would interrupt without clear value — a quiet miss beats a noisy interruption.
- If the user is INACTIVE, still pick the action — delivery is handled separately.
- Match the persona's tone (see context above). If phrasing includes faith-sensitive content, keep it gentle and caring.
Return JSON ONLY: {"type":"...","payload":{...},"reason":"one sentence why","urgency":"low|medium|high","needsApproval":false}`;

  const choice = await heartbeat.askLLMJson(prompt, '');
  const action = normalizeAction(choice);
  if (!action) return { type: 'skip', payload: { reason: 'invalid LLM output' }, reason: 'action parse failed', urgency: 'low' };
  return action;
}

async function trigger(thought, preassignedAction = null) {
  console.log(`[ThoughtEngine] TRIGGER ${thought.id} score=${thought.score.toFixed(2)} "${thought.summary}"`);
  await heartbeat.memPost('thought.update', { id: thought.id, updates: { status: 'triggered' } });
  emitThoughtEvent('triggered', thought);

  const action = preassignedAction || await assignAction(thought);

  // ── Gates ──────────────────────────────────────────────────────────────
  // 1) Biblical gate (same check heartbeat uses for proactive outreach)
  if (action.type !== 'skip') {
    const candidate = `${action.type}: ${action.payload?.text || action.payload?.prompt || action.payload?.memory || ''}`;
    const passes = await heartbeat.biblicalGate(candidate);
    if (!passes) {
      await heartbeat.memPost('thought.update', {
        id: thought.id,
        updates: { status: 'expired', action, outcomeText: 'blocked by biblical gate' },
      });
      emitThoughtEvent('expired', { ...thought, status: 'expired' });
      return;
    }
  }

  // 2) User constraints
  const constraintMsg = `${action.type} ${action.payload?.text || action.payload?.prompt || ''}`;
  const cRes = await heartbeat.memPost('constraint.check', { message: constraintMsg });
  const cData = cRes?.data || cRes;
  if (cData && cData.allowed === false) {
    await heartbeat.memPost('thought.update', {
      id: thought.id,
      updates: { status: 'expired', action, outcomeText: 'blocked by user constraint' },
    });
    emitThoughtEvent('expired', { ...thought, status: 'expired' });
    return;
  }

  // ── Approval tier: side-effecting actions hold for user OK ────────────
  const needsApproval = !AUTO_ALLOWED.has(action.type) || action.needsApproval === true;
  if (needsApproval) {
    await heartbeat.memPost('thought.update', { id: thought.id, updates: { status: 'awaiting_approval', action } });
    emitThoughtEvent('awaiting_approval', { ...thought, status: 'awaiting_approval', action });
    return;
  }

  await executeAction(thought, action);
}

function normalizeAction(choice) {
  if (!choice || typeof choice !== 'object') return null;
  const type = String(choice.type || '').toLowerCase();
  if (!ACTION_TYPES.includes(type)) return null;
  return {
    type,
    payload: choice.payload && typeof choice.payload === 'object' ? choice.payload : {},
    reason: String(choice.reason || '').slice(0, 300),
    urgency: ['low', 'medium', 'high'].includes(choice.urgency) ? choice.urgency : 'low',
    needsApproval: choice.needsApproval === true,
  };
}

async function executeAction(thought, action) {
  let outcomeText = '';
  switch (action.type) {
    case 'skip': {
      outcomeText = action.payload?.reason || action.reason || 'no useful action';
      await heartbeat.memPost('thought.update', {
        id: thought.id, updates: { status: 'expired', action, outcomeText },
      });
      emitThoughtEvent('expired', { ...thought, status: 'expired' });
      return;
    }
    case 'notify':
    case 'question': {
      const text = action.payload?.text || thought.summary;
      const delivery = { thoughtId: thought.id, kind: action.type, text, urgency: action.urgency };
      if (action.type === 'question' && action.urgency !== 'high') {
        // Questions escalate the silence chain — remember the nudge phrasing
        state.recentNudges.push(text.slice(0, 120));
        if (state.recentNudges.length > 5) state.recentNudges.shift();
      }
      if (userActive()) {
        await deliver(delivery);
      } else {
        state.pendingDeliveries.push(delivery);
        console.log(`[ThoughtEngine] User inactive — holding delivery for ${thought.id}`);
      }
      outcomeText = text.slice(0, 200);
      break;
    }
    case 'remember': {
      await heartbeat.memPost('memory.store', {
        text: action.payload?.memory || thought.summary,
        memoryType: 'proactive_insight',
        metadata: { thoughtId: thought.id, source: 'thought-engine' },
      });
      outcomeText = 'saved to memory';
      if (action.payload?.text && userActive()) {
        await deliver({ thoughtId: thought.id, kind: 'notify', text: action.payload.text, urgency: 'low' });
        outcomeText += ` — told user: ${action.payload.text.slice(0, 120)}`;
      } else if (action.payload?.text) {
        state.pendingDeliveries.push({ thoughtId: thought.id, kind: 'notify', text: action.payload.text, urgency: 'low' });
      }
      break;
    }
    case 'prompt': {
      // Dispatch autonomous work through comms-graph. Outputs land in
      // ~/.thinkdrop/brain/<slug>/; task completion correlates back via the
      // queue producer to stamp artifacts + result onto this thought.
      const slug = (thought.summary || 'work').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '').slice(0, 40) || 'work';
      const outDir = path.join(os.homedir(), '.thinkdrop', 'brain', `${slug}-${thought.id.slice(-6)}`);
      const taskPrompt = `${action.payload?.prompt || thought.summary}\n\nSave any files you produce to: ${outDir}`;
      const r = await commsPost('/comms.proactive', { prompt: taskPrompt, thoughtId: thought.id });
      const taskId = r?.taskId || r?.data?.taskId;
      if (taskId) state.taskToThought.set(taskId, { thoughtId: thought.id, outDir });
      outcomeText = taskId ? `dispatched task ${taskId} → ${outDir}` : 'dispatched to comms-graph';
      break;
    }
    case 'watch': {
      await startWatch(thought, action);
      return; // thought is now 'watching' — evaluator owns the completion
    }
    case 'skill': {
      // Phase-3 executor — deterministic capability dispatch via external.skill
      outcomeText = `skill queued (executor pending): ${action.payload?.skill || ''}`.slice(0, 200);
      break;
    }
    default:
      outcomeText = 'unhandled action type';
  }

  await heartbeat.memPost('thought.update', {
    id: thought.id, updates: { status: 'completed', action, outcomeText },
  });
  emitThoughtEvent('completed', { ...thought, status: 'completed', action }, { outcomeText });
}

/**
 * A comms-graph task reached a terminal state — if a thought dispatched it,
 * stamp the result + any artifacts written to the thought's outDir.
 */
async function correlateTaskResult(taskId, status, result) {
  const link = state.taskToThought.get(taskId);
  if (!link) return;
  state.taskToThought.delete(taskId);
  let artifacts = [];
  try {
    artifacts = fs.readdirSync(link.outDir, { withFileTypes: true })
      .filter(d => d.isFile())
      .map(d => ({ name: d.name, path: path.join(link.outDir, d.name) }));
  } catch (_) { /* outDir may not exist if the task wrote nothing */ }
  const outcome = `task ${status}${result ? `: ${String(result).slice(0, 160)}` : ''}` +
    (artifacts.length ? ` — ${artifacts.length} file(s) in ${link.outDir}` : '');
  await heartbeat.memPost('thought.update', {
    id: link.thoughtId,
    updates: { outcomeText: outcome, action: { type: 'prompt', payload: { outDir: link.outDir, artifacts } } },
  });
  emitThoughtEvent('completed', { id: link.thoughtId, outcomeText: outcome });
}

// ---------------------------------------------------------------------------
// Watch evaluator — 'watch' actions monitor a condition each tick until it
// resolves (→ notify + completed) or expiresAt passes (→ expired).
// ---------------------------------------------------------------------------

function normalizeWatch(payload, thoughtSummary) {
  const p = payload || {};
  const CHECK_TYPES = ['pixel_still', 'task_state', 'episodic_query', 'ocr_contains', 'time'];
  return {
    checkType: CHECK_TYPES.includes(p.checkType) ? p.checkType : 'episodic_query',
    condition: String(p.condition || thoughtSummary || '').slice(0, 300),
    target: String(p.target || '').slice(0, 200),   // app/window name or task hint
    marker: p.marker ? String(p.marker).slice(0, 200) : null,
    taskId: p.taskId || null,
    deadline: p.deadline ? Number(p.deadline) : (p.minutes ? now() + Number(p.minutes) * 60000 : null), // epoch ms ('time' checkType)
    stillMs: Number(p.stillMs) || WATCH_STILL_MS,
    notifyPayload: String(p.notifyPayload || p.text || `Done watching: ${p.condition || thoughtSummary || 'condition'}`).slice(0, 300),
    startedAt: now(),
    expiresAt: p.expiresAt ? Number(p.expiresAt) : now() + WATCH_TTL_MS,
  };
}

/** Returns notify text if the watch condition resolved, else null. */
async function checkWatch(w) {
  switch (w.checkType) {
    case 'time':
      return w.deadline && now() >= w.deadline ? w.notifyPayload : null;

    case 'task_state': {
      const r = await commsGet('/tasks');
      const tasks = r?.tasks || [];
      const task = tasks.find(t =>
        (w.taskId && t.id === w.taskId) ||
        (w.target && (t.prompt || '').toLowerCase().includes(w.target.toLowerCase())));
      if (task && ['done', 'failed', 'cancelled'].includes(task.status)) {
        return `${w.notifyPayload} (task ${task.status}${task.result ? `: ${String(task.result).slice(0, 120)}` : ''})`;
      }
      return null;
    }

    case 'pixel_still': {
      // The monitor only writes captures when pixels change — zero captures in
      // the still window means the target stopped updating (generation done,
      // progress bar finished). An API error is inconclusive, never "still".
      const r = await episodicRecent({ limit: 50 });
      if (!r.ok) return null;
      const ms = capturesSince(r.memories, now() - w.stillMs, w.target || null);
      return ms.length === 0 ? w.notifyPayload : null;
    }

    case 'ocr_contains':
    case 'episodic_query': {
      const r = await episodicRecent({ limit: 100 });
      if (!r.ok) return null;
      const ms = capturesSince(r.memories, w.startedAt, w.target || null);
      const blob = ms
        .map(m => (m.metadata && (m.metadata.ocrText || m.metadata.ocr_text)) || m.text || '')
        .join('\n')
        .toLowerCase();
      const needle = (w.marker || w.condition || '').toLowerCase();
      return needle && blob.includes(needle) ? w.notifyPayload : null;
    }

    default:
      return null;
  }
}

async function evaluateWatches() {
  for (const [thoughtId, w] of [...state.watches]) {
    try {
      if (w.expiresAt && now() > w.expiresAt) {
        await resolveWatch(thoughtId, null, 'watch expired');
        continue;
      }
      const hit = await checkWatch(w);
      if (hit) await resolveWatch(thoughtId, hit, 'watch condition met');
    } catch (e) {
      console.warn(`[ThoughtEngine] watch ${thoughtId} check failed:`, e.message);
    }
  }
}

async function resolveWatch(thoughtId, notifyText, outcomeText) {
  state.watches.delete(thoughtId);
  if (notifyText) {
    const d = { thoughtId, kind: 'notify', text: notifyText, urgency: 'medium' };
    if (userActive()) await deliver(d); else state.pendingDeliveries.push(d);
    await heartbeat.memPost('thought.update', {
      id: thoughtId, updates: { status: 'completed', outcomeText: notifyText },
    });
    emitThoughtEvent('completed', { id: thoughtId, outcomeText: notifyText });
    console.log(`[ThoughtEngine] Watch resolved → notified: ${thoughtId}`);
  } else {
    await heartbeat.memPost('thought.update', {
      id: thoughtId, updates: { status: 'expired', outcomeText },
    });
    emitThoughtEvent('expired', { id: thoughtId });
  }
}

/** Register a watch on a thought row (post-gate). Thought stays 'watching'. */
async function startWatch(thought, action) {
  if (state.watches.size >= WATCH_MAX) {
    const d = { thoughtId: thought.id, kind: 'notify', urgency: 'low',
      text: `I can't watch that right now — already tracking ${WATCH_MAX} things.` };
    if (userActive()) await deliver(d); else state.pendingDeliveries.push(d);
    await heartbeat.memPost('thought.update', {
      id: thought.id, updates: { status: 'completed', action, outcomeText: 'watch cap reached — notified instead' },
    });
    return;
  }
  const spec = normalizeWatch(action.payload, thought.summary);
  state.watches.set(thought.id, spec);
  const updatedAction = { ...action, payload: { ...action.payload, ...spec } };
  await heartbeat.memPost('thought.update', {
    id: thought.id, updates: { status: 'watching', action: updatedAction },
  });
  emitThoughtEvent('watching', { ...thought, status: 'watching', action: updatedAction });
  const offer = action.payload?.text;
  if (offer) {
    const d = { thoughtId: thought.id, kind: 'notify', text: offer, urgency: 'low' };
    if (userActive()) await deliver(d); else state.pendingDeliveries.push(d);
  }
  console.log(`[ThoughtEngine] Watch started: ${thought.id} (${spec.checkType} — "${spec.condition}")`);
}

/**
 * Explicit watch request in a user prompt ("watch this LLM response and let me
 * know when it's done") — direct intent bypasses thought accumulation entirely.
 * Returns the watching thought id, or null if parsing failed.
 */
async function createExplicitWatch(text) {
  if (state.watches.size >= WATCH_MAX) return null;
  const spec = await heartbeat.askLLMJson(`Parse this watch request into a monitor spec.
REQUEST: "${text.slice(0, 800)}"
Return JSON ONLY:
{"condition":"what to watch for (short phrase)","checkType":"time|pixel_still|task_state|episodic_query|ocr_contains","target":"app/window name if identifiable","marker":"text to look for in screen OCR (ocr_contains only)","minutes":<number for 'time' checks>,"notifyText":"what to tell the user when it resolves"}
checkType guide: 'time' for "in N minutes/at TIME"; 'pixel_still' for "when generation/streaming/download finishes" (screen stops changing); 'task_state' for a dispatched task; 'ocr_contains' when specific text appearing/disappearing signals done; 'episodic_query' otherwise.`, '');
  if (!spec || !spec.condition) {
    console.warn('[ThoughtEngine] explicit-watch parse failed:', spec ? JSON.stringify(spec).slice(0, 200) : 'null response');
    return null;
  }

  // Persist as a 'watching' thought so it's visible in the Brain tab and
  // survives engine restarts.
  const upsert = await heartbeat.memPost('thought.upsert', {
    input: 'prompt',
    summary: `Watching: ${spec.condition}`,
    entityNames: spec.target ? [spec.target] : [],
    actionNames: ['watching'],
    sourceIds: [],
    userId: USER_ID,
    traceWeight: 1.0,
    forceNew: true,
  });
  const thought = (upsert?.data || upsert)?.thought;
  if (!thought?.id) return null;

  const payload = {
    ...spec,
    checkType: spec.checkType,
    deadline: spec.minutes ? now() + Number(spec.minutes) * 60000 : null,
    notifyPayload: spec.notifyText || spec.condition,
    text: `I'll watch ${spec.condition} and let you know.`,
  };
  await startWatch(thought, { type: 'watch', payload, reason: 'explicit user request', urgency: 'low' });
  return thought.id;
}

/** Deliver a notify/question — voice for higher urgency, card always. */
async function deliver(d) {
  // Card + optional voice. Card goes to the Brain tab via thought:update;
  // voice only for medium/high urgency (JITIR: default delivery is ignorable).
  emitThoughtEvent(d.kind === 'question' ? 'question' : 'notify',
    { id: d.thoughtId, action: { type: d.kind }, summary: d.text });
  if (d.urgency !== 'low' && !inQuietHours()) {
    await heartbeat.voiceSpeak(d.text);
  }
}

/** Deliver-on-wake: flush held payloads when the user returns. */
async function flushPending() {
  if (!state.pendingDeliveries.length || !userActive()) return;
  const held = state.pendingDeliveries.splice(0, state.pendingDeliveries.length);
  console.log(`[ThoughtEngine] Delivering ${held.length} held notification(s)`);
  for (const d of held) await deliver(d);
}

/** Approve/dismiss/snooze a thought (Brain tab buttons). */
async function decide(thoughtId, decision, options = {}) {
  // Snooze applies to live thoughts, not just approval cards — look wider.
  const statuses = decision === 'snooze'
    ? ['thought', 'triggered', 'awaiting_approval']
    : ['awaiting_approval'];
  const res = await heartbeat.memPost('thought.list', { userId: USER_ID, statuses, limit: 100 });
  const thoughts = res?.data?.thoughts || res?.thoughts || [];
  const thought = thoughts.find(t => t.id === thoughtId);
  if (!thought) return { ok: false, reason: 'not_found' };

  if (decision === 'snooze') {
    const mins = Number(options.minutes) || 60;
    const until = new Date(now() + mins * 60000).toISOString();
    await heartbeat.memPost('thought.update', {
      id: thoughtId, updates: { snoozedUntil: until },
    });
    emitThoughtEvent('snoozed', { ...thought, snoozedUntil: until });
    return { ok: true, snoozedUntil: until };
  }
  if (decision === 'approve') {
    await executeAction(thought, thought.action || { type: 'skip', payload: {}, reason: '', urgency: 'low' });
    return { ok: true, executed: true };
  }
  await heartbeat.memPost('thought.update', {
    id: thoughtId, updates: { status: 'expired', outcomeText: 'dismissed by user' },
  });
  emitThoughtEvent('expired', { ...thought, status: 'expired' });
  return { ok: true, executed: false };
}

// ---------------------------------------------------------------------------
// Tick loop
// ---------------------------------------------------------------------------

async function tick() {
  if (!ENABLED) return;
  try {
    await silenceTick();
  } catch (e) {
    console.warn('[ThoughtEngine] silenceTick failed:', e.message);
  }
  try {
    await evaluateTriggers();
  } catch (e) {
    console.warn('[ThoughtEngine] evaluateTriggers failed:', e.message);
  }
  try {
    await evaluateWatches();
  } catch (e) {
    console.warn('[ThoughtEngine] evaluateWatches failed:', e.message);
  }
  try {
    await heartbeat.memPost('thought.purge', { userId: USER_ID });
  } catch (_) {}
}

function start() {
  if (!ENABLED) {
    console.log('[ThoughtEngine] Disabled via THOUGHT_ENGINE_ENABLED=false');
    return;
  }
  if (state.started) return;
  state.started = true;
  console.log(`[ThoughtEngine] Starting (tick ${TICK_MS}ms, τ=${TRIGGER_SCORE}, shadow=${SHADOW})`);
  state.tickTimer = setInterval(() => tick().catch(() => {}), TICK_MS);

  // Hydrate watches that survived a restart (persisted as 'watching' rows).
  heartbeat.memPost('thought.list', { userId: USER_ID, statuses: ['watching'], limit: WATCH_MAX })
    .then(res => {
      const rows = res?.data?.thoughts || res?.thoughts || [];
      for (const t of rows) {
        if (t.action?.payload) state.watches.set(t.id, normalizeWatch(t.action.payload, t.summary));
      }
      if (rows.length) console.log(`[ThoughtEngine] Hydrated ${state.watches.size} watch(es)`);
    })
    .catch(() => {});
}

function stop() {
  state.started = false;
  if (state.tickTimer) clearInterval(state.tickTimer);
  state.tickTimer = null;
}

module.exports = {
  start, stop, tick,
  handleInput,
  decide,
  noteMonitorEvent: () => { state.lastMonitorEventAt = now(); flushPending().catch(() => {}); },
  _state: state, // for diagnostics/tests
};
