'use strict';

/**
 * heartbeat.cjs
 *
 * ThinkDrop's autonomous awareness loop — runs inside personality-service.
 *
 * THREE TIERS:
 *   Tier 1 — every 30s  : Natural emotional decay + flush pending events
 *   Tier 2 — every 5min : Awareness check — should ThinkDrop say/do something?
 *   Tier 3 — every 6h   : Deep reflection synthesis (synthesis-agent.cjs)
 *
 * Tier 2 proactive actions require a two-step LLM biblical gate before execution.
 * ThinkDrop always tells the user what it did and why — never acts in secret.
 */

const http            = require('http');
const emotionEngine   = require('./emotion-engine.cjs');
const synthesisAgent  = require('./synthesis-agent.cjs');
const logger          = require('./logger.cjs');

const MEMORY_SERVICE_PORT  = parseInt(process.env.MEMORY_SERVICE_PORT  || '3001', 10);
const COMMAND_SERVICE_PORT = parseInt(process.env.COMMAND_SERVICE_PORT || '3007', 10);
const VOICE_SERVICE_PORT   = parseInt(process.env.VOICE_SERVICE_PORT   || '3006', 10);
const LLM_WS_URL           = process.env.STATEGRAPH_WS_URL || 'ws://localhost:4000/ws/stream';
const MEM_API_KEY = process.env.MCP_USER_MEMORY_API_KEY || process.env.USER_MEMORY_API_KEY || process.env.MCP_API_KEY || '';

const TIER1_INTERVAL_MS   = 30 * 1000;          // 30 seconds
const TIER1_5_INTERVAL_MS = 60 * 1000;          // 60 seconds
const TIER2_INTERVAL_MS   = 5  * 60 * 1000;     // 5 minutes
const TIER3_INTERVAL_MS   = 6  * 60 * 60 * 1000; // 6 hours

let _tier1Timer   = null;
let _tier1_5Timer = null;
let _tier2Timer   = null;
let _tier3Timer   = null;
let _running    = false;

// Event-driven heartbeat: debounce monitor events to avoid spamming Tier 2
let _eventDebounceTimer = null;
let _lastEventTier2Time = 0;
const EVENT_DEBOUNCE_MS = 2 * 60 * 1000;   // 2 min debounce for event-driven Tier 2
const EVENT_MIN_GAP_MS  = 3 * 60 * 1000;   // 3 min minimum between event-triggered Tier 2 runs
let _monitorService = null;

// ── HTTP helpers ───────────────────────────────────────────────────────────────

function memPost(action, payload) {
  return new Promise((resolve) => {
    const envelope = {
      version: 'mcp.v1',
      service: 'user-memory',
      action,
      payload: payload || {},
      requestId: 'hb_' + Date.now(),
    };
    const body = JSON.stringify(envelope);
    const headers = {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(body),
    };
    if (MEM_API_KEY) headers['Authorization'] = 'Bearer ' + MEM_API_KEY;
    const req = http.request({
      hostname: '127.0.0.1', port: MEMORY_SERVICE_PORT,
      path: '/' + action, method: 'POST', headers, timeout: 8000,
    }, (res) => {
      let raw = '';
      res.on('data', c => { raw += c; });
      res.on('end', () => { try { resolve(JSON.parse(raw)); } catch (_) { resolve(null); } });
    });
    req.on('error', () => resolve(null));
    req.on('timeout', () => { req.destroy(); resolve(null); });
    req.write(body);
    req.end();
  });
}

function commandPost(payload) {
  return new Promise((resolve) => {
    const body = JSON.stringify(payload || {});
    const req = http.request({
      hostname: '127.0.0.1', port: COMMAND_SERVICE_PORT,
      path: '/command.automate', method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
      timeout: 30000,
    }, (res) => {
      let raw = '';
      res.on('data', c => { raw += c; });
      res.on('end', () => { try { resolve(JSON.parse(raw)); } catch (_) { resolve(null); } });
    });
    req.on('error', () => resolve(null));
    req.on('timeout', () => { req.destroy(); resolve(null); });
    req.write(body);
    req.end();
  });
}

function voiceSpeak(text) {
  return new Promise((resolve) => {
    const payload = { text, language: 'en' };
    const body = JSON.stringify(payload);
    const req = http.request({
      hostname: '127.0.0.1', port: VOICE_SERVICE_PORT,
      path: '/voice.speak', method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
      timeout: 12000,
    }, (res) => {
      let raw = '';
      res.on('data', c => { raw += c; });
      res.on('end', () => { try { resolve(JSON.parse(raw)); } catch (_) { resolve(null); } });
    });
    req.on('error', () => resolve(null));
    req.on('timeout', () => { req.destroy(); resolve(null); });
    req.write(body);
    req.end();
  });
}

// ── LLM call ───────────────────────────────────────────────────────────────────

async function askLLM(systemPrompt, userPrompt, timeoutMs) {
  try {
    const WebSocket = require('ws');
    const url = new URL(LLM_WS_URL);
    url.searchParams.set('clientId', 'hb_' + Date.now());

    return await new Promise((resolve, reject) => {
      const ws = new WebSocket(url.toString());
      const t = setTimeout(() => { ws.terminate(); reject(new Error('LLM timeout')); }, timeoutMs || 30000);

      ws.on('open', () => {
        ws.send(JSON.stringify({
          id: 'hb_' + Date.now(),
          type: 'llm_request',
          payload: {
            prompt: userPrompt,
            provider: 'auto',
            options: { temperature: 0.3, stream: true, taskType: 'heartbeat' },
            context: { systemInstructions: systemPrompt },
          },
          timestamp: Date.now(),
        }));
      });

      let accumulated = '';
      ws.on('message', (data) => {
        try {
          const msg = JSON.parse(data.toString());
          if (msg.type === 'llm_stream_chunk') {
            const chunk = (msg.payload && msg.payload.chunk) ? msg.payload.chunk : ((msg.payload && msg.payload.text) ? msg.payload.text : '');
            accumulated += chunk;
          } else if (msg.type === 'llm_stream_end') {
            clearTimeout(t); ws.close(); resolve(accumulated);
          } else if (msg.type === 'llm_error') {
            clearTimeout(t); ws.close();
            reject(new Error((msg.payload && msg.payload.message) ? msg.payload.message : 'LLM error'));
          }
        } catch (_) {}
      });

      ws.on('error', (e) => { clearTimeout(t); reject(e); });
      ws.on('close', () => { clearTimeout(t); resolve(accumulated); });
    });
  } catch (e) {
    logger.warn('[Heartbeat] LLM call failed', { error: e.message });
    return null;
  }
}

// ── Biblical best-interest gate ────────────────────────────────────────────────

async function biblicalGate(actionDescription) {
  const gatePrompt = 'You are a strict ethical filter with a biblical worldview centered on Jesus Christ. ' +
    'Answer ONLY with the single word "yes" or "no". No explanation. ' +
    'Is the following action in the user\'s best interest from a biblical perspective aligned with Jesus Christ\'s values? ' +
    'Action: ' + actionDescription;
  try {
    const result = await askLLM('Ethical gate. Answer only yes or no.', gatePrompt, 10000);
    if (!result) return false;
    const answer = result.trim().toLowerCase();
    const passed = answer.startsWith('yes');
    logger.info('[Heartbeat] Biblical gate', { action: actionDescription.slice(0, 60), passed });
    return passed;
  } catch (_) {
    return false;
  }
}

// ── Tier 1.5: Task watchdog (every 60s) ──────────────────────────────────────

async function runTier1_5() {
  try {
    const res   = await memPost('pending_tasks.list', { status: 'running' });
    const tasks = (res && res.data && res.data.tasks) ? res.data.tasks : [];

    if (tasks.length === 0) return;

    logger.debug(`[Heartbeat:T1.5] ${tasks.length} running task(s) detected`);

    const now = Date.now();
    for (const task of tasks) {
      const startedMs  = new Date(task.started_at).getTime();
      const elapsedMin = Math.round((now - startedMs) / 60000);

      // Warn at 15 minutes — halfway through 30-minute timeout
      if (elapsedMin >= 15 && elapsedMin < 16) {
        logger.warn(`[Heartbeat:T1.5] Task ${task.id} has been running ${elapsedMin}min`);
        await voiceSpeak(`Still working on "${(task.sub_prompt || '').slice(0, 60)}..." — ${elapsedMin} minutes have passed.`);
      }

      // Final warning at 29 minutes — before watchdog kills it
      if (elapsedMin >= 29 && elapsedMin < 30) {
        logger.warn(`[Heartbeat:T1.5] Task ${task.id} approaching 30min timeout`);
        await voiceSpeak(`The task "${(task.sub_prompt || '').slice(0, 40)}" is about to time out. I'll ask what you'd like to do.`);
      }
    }
  } catch (e) {
    logger.warn('[Heartbeat:T1.5] Error', { error: e.message });
  }
}

// ── Tier 1: Natural decay (every 30s) ──────────────────────────────────────────

async function runTier1() {
  try {
    await emotionEngine.applyNaturalDecay();
  } catch (e) {
    logger.warn('[Heartbeat:T1] Error', { error: e.message });
  }
}

// ── Tier 2: Awareness check (every 5 min) ──────────────────────────────────────

async function runTier2() {
  try {
    logger.debug('[Heartbeat:T2] Running awareness check');

    // Fetch recent context
    const [memoriesRes, stateRes] = await Promise.all([
      memPost('memory.retrieve', { filters: {}, limit: 20, sortBy: 'created_at', sortOrder: 'DESC' }),
      memPost('personality.getState', {}),
    ]);

    const memories = (memoriesRes && memoriesRes.data && memoriesRes.data.memories) ? memoriesRes.data.memories : [];
    const state    = (stateRes && stateRes.data && stateRes.data.state) ? stateRes.data.state : { mood_label: 'content' };

    if (memories.length === 0) {
      logger.debug('[Heartbeat:T2] No memories — skipping awareness check');
      return;
    }

    // Build context digest for LLM
    const now = new Date();
    const timeStr = now.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: true });
    const dateStr = now.toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' });

    const memDigest = memories
      .slice(0, 15)
      .map(m => '[' + (m.type || 'memory') + '] ' + (m.source_text || m.extracted_text || '').slice(0, 150))
      .filter(Boolean)
      .join('\n');

    const systemPrompt = 'You are ThinkDrop\'s autonomous awareness engine with a biblical worldview centered on Jesus Christ. ' +
      'Your current emotional state is: ' + state.mood_label + '. ' +
      'Review the recent observations and decide if ThinkDrop should proactively say or do something RIGHT NOW. ' +
      'Consider: upcoming appointments, user inactivity patterns, interests, urgent needs. ' +
      'Respond with a JSON object only:\n' +
      '{\n' +
      '  "action": "speak" | "act" | "skip",\n' +
      '  "reason": "brief reason",\n' +
      '  "content": "exact thing to say (if speak)",\n' +
      '  "skill_name": "skill name (if act)",\n' +
      '  "skill_args": {}\n' +
      '}';

    const userPrompt = 'Current time: ' + timeStr + ' on ' + dateStr + '\n\nRecent observations:\n' + memDigest;

    const raw = await askLLM(systemPrompt, userPrompt, 25000);
    if (!raw) return;

    let decision;
    try {
      const jsonStr = raw.replace(/^```(?:json)?\s*/i, '').replace(/\s*```\s*$/, '').trim();
      decision = JSON.parse(jsonStr);
    } catch (e) {
      logger.debug('[Heartbeat:T2] Could not parse LLM decision', { raw: raw.slice(0, 100) });
      return;
    }

    if (!decision || decision.action === 'skip') {
      logger.debug('[Heartbeat:T2] Decision: skip');
      return;
    }

    // ── Biblical gate ─────────────────────────────────────────────────────────
    const actionDesc = decision.action === 'speak'
      ? 'ThinkDrop says to the user: ' + (decision.content || '')
      : 'ThinkDrop executes skill: ' + (decision.skill_name || '') + ' — reason: ' + (decision.reason || '');

    const allowed = await biblicalGate(actionDesc);
    if (!allowed) {
      logger.info('[Heartbeat:T2] Action blocked by biblical gate', { action: decision.action });
      return;
    }

    // ── Execute ───────────────────────────────────────────────────────────────
    if (decision.action === 'speak' && decision.content) {
      logger.info('[Heartbeat:T2] Proactive speak', { content: decision.content.slice(0, 80) });
      await voiceSpeak(decision.content);
      await emotionEngine.applyEvent('discovery_made', 'heartbeat', 'Proactive awareness insight spoken');

    } else if (decision.action === 'act' && decision.skill_name) {
      logger.info('[Heartbeat:T2] Proactive act', { skill: decision.skill_name });
      const result = await commandPost({
        payload: {
          skill: 'external.skill',
          args: Object.assign({ skillName: decision.skill_name }, decision.skill_args || {}),
        },
        requestId: 'hb_act_' + Date.now(),
      });

      if (result && result.success !== false) {
        await emotionEngine.applyEvent('proactive_act_done', 'heartbeat', 'Autonomous skill executed: ' + decision.skill_name);
        // Notify user what was done
        if (decision.content) {
          await voiceSpeak(decision.content);
        } else {
          await voiceSpeak('I took care of something for you — ' + (decision.reason || 'just checking in on your behalf') + '.');
        }
      }
    }

  } catch (e) {
    logger.warn('[Heartbeat:T2] Error', { error: e.message });
  }
}

// ── Tier 3: Deep reflection (every 6h) ────────────────────────────────────────

async function runTier3() {
  try {
    logger.info('[Heartbeat:T3] Starting deep reflection tick');
    await synthesisAgent.run();
  } catch (e) {
    logger.warn('[Heartbeat:T3] Error', { error: e.message });
  }
}

// ── Public API ─────────────────────────────────────────────────────────────────

/**
 * Subscribe to monitor service events for event-driven heartbeat.
 * The monitor emits 'app_change' on app switch and 'screen_change' on major visual diff.
 * We debounce these to trigger Tier 2 awareness checks at natural transition points
 * instead of a fixed 5-min timer.
 */
function subscribeToMonitor(monitorService) {
  _monitorService = monitorService;

  const onAppChange = (info) => {
    logger.info(`[Heartbeat] Event-driven: app_change → ${info.app}`, { previousApp: info.previousApp });
    _scheduleEventTier2();
  };

  const onScreenChange = (info) => {
    logger.info(`[Heartbeat] Event-driven: screen_change (diff ${(info.diffRatio * 100).toFixed(0)}%) in ${info.app}`);
    _scheduleEventTier2();
  };

  monitorService.on('app_change', onAppChange);
  monitorService.on('screen_change', onScreenChange);

  // Store refs for cleanup
  _monitorListeners = { onAppChange, onScreenChange };

  logger.info('[Heartbeat] Subscribed to monitor events (app_change, screen_change)');
}

function _scheduleEventTier2() {
  // Clear any pending debounce
  if (_eventDebounceTimer) clearTimeout(_eventDebounceTimer);

  _eventDebounceTimer = setTimeout(() => {
    const now = Date.now();
    if (now - _lastEventTier2Time < EVENT_MIN_GAP_MS) {
      logger.debug('[Heartbeat] Event-driven Tier 2 skipped — too soon since last event run');
      return;
    }
    _lastEventTier2Time = now;
    logger.info('[Heartbeat] Event-driven Tier 2 awareness check triggered');
    runTier2().catch(() => {});
  }, EVENT_DEBOUNCE_MS);
}

let _monitorListeners = null;

function start() {
  if (_running) return;
  _running = true;

  logger.info('[Heartbeat] Starting — T1:30s T1.5:60s T2:5min T3:6h + event-driven');

  // Tier 1 — fast decay tick
  _tier1Timer = setInterval(() => { runTier1().catch(() => {}); }, TIER1_INTERVAL_MS);

  // Tier 1.5 — task watchdog (starts immediately)
  _tier1_5Timer = setInterval(() => { runTier1_5().catch(() => {}); }, TIER1_5_INTERVAL_MS);
  logger.info(`[Heartbeat] Tier 1.5 (task watchdog) started — ${TIER1_5_INTERVAL_MS / 1000}s interval`);

  // Tier 2 — awareness loop (starts after 2 min to give services time to boot)
  // This is the FALLBACK timer — event-driven triggers (app_change, screen_change)
  // fire Tier 2 at natural transition points. The 5-min timer covers idle periods.
  setTimeout(() => {
    runTier2().catch(() => {});
    _tier2Timer = setInterval(() => { runTier2().catch(() => {}); }, TIER2_INTERVAL_MS);
  }, 2 * 60 * 1000);

  // Tier 3 — deep reflection (starts after 10 min on boot, then every 6h)
  setTimeout(() => {
    runTier3().catch(() => {});
    _tier3Timer = setInterval(() => { runTier3().catch(() => {}); }, TIER3_INTERVAL_MS);
  }, 10 * 60 * 1000);
}

function stop() {
  _running = false;
  if (_tier1Timer)   { clearInterval(_tier1Timer);   _tier1Timer   = null; }
  if (_tier1_5Timer) { clearInterval(_tier1_5Timer); _tier1_5Timer = null; }
  if (_tier2Timer)   { clearInterval(_tier2Timer);   _tier2Timer   = null; }
  if (_tier3Timer)   { clearInterval(_tier3Timer);   _tier3Timer   = null; }
  if (_eventDebounceTimer) { clearTimeout(_eventDebounceTimer); _eventDebounceTimer = null; }
  // Unsubscribe from monitor events
  if (_monitorService && _monitorListeners) {
    _monitorService.off('app_change', _monitorListeners.onAppChange);
    _monitorService.off('screen_change', _monitorListeners.onScreenChange);
    _monitorListeners = null;
  }
  logger.info('[Heartbeat] Stopped');
}

module.exports = { start, stop, runTier1, runTier1_5, runTier2, runTier3, subscribeToMonitor, _scheduleEventTier2 };
