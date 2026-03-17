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

const TIER1_INTERVAL_MS = 30 * 1000;          // 30 seconds
const TIER2_INTERVAL_MS = 5  * 60 * 1000;     // 5 minutes
const TIER3_INTERVAL_MS = 6  * 60 * 60 * 1000; // 6 hours

let _tier1Timer = null;
let _tier2Timer = null;
let _tier3Timer = null;
let _running    = false;

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
            provider: 'openai',
            options: { temperature: 0.3, stream: true, taskType: 'ask' },
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

function start() {
  if (_running) return;
  _running = true;

  logger.info('[Heartbeat] Starting — T1:30s T2:5min T3:6h');

  // Tier 1 — fast decay tick
  _tier1Timer = setInterval(() => { runTier1().catch(() => {}); }, TIER1_INTERVAL_MS);

  // Tier 2 — awareness loop (starts after 2 min to give services time to boot)
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
  if (_tier1Timer) { clearInterval(_tier1Timer); _tier1Timer = null; }
  if (_tier2Timer) { clearInterval(_tier2Timer); _tier2Timer = null; }
  if (_tier3Timer) { clearInterval(_tier3Timer); _tier3Timer = null; }
  logger.info('[Heartbeat] Stopped');
}

module.exports = { start, stop, runTier1, runTier2, runTier3 };
