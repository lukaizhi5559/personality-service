'use strict';

/**
 * personality-service/src/server.cjs
 *
 * ThinkDrop Personality & Emotion MCP Service — port 3008
 *
 * HTTP endpoints:
 *   POST /personality.getState    — current VAD + mood_label + behavioral_guidance
 *   POST /personality.event       — apply a named mood event
 *   POST /personality.resetState  — manual reset to content/neutral
 *   POST /personality.getTraits   — all personality traits
 *   POST /personality.upsertTrait — write/update a trait
 *   POST /personality.getOverlay  — full THINKDROP LIVE STATE block for LLM injection
 *   GET  /health                  — service health check
 *
 * All state persists in the user-memory MCP DuckDB (port 3001).
 * This service owns the emotion engine, heartbeat, and synthesis agent.
 * It is a pure consumer of user-memory — no local DB.
 */

require('dotenv').config();

const http       = require('http');
const emotionEngine  = require('./emotion-engine.cjs');
const personalityDoc = require('./personality-doc.cjs');
const heartbeat      = require('./heartbeat.cjs');
const logger         = require('./logger.cjs');

const PORT         = parseInt(process.env.PORT || '3008', 10);
const MEMORY_PORT  = parseInt(process.env.MEMORY_SERVICE_PORT || '3001', 10);
const MEM_API_KEY  = process.env.MCP_USER_MEMORY_API_KEY || process.env.USER_MEMORY_API_KEY || process.env.MCP_API_KEY || '';

// ── Simple JSON response helper ────────────────────────────────────────────────

function jsonResponse(res, statusCode, data) {
  const body = JSON.stringify(data);
  res.writeHead(statusCode, {
    'Content-Type': 'application/json',
    'Content-Length': Buffer.byteLength(body),
  });
  res.end(body);
}

// ── Forward personality.* requests to user-memory MCP ─────────────────────────
// personality-service is the *orchestrator* — actual DuckDB state lives in user-memory.
// These forwarded calls hit the personality routes registered in user-memory server.js.

function forwardToMemory(action, payload) {
  return new Promise((resolve) => {
    const envelope = {
      version: 'mcp.v1',
      service: 'user-memory',
      action,
      payload: payload || {},
      requestId: 'ps_fwd_' + Date.now(),
    };
    const body = JSON.stringify(envelope);
    const headers = {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(body),
    };
    if (MEM_API_KEY) headers['Authorization'] = 'Bearer ' + MEM_API_KEY;

    const req = http.request({
      hostname: '127.0.0.1',
      port: MEMORY_PORT,
      path: '/' + action,
      method: 'POST',
      headers,
      timeout: 8000,
    }, (res) => {
      let raw = '';
      res.on('data', c => { raw += c; });
      res.on('end', () => { try { resolve(JSON.parse(raw)); } catch (_) { resolve(null); } });
    });
    req.on('error', (e) => { logger.warn('[Server] Forward error', { error: e.message }); resolve(null); });
    req.on('timeout', () => { req.destroy(); resolve(null); });
    req.write(body);
    req.end();
  });
}

// ── Request router ─────────────────────────────────────────────────────────────

async function handleRequest(req, res) {
  const { method, url } = req;

  // Health check
  if (method === 'GET' && (url === '/health' || url === '/service.health')) {
    return jsonResponse(res, 200, {
      service: 'personality-service',
      version: '1.0.0',
      status: 'up',
      port: PORT,
      uptime: process.uptime(),
      timestamp: new Date().toISOString(),
    });
  }

  // All personality routes require POST
  if (method !== 'POST') {
    return jsonResponse(res, 405, { error: 'Method not allowed' });
  }

  // Parse request body
  let body = '';
  req.on('data', chunk => { body += chunk; });
  await new Promise(resolve => req.on('end', resolve));

  let parsed;
  try {
    parsed = body ? JSON.parse(body) : {};
  } catch (_) {
    return jsonResponse(res, 400, { error: 'Invalid JSON' });
  }

  const { payload = {}, requestId } = parsed;
  const action = url.replace(/^\//, '');

  // ── Routes forwarded to user-memory MCP ─────────────────────────────────────

  const forwardedActions = [
    'personality.getState',
    'personality.event',
    'personality.resetState',
    'personality.getTraits',
    'personality.upsertTrait',
    'personality.getOverlay',
  ];

  if (forwardedActions.includes(action)) {
    const result = await forwardToMemory(action, payload);
    if (!result) {
      return jsonResponse(res, 503, { error: 'user-memory MCP unavailable', action });
    }
    // Invalidate overlay cache on state-changing events
    if (action === 'personality.event' || action === 'personality.resetState' || action === 'personality.upsertTrait') {
      personalityDoc.invalidateCache();
    }
    return jsonResponse(res, 200, result);
  }

  // ── /personality.overlay shortcut (returns overlay string directly) ──────────
  if (action === 'personality.overlay') {
    const overlay = await personalityDoc.getOverlay();
    return jsonResponse(res, 200, {
      version: 'mcp.v1',
      service: 'personality-service',
      action: 'personality.overlay',
      requestId,
      status: 'ok',
      data: { overlay },
    });
  }

  // ── /personality.moodContext — lightweight mood+guidance only ────────────────
  if (action === 'personality.moodContext') {
    const ctx = await personalityDoc.getMoodContext();
    return jsonResponse(res, 200, {
      version: 'mcp.v1',
      service: 'personality-service',
      action: 'personality.moodContext',
      requestId,
      status: 'ok',
      data: ctx,
    });
  }

  // ── /heartbeat.tier2 — manual trigger for awareness check (debug/testing) ────
  if (action === 'heartbeat.tier2') {
    heartbeat.runTier2().catch(() => {});
    return jsonResponse(res, 200, { status: 'ok', message: 'Tier-2 awareness check triggered' });
  }

  // ── /heartbeat.tier3 — manual trigger for deep reflection (debug/testing) ────
  if (action === 'heartbeat.tier3') {
    heartbeat.runTier3().catch(() => {});
    return jsonResponse(res, 200, { status: 'ok', message: 'Tier-3 deep reflection triggered' });
  }

  return jsonResponse(res, 404, { error: 'Unknown action: ' + action });
}

// ── Start server ───────────────────────────────────────────────────────────────

const server = http.createServer(handleRequest);

server.listen(PORT, '127.0.0.1', () => {
  logger.info('[PersonalityService] Started', { port: PORT });

  console.log('\n🧠 ThinkDrop Personality Service running');
  console.log('   Port: ' + PORT);
  console.log('   Endpoints:');
  console.log('     POST /personality.getState');
  console.log('     POST /personality.event');
  console.log('     POST /personality.resetState');
  console.log('     POST /personality.getTraits');
  console.log('     POST /personality.upsertTrait');
  console.log('     POST /personality.getOverlay');
  console.log('     POST /personality.overlay');
  console.log('     POST /personality.moodContext');
  console.log('     GET  /health\n');

  // Start heartbeat daemon
  heartbeat.start();
  logger.info('[PersonalityService] Heartbeat daemon started');
});

server.on('error', (err) => {
  logger.error('[PersonalityService] Server error', { error: err.message });
  process.exit(1);
});

// Graceful shutdown
process.on('SIGTERM', () => {
  logger.info('[PersonalityService] SIGTERM — shutting down');
  heartbeat.stop();
  server.close(() => process.exit(0));
});

process.on('SIGINT', () => {
  logger.info('[PersonalityService] SIGINT — shutting down');
  heartbeat.stop();
  server.close(() => process.exit(0));
});
