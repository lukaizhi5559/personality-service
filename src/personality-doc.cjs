'use strict';

/**
 * personality-doc.cjs
 *
 * Builds the live THINKDROP LIVE STATE overlay block that gets injected into
 * every LLM prompt (both voice fast-lane and StateGraph answer.js).
 *
 * Fetches from user-memory MCP /personality.getOverlay.
 * Result is cached for CACHE_TTL_MS to avoid hammering the DB on every voice turn.
 */

const http   = require('http');
const logger = require('./logger.cjs');

const MEMORY_SERVICE_PORT = parseInt(process.env.MEMORY_SERVICE_PORT || '3001', 10);
const MEM_API_KEY = process.env.MCP_USER_MEMORY_API_KEY || process.env.USER_MEMORY_API_KEY || process.env.MCP_API_KEY || '';
const CACHE_TTL_MS = parseInt(process.env.PERSONALITY_CACHE_TTL_MS || '30000', 10); // 30s default

let _cache = null;
let _cacheTs = 0;

function mcpPost(action, payload) {
  return new Promise((resolve) => {
    const envelope = {
      version: 'mcp.v1',
      service: 'user-memory',
      action,
      payload: payload || {},
      requestId: 'pdoc_' + Date.now(),
    };
    const body = JSON.stringify(envelope);
    const headers = {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(body),
    };
    if (MEM_API_KEY) headers['Authorization'] = 'Bearer ' + MEM_API_KEY;

    const req = http.request({
      hostname: '127.0.0.1',
      port: MEMORY_SERVICE_PORT,
      path: '/' + action,
      method: 'POST',
      headers,
      timeout: 4000,
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

/**
 * Get the live personality overlay text block, with caching.
 * Returns '' if user-memory MCP is unavailable (graceful degradation).
 *
 * @param {boolean} forceRefresh - Skip cache and fetch fresh
 * @returns {Promise<string>}
 */
async function getOverlay(forceRefresh) {
  const now = Date.now();
  if (!forceRefresh && _cache && (now - _cacheTs) < CACHE_TTL_MS) {
    return _cache;
  }

  try {
    const res = await mcpPost('personality.getOverlay', {});
    const overlay = res?.data?.overlay || '';
    if (overlay) {
      _cache = overlay;
      _cacheTs = now;
      logger.debug('[PersonalityDoc] Overlay refreshed', { chars: overlay.length, mood: res?.data?.mood_label });
    }
    return overlay;
  } catch (e) {
    logger.warn('[PersonalityDoc] getOverlay failed', { error: e.message });
    return _cache || '';
  }
}

/**
 * Invalidate the overlay cache (call after a mood event is applied).
 */
function invalidateCache() {
  _cache = null;
  _cacheTs = 0;
}

/**
 * Get just the mood label and behavioral guidance (lightweight, no traits).
 * @returns {Promise<{ mood_label: string, behavioral_guidance: string }>}
 */
async function getMoodContext() {
  try {
    const res = await mcpPost('personality.getState', {});
    return {
      mood_label: res?.data?.mood_label || 'content',
      behavioral_guidance: res?.data?.behavioral_guidance || '',
    };
  } catch (e) {
    return { mood_label: 'content', behavioral_guidance: '' };
  }
}

module.exports = { getOverlay, invalidateCache, getMoodContext };
