'use strict';

/**
 * emotion-engine.cjs
 *
 * VAD (Valence-Arousal-Dominance) state machine for ThinkDrop's persistent emotional state.
 * Reads and writes to the personality_state table via the user-memory MCP (port 3001).
 *
 * All emotion changes are applied through named events with predefined deltas.
 * Natural decay runs on every heartbeat Tier-1 tick (every 30s).
 *
 * Event types:
 *   positive_feedback, user_insult, user_raised_voice, user_frustrated,
 *   task_success, task_failure, ignored, repetitive_request,
 *   discovery_made, proactive_act_done
 */

const http = require('http');
const logger = require('./logger.cjs');

const MEMORY_SERVICE_PORT = parseInt(process.env.MEMORY_SERVICE_PORT || '3001', 10);
const MEM_API_KEY = process.env.MCP_USER_MEMORY_API_KEY || process.env.USER_MEMORY_API_KEY || process.env.MCP_API_KEY || '';

// ── Natural decay constants ────────────────────────────────────────────────────
const DECAY_RATE = 0.02; // 2% toward 0 per tick

// ── HTTP helper for user-memory MCP ───────────────────────────────────────────

function mcpPost(action, payload) {
  return new Promise((resolve) => {
    const envelope = {
      version: 'mcp.v1',
      service: 'user-memory',
      action,
      payload: payload || {},
      requestId: 'emo_' + Date.now(),
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
      timeout: 6000,
    }, (res) => {
      let raw = '';
      res.on('data', c => { raw += c; });
      res.on('end', () => {
        try { resolve(JSON.parse(raw)); } catch (_) { resolve(null); }
      });
    });
    req.on('error', (e) => { logger.warn('[EmotionEngine] MCP HTTP error', { error: e.message }); resolve(null); });
    req.on('timeout', () => { req.destroy(); resolve(null); });
    req.write(body);
    req.end();
  });
}

// ── Public API ─────────────────────────────────────────────────────────────────

/**
 * Get current personality state from user-memory MCP.
 * @returns {{ mood_label, valence, arousal, dominance, behavioral_guidance, hurt_count, joy_count } | null}
 */
async function getState() {
  try {
    const res = await mcpPost('personality.getState', {});
    return res?.data?.state || null;
  } catch (e) {
    logger.warn('[EmotionEngine] getState failed', { error: e.message });
    return null;
  }
}

/**
 * Apply a named mood event. Fire-and-forget friendly (returns result or null).
 * @param {string} eventType - e.g. 'positive_feedback', 'user_insult'
 * @param {string} source    - where the event came from ('voice', 'text', 'heartbeat')
 * @param {string} reason    - optional human-readable reason
 */
async function applyEvent(eventType, source, reason) {
  try {
    const res = await mcpPost('personality.event', {
      event_type: eventType,
      source: source || 'unknown',
      reason: reason || '',
    });
    if (res?.data) {
      logger.info('[EmotionEngine] Event applied', {
        event: eventType,
        prev: res.data.previous_mood,
        new: res.data.new_mood,
        valence: res.data.valence?.toFixed(3),
      });
    }
    return res?.data || null;
  } catch (e) {
    logger.warn('[EmotionEngine] applyEvent failed', { event: eventType, error: e.message });
    return null;
  }
}

/**
 * Apply natural emotional decay toward baseline (0.0).
 * Called by heartbeat Tier-1 every 30s.
 * If mood is already neutral/content, skips the MCP call.
 */
async function applyNaturalDecay() {
  try {
    const state = await getState();
    if (!state) return;

    const { valence, arousal } = state;
    const absVal = Math.abs(valence);
    const absAro = Math.abs(arousal);

    // Skip if already very close to baseline
    if (absVal < 0.02 && absAro < 0.02) return;

    // Only apply decay event if meaningfully away from baseline
    if (absVal > 0.05 || absAro > 0.05) {
      await applyEvent('natural_decay', 'heartbeat', 'Periodic natural emotional decay toward neutral');
      logger.debug('[EmotionEngine] Natural decay applied', {
        valence: valence?.toFixed(3),
        arousal: arousal?.toFixed(3),
      });
    }
  } catch (e) {
    logger.warn('[EmotionEngine] applyNaturalDecay failed', { error: e.message });
  }
}

/**
 * Get the full personality overlay text block for LLM injection.
 * @returns {string} Formatted overlay block, or '' if unavailable
 */
async function getOverlay() {
  try {
    const res = await mcpPost('personality.getOverlay', {});
    return res?.data?.overlay || '';
  } catch (e) {
    logger.warn('[EmotionEngine] getOverlay failed', { error: e.message });
    return '';
  }
}

/**
 * Reset ThinkDrop's emotional state to content/neutral baseline.
 */
async function resetState() {
  try {
    const res = await mcpPost('personality.resetState', {});
    logger.info('[EmotionEngine] State reset to content');
    return res?.data || null;
  } catch (e) {
    logger.warn('[EmotionEngine] resetState failed', { error: e.message });
    return null;
  }
}

/**
 * Update a personality trait (user_interests, user_projects, etc.)
 * @param {string} traitKey   - e.g. 'user_interests'
 * @param {string} traitValue - the new value text
 * @param {string} source     - 'learned' | 'system' | 'user_set'
 */
async function upsertTrait(traitKey, traitValue, source) {
  try {
    const res = await mcpPost('personality.upsertTrait', {
      trait_key: traitKey,
      trait_value: traitValue,
      source: source || 'learned',
      weight: 1.0,
    });
    logger.info('[EmotionEngine] Trait updated', { trait_key: traitKey });
    return res?.data || null;
  } catch (e) {
    logger.warn('[EmotionEngine] upsertTrait failed', { error: e.message });
    return null;
  }
}

// Override natural_decay to apply actual VAD math locally and push result
// rather than calling personality.event with a fake event type.
// We patch the EVENT_DELTAS in the MCP route to handle 'natural_decay' specially,
// OR we apply a lightweight direct update. For now we rely on the MCP route
// to handle 'natural_decay' as a special case that applies the decay math.
// The MCP route will see event_type='natural_decay' and apply:
//   newValence = valence * (1 - DECAY_RATE)
//   newArousal = arousal * (1 - DECAY_RATE)
// We'll add that to the personality route's EVENT_DELTAS.

module.exports = {
  getState,
  applyEvent,
  applyNaturalDecay,
  getOverlay,
  resetState,
  upsertTrait,
  DECAY_RATE,
};
