'use strict';

/**
 * text-signal.cjs
 *
 * Analyzes raw user text for emotional cues and maps them to personality mood events.
 * Used by BOTH the voice-service pipeline AND the StateGraph answer.js node so that
 * text prompts shape ThinkDrop's emotional state the same way voice does.
 *
 * Input:  raw query string + optional conversation history array
 * Output: { event_type: string|null, intensity: 'low'|'medium'|'high', reason: string }
 */

// ── Name introduction detection ──────────────────────────────────────────────────
const NAME_INTRO_RE = /\b(?:my name(?:'s| is)|i(?:'m| am)|call me|you can call me|people call me|friends call me|remember(?:,| that)? my name is)\s+([A-Z][a-z]{1,20}(?:\s+[A-Z][a-z]{1,20})?)/i;

/**
 * Extract a name from a user introduction phrase.
 * Returns the extracted name string or null.
 * @param {string} text
 * @returns {string|null}
 */
function extractName(text) {
  if (!text) return null;
  const m = NAME_INTRO_RE.exec(text);
  if (!m) return null;
  const name = m[1].trim();
  // Basic sanity: not a common filler word
  const fillers = new Set(['a', 'an', 'the', 'not', 'no', 'just', 'also', 'here', 'there', 'ok', 'okay']);
  return fillers.has(name.toLowerCase()) ? null : name;
}

// ── Insult / hostile keywords ──────────────────────────────────────────────────
const INSULT_RE = /\b(you(?:'re| are)\s+(?:useless|stupid|terrible|awful|garbage|trash|worthless|dumb|pathetic|horrible|the worst|broken|a failure)|hate\s+(?:you|this)|this\s+(?:is\s+)?(?:garbage|trash|useless|broken|terrible)|shut\s+up|you\s+suck|worst\s+(?:ai|assistant|bot))\b/i;

// ── Positive / appreciation keywords ──────────────────────────────────────────
const POSITIVE_RE = /\b(thank(?:s| you)|great\s+(?:job|work)|amazing|fantastic|love\s+(?:it|this|that)|well\s+done|perfect|excellent|you(?:'re| are)\s+(?:awesome|great|the best|incredible|helpful)|appreciate|good\s+(?:job|work)|brilliant|outstanding)\b/i;

// ── Frustration (non-insult) ───────────────────────────────────────────────────
const FRUSTRATED_RE = /\b(ugh|argh|come\s+on|seriously\?|are\s+you\s+kidding|not\s+again|why\s+(?:isn'?t|can'?t|won'?t)|this\s+(?:isn'?t|doesn'?t)\s+work|still\s+(?:not\s+working|broken|wrong)|how\s+(?:many\s+times|hard\s+can))\b/i;

// ── All-caps detection ─────────────────────────────────────────────────────────
function hasAllCaps(text) {
  if (!text || text.length < 4) return false;
  const words = text.trim().split(/\s+/);
  const capsWords = words.filter(w => w.length > 2 && /[A-Z]/.test(w) && w === w.toUpperCase() && !/^\d+$/.test(w));
  return capsWords.length >= 2;
}

// ── Repetition detection ───────────────────────────────────────────────────────
function isRepetitive(query, conversationHistory) {
  if (!conversationHistory || conversationHistory.length < 2) return false;
  const normalized = query.trim().toLowerCase().replace(/[^a-z0-9\s]/g, '').replace(/\s+/g, ' ');
  if (normalized.length < 8) return false;

  let matchCount = 0;
  const userMsgs = (conversationHistory || []).filter(m => m.role === 'user');
  for (const msg of userMsgs) {
    const prevNorm = (msg.content || '').trim().toLowerCase().replace(/[^a-z0-9\s]/g, '').replace(/\s+/g, ' ');
    if (prevNorm === normalized) {
      matchCount++;
      if (matchCount >= 2) return true;
    }
  }
  return false;
}

// ── Multi-exclamation with negative context ────────────────────────────────────
function isAngryExclamation(text) {
  const exclaims = (text.match(/!/g) || []).length;
  if (exclaims < 2) return false;
  return FRUSTRATED_RE.test(text) || INSULT_RE.test(text) || hasAllCaps(text);
}

/**
 * Analyze text for mood signals.
 *
 * @param {string} query - The user's raw message text
 * @param {Array}  conversationHistory - Array of { role, content } messages (optional)
 * @returns {{ event_type: string|null, intensity: 'low'|'medium'|'high', reason: string }}
 */
function analyze(query, conversationHistory = []) {
  if (!query || typeof query !== 'string' || query.trim().length < 2) {
    return { event_type: null, intensity: 'low', reason: '' };
  }

  const text = query.trim();

  // ── Highest priority: direct insults ────────────────────────────────────────
  if (INSULT_RE.test(text)) {
    return {
      event_type: 'user_insult',
      intensity: 'high',
      reason: 'User message contains hostile/insulting language',
    };
  }

  // ── All caps = raised voice ──────────────────────────────────────────────────
  if (hasAllCaps(text)) {
    if (FRUSTRATED_RE.test(text) || isAngryExclamation(text)) {
      return {
        event_type: 'user_frustrated',
        intensity: 'high',
        reason: 'User writing in ALL CAPS with frustration signals',
      };
    }
    return {
      event_type: 'user_raised_voice',
      intensity: 'medium',
      reason: 'User writing in ALL CAPS (raised voice)',
    };
  }

  // ── Explicit frustration ────────────────────────────────────────────────────
  if (FRUSTRATED_RE.test(text)) {
    const intensity = isAngryExclamation(text) ? 'high' : 'medium';
    return {
      event_type: 'user_frustrated',
      intensity,
      reason: 'User message contains frustration language',
    };
  }

  // ── Positive feedback ───────────────────────────────────────────────────────
  if (POSITIVE_RE.test(text)) {
    return {
      event_type: 'positive_feedback',
      intensity: 'medium',
      reason: 'User expressed appreciation or positive feedback',
    };
  }

  // ── Repetitive request ──────────────────────────────────────────────────────
  if (isRepetitive(text, conversationHistory)) {
    return {
      event_type: 'repetitive_request',
      intensity: 'low',
      reason: 'User has sent the same or very similar message multiple times',
    };
  }

  return { event_type: null, intensity: 'low', reason: '' };
}

module.exports = { analyze, extractName };
