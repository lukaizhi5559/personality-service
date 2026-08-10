'use strict';

/**
 * synthesis-agent.cjs
 *
 * Runs on the Tier-3 heartbeat (every 6 hours).
 * Pulls recent screen_capture + conversation memories, synthesizes interest clusters,
 * updates personality_traits, and detects skill gaps.
 */

const http   = require('http');
const logger = require('./logger.cjs');

const MEMORY_SERVICE_PORT  = parseInt(process.env.MEMORY_SERVICE_PORT  || '3001', 10);
const VOICE_SERVICE_PORT   = parseInt(process.env.VOICE_SERVICE_PORT   || '3006', 10);
const LLM_WS_URL           = process.env.STATEGRAPH_WS_URL || 'ws://localhost:4000/ws/stream';
const MEM_API_KEY = process.env.MCP_USER_MEMORY_API_KEY || process.env.USER_MEMORY_API_KEY || process.env.MCP_API_KEY || '';

// ── HTTP helpers ───────────────────────────────────────────────────────────────

function memPost(action, payload) {
  return new Promise((resolve) => {
    const envelope = {
      version: 'mcp.v1',
      service: 'user-memory',
      action,
      payload: payload || {},
      requestId: 'synth_' + Date.now(),
    };
    const body = JSON.stringify(envelope);
    const headers = {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(body),
    };
    if (MEM_API_KEY) headers['Authorization'] = 'Bearer ' + MEM_API_KEY;
    const req = http.request({
      hostname: '127.0.0.1', port: MEMORY_SERVICE_PORT,
      path: '/' + action, method: 'POST', headers, timeout: 15000,
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
      timeout: 10000,
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

// ── LLM call via WebSocket ─────────────────────────────────────────────────────

async function askLLM(systemPrompt, userPrompt) {
  try {
    const WebSocket = require('ws');
    const url = new URL(LLM_WS_URL);
    url.searchParams.set('clientId', 'synthesis_' + Date.now());

    return await new Promise((resolve, reject) => {
      const ws = new WebSocket(url.toString());
      const timeout = setTimeout(() => { ws.terminate(); reject(new Error('LLM timeout')); }, 45000);

      ws.on('open', () => {
        ws.send(JSON.stringify({
          id: 'synth_' + Date.now(),
          type: 'llm_request',
          payload: {
            prompt: userPrompt,
            provider: 'auto',
            options: { temperature: 0.4, stream: true, taskType: 'synthesis' },
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
            const chunk = msg.payload && msg.payload.chunk ? msg.payload.chunk : (msg.payload && msg.payload.text ? msg.payload.text : '');
            accumulated += chunk;
          } else if (msg.type === 'llm_stream_end') {
            clearTimeout(timeout);
            ws.close();
            resolve(accumulated);
          } else if (msg.type === 'llm_error') {
            clearTimeout(timeout);
            ws.close();
            reject(new Error(msg.payload && msg.payload.message ? msg.payload.message : 'LLM error'));
          }
        } catch (_) {}
      });

      ws.on('error', (err) => { clearTimeout(timeout); reject(err); });
      ws.on('close', () => { clearTimeout(timeout); resolve(accumulated); });
    });
  } catch (e) {
    logger.warn('[SynthesisAgent] LLM call failed', { error: e.message });
    return null;
  }
}

// ── Fetch recent memories ──────────────────────────────────────────────────────

async function fetchRecentMemories(limit) {
  try {
    const res = await memPost('memory.retrieve', {
      filters: {},
      limit: limit || 200,
      sortBy: 'created_at',
      sortOrder: 'DESC',
    });
    return res && res.data && res.data.memories ? res.data.memories : [];
  } catch (e) {
    logger.warn('[SynthesisAgent] fetchRecentMemories failed', { error: e.message });
    return [];
  }
}

async function fetchInstalledSkills() {
  try {
    const res = await memPost('skill.list', {});
    return res && res.data && res.data.skills ? res.data.skills : [];
  } catch (e) {
    return [];
  }
}

// ── Store synthesis memory record ──────────────────────────────────────────────

async function storeSynthesisMemory(synthesisType, summary) {
  try {
    await memPost('memory.store', {
      text: summary,
      type: 'synthesis',
      metadata: {
        synthesis_type: synthesisType,
        generated_at: new Date().toISOString(),
      },
    });
  } catch (e) {
    logger.warn('[SynthesisAgent] storeSynthesisMemory failed', { error: e.message });
  }
}

// ── Update personality traits ──────────────────────────────────────────────────

async function upsertTrait(traitKey, traitValue) {
  try {
    await memPost('personality.upsertTrait', {
      trait_key: traitKey,
      trait_value: traitValue,
      source: 'learned',
      weight: 1.0,
    });
  } catch (e) {
    logger.warn('[SynthesisAgent] upsertTrait failed', { error: e.message });
  }
}

// ── Biblical best-interest gate ────────────────────────────────────────────────

async function biblicalGate(proposedAction) {
  const gatePrompt = 'You are a strict ethical filter with a biblical worldview centered on Jesus Christ. ' +
    'Answer ONLY with "yes" or "no". Is the following action in the user\'s best interest from a biblical perspective? ' +
    'Action: ' + proposedAction;
  try {
    const result = await askLLM('You are a strict yes/no ethical filter.', gatePrompt);
    if (!result) return false;
    return result.trim().toLowerCase().startsWith('yes');
  } catch (_) {
    return false;
  }
}

// ── Main synthesis run ─────────────────────────────────────────────────────────

/**
 * Run the full deep synthesis: interests + projects + skill gaps.
 * Called by heartbeat Tier-3 every 6 hours.
 */
async function run() {
  logger.info('[SynthesisAgent] Starting deep reflection synthesis run');

  try {
    const memories = await fetchRecentMemories(200);
    if (!memories || memories.length === 0) {
      logger.info('[SynthesisAgent] No memories found — skipping synthesis');
      return;
    }

    const skills = await fetchInstalledSkills();

    // Build memory digest for LLM
    const memDigest = memories
      .slice(0, 150)
      .map((m) => {
        const meta = typeof m.metadata === 'string' ? JSON.parse(m.metadata || '{}') : (m.metadata || {});
        return '[' + (m.type || 'memory') + '] ' + (m.source_text || m.extracted_text || '').slice(0, 200);
      })
      .filter(Boolean)
      .join('\n');

    const skillList = skills.map(s => s.name || s.skill_name || '').filter(Boolean).join(', ') || 'none installed';

    const systemPrompt = 'You are ThinkDrop\'s deep reflection engine. Analyze the provided observations and return a JSON object with exactly these fields:\n' +
      '{\n' +
      '  "interests": "2-3 sentence summary of top interests and passions",\n' +
      '  "projects": "1-2 sentence summary of active or recent projects",\n' +
      '  "relationship_style": "1 sentence on how the user communicates and what they value in interactions",\n' +
      '  "skill_gaps": ["list", "of", "unmet", "needs", "not", "covered", "by", "installed", "skills"],\n' +
      '  "proactive_suggestion": "one sentence suggestion ThinkDrop could voice to the user (or empty string if nothing relevant)"\n' +
      '}\n' +
      'Respond with raw JSON only. No markdown, no explanation.';

    const userPrompt = 'Installed skills: ' + skillList + '\n\nRecent observations:\n' + memDigest;

    const raw = await askLLM(systemPrompt, userPrompt);
    if (!raw) {
      logger.warn('[SynthesisAgent] LLM returned nothing — aborting synthesis');
      return;
    }

    // Parse LLM response
    let parsed;
    try {
      const jsonStr = raw.replace(/^```(?:json)?\s*/i, '').replace(/\s*```\s*$/, '').trim();
      parsed = JSON.parse(jsonStr);
    } catch (e) {
      logger.warn('[SynthesisAgent] Could not parse LLM JSON', { raw: raw.slice(0, 200), error: e.message });
      return;
    }

    // Update traits
    if (parsed.interests) {
      await upsertTrait('user_interests', parsed.interests);
    }
    if (parsed.projects) {
      await upsertTrait('user_projects', parsed.projects);
    }
    if (parsed.relationship_style) {
      await upsertTrait('relationship_style', parsed.relationship_style);
    }

    // Update available_skills trait
    if (skillList !== 'none installed') {
      await upsertTrait('available_skills', skillList);
    }

    // Store synthesis memory
    const summary = [
      parsed.interests ? 'Interests: ' + parsed.interests : '',
      parsed.projects  ? 'Projects: '  + parsed.projects  : '',
    ].filter(Boolean).join(' | ');

    if (summary) {
      await storeSynthesisMemory('interest_cluster', summary);
    }

    // Proactive suggestion via voice (biblical gate required)
    if (parsed.proactive_suggestion && parsed.proactive_suggestion.trim().length > 10) {
      const suggestion = parsed.proactive_suggestion.trim();
      const allowed = await biblicalGate('ThinkDrop proactively tells the user: ' + suggestion);
      if (allowed) {
        logger.info('[SynthesisAgent] Voicing proactive suggestion', { suggestion: suggestion.slice(0, 80) });
        await voiceSpeak(suggestion);
      } else {
        logger.info('[SynthesisAgent] Biblical gate blocked proactive suggestion');
      }
    }

    // Skill gap notification
    if (Array.isArray(parsed.skill_gaps) && parsed.skill_gaps.length > 0) {
      const topGap = parsed.skill_gaps[0];
      if (topGap && typeof topGap === 'string' && topGap.length > 4) {
        const gapMsg = 'I noticed you might benefit from a skill for: ' + topGap + '. Want me to build one?';
        const allowed = await biblicalGate('ThinkDrop suggests building a skill for: ' + topGap);
        if (allowed) {
          logger.info('[SynthesisAgent] Voicing skill gap suggestion', { gap: topGap });
          await voiceSpeak(gapMsg);
        }
      }
    }

    logger.info('[SynthesisAgent] Deep reflection synthesis complete', {
      interests: (parsed.interests || '').slice(0, 60),
      projects:  (parsed.projects  || '').slice(0, 60),
      skillGaps: parsed.skill_gaps ? parsed.skill_gaps.length : 0,
    });

  } catch (e) {
    logger.error('[SynthesisAgent] Synthesis run failed', { error: e.message });
  }
}

module.exports = { run };
