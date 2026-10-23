'use strict';

/**
 * decision.cjs — local typed-decision client for the personality service.
 *
 * POSTs to phi4's /decision.answer (Ollama System One — choice/noul/score,
 * no text generation). Used for bounded verdicts: noise gates, silence
 * verdicts, ethical gates, judgment score adjustments — the parts of
 * askLLMJson responses that are finite-vocabulary.
 *
 * Fail-open everywhere: DECISION_LOCAL=0 disables; any transport/model
 * failure returns null so callers fall back to the remote LLM path.
 */

const PHI4_URL = process.env.MCP_PHI4_URL || 'http://127.0.0.1:3009';
const PHI4_API_KEY = process.env.MCP_PHI4_API_KEY || process.env.MCP_API_KEY || '';
const DEFAULT_TIMEOUT_MS = parseInt(process.env.DECISION_TIMEOUT_MS || '3000', 10);

function decisionEnabled() { return process.env.DECISION_LOCAL !== '0'; }

/**
 * @param {Array} questions [{id, type:'choice'|'noul'|'score', question, options?, min?, max?}]
 * @param {object} [opts] { state?, timeoutMs? }
 * @returns {Promise<{answers,elapsedMs,model}|null>} null on any failure
 */
async function askDecision(questions, opts = {}) {
  if (!decisionEnabled()) return null;
  const start = Date.now();
  try {
    const res = await fetch(`${PHI4_URL}/decision.answer`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(PHI4_API_KEY ? { Authorization: `Bearer ${PHI4_API_KEY}` } : {}),
      },
      body: JSON.stringify({
        version: 'mcp.v1',
        service: 'phi4',
        action: 'decision.answer',
        requestId: `ps_dec_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
        payload: { state: opts.state || '', questions },
      }),
      // Batch-scaled — silence/action batches carry several questions;
      // flat 3s ceiling timed them out on the small-tier model.
      signal: AbortSignal.timeout(opts.timeoutMs
        || (process.env.DECISION_TIMEOUT_MS ? DEFAULT_TIMEOUT_MS : Math.min(1200 + 250 * (Array.isArray(questions) ? questions.length : 1), 12000))),
    });
    const body = await res.json().catch(() => null);
    if (!body || body.status !== 'ok' || !body.data?.answers) return null;
    return { answers: body.data.answers, elapsedMs: Date.now() - start, model: body.data.model };
  } catch {
    return null;
  }
}

module.exports = { askDecision, decisionEnabled };
