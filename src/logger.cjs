'use strict';

const LOG_LEVEL = process.env.LOG_LEVEL || 'info';
const LEVELS = { debug: 0, info: 1, warn: 2, error: 3 };
const currentLevel = LEVELS[LOG_LEVEL] ?? 1;

function _log(level, msg, meta) {
  if ((LEVELS[level] ?? 0) < currentLevel) return;
  const ts = new Date().toISOString();
  const line = meta ? JSON.stringify({ ts, level, msg, ...meta }) : JSON.stringify({ ts, level, msg });
  if (level === 'error') {
    process.stderr.write(line + '\n');
  } else {
    process.stdout.write(line + '\n');
  }
}

module.exports = {
  debug: (msg, meta) => _log('debug', msg, meta),
  info:  (msg, meta) => _log('info',  msg, meta),
  warn:  (msg, meta) => _log('warn',  msg, meta),
  error: (msg, meta) => _log('error', msg, meta),
};
