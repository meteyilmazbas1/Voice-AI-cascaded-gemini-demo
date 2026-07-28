const path = require('path');
const Database = require('better-sqlite3');

const dbPath = path.join(__dirname, 'calls.db');
const db = new Database(dbPath);

db.pragma('journal_mode = WAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS calls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    call_sid TEXT UNIQUE,
    stream_sid TEXT,
    worker_phone TEXT,
    start_time TEXT,
    end_time TEXT,
    duration_seconds INTEGER,
    session_state TEXT,
    final_intent TEXT,
    model_used TEXT,
    recording_url TEXT,
    transcript TEXT,
    silence_count INTEGER DEFAULT 0,
    ttfa_ms INTEGER DEFAULT NULL,
    created_at TEXT DEFAULT (datetime('now'))
  )
`);

try {
  db.exec('ALTER TABLE calls ADD COLUMN ttfa_ms INTEGER DEFAULT NULL');
} catch (e) {
  // column already exists
}
try {
  db.exec('ALTER TABLE calls ADD COLUMN stt_ms INTEGER DEFAULT NULL');
} catch (e) {}
try {
  db.exec('ALTER TABLE calls ADD COLUMN llm_ms INTEGER DEFAULT NULL');
} catch (e) {}
try {
  db.exec('ALTER TABLE calls ADD COLUMN tts_ms INTEGER DEFAULT NULL');
} catch (e) {}
try {
  db.exec('ALTER TABLE calls ADD COLUMN deepgram_cost REAL DEFAULT NULL');
} catch (e) {}

const insertCallStmt = db.prepare(`
  INSERT OR IGNORE INTO calls (call_sid, stream_sid, worker_phone, start_time, model_used, session_state)
  VALUES (?, ?, ?, ?, ?, ?)
`);

const updateCallStmt = db.prepare(`
  UPDATE calls SET
    stream_sid = COALESCE(?, stream_sid),
    worker_phone = COALESCE(?, worker_phone),
    end_time = ?,
    duration_seconds = ?,
    session_state = ?,
    final_intent = ?,
    recording_url = COALESCE(?, recording_url),
    transcript = ?,
    silence_count = ?,
    ttfa_ms = COALESCE(?, ttfa_ms),
    stt_ms = COALESCE(?, stt_ms),
    llm_ms = COALESCE(?, llm_ms),
    tts_ms = COALESCE(?, tts_ms),
    deepgram_cost = COALESCE(?, deepgram_cost)
  WHERE call_sid = ?
`);

const updateRecordingStmt = db.prepare(`
  UPDATE calls SET recording_url = ? WHERE call_sid = ?
`);

const updateLatencyStmt = db.prepare(`
  UPDATE calls SET
    ttfa_ms = COALESCE(?, ttfa_ms),
    stt_ms = COALESCE(?, stt_ms),
    llm_ms = COALESCE(?, llm_ms),
    tts_ms = COALESCE(?, tts_ms),
    deepgram_cost = COALESCE(?, deepgram_cost)
  WHERE call_sid = ?
`);

function insertCallStart({ callSid, streamSid, workerPhone, startTime, modelUsed }) {
  insertCallStmt.run(
    callSid,
    streamSid || null,
    workerPhone || null,
    startTime,
    modelUsed,
    'ACTIVE'
  );
}

function updateCallEnd({ callSid, streamSid, workerPhone, endTime, durationSeconds, sessionState, finalIntent, recordingUrl, transcript, silenceCount, ttfaMs, sttMs, llmMs, ttsMs, deepgramCost }) {
  updateCallStmt.run(
    streamSid || null,
    workerPhone || null,
    endTime,
    durationSeconds,
    sessionState || 'UNKNOWN',
    finalIntent || null,
    recordingUrl || null,
    JSON.stringify(transcript || []),
    silenceCount || 0,
    ttfaMs != null ? ttfaMs : null,
    sttMs != null ? sttMs : null,
    llmMs != null ? llmMs : null,
    ttsMs != null ? ttsMs : null,
    deepgramCost != null ? deepgramCost : null,
    callSid
  );
}

function updateCallLatency({ callSid, sttMs, llmMs, ttsMs, deepgramCost, ttfaMs }) {
  updateLatencyStmt.run(
    ttfaMs != null ? ttfaMs : null,
    sttMs != null ? sttMs : null,
    llmMs != null ? llmMs : null,
    ttsMs != null ? ttsMs : null,
    deepgramCost != null ? deepgramCost : null,
    callSid
  );
}

function updateRecordingUrl(callSid, recordingUrl) {
  updateRecordingStmt.run(recordingUrl, callSid);
}

const upsertSessionStateStmt = db.prepare(`
  INSERT INTO calls (call_sid, start_time, session_state)
  VALUES (?, datetime('now'), ?)
  ON CONFLICT(call_sid) DO UPDATE SET session_state = excluded.session_state
`);

function updateCallSessionState(callSid, sessionState) {
  upsertSessionStateStmt.run(callSid, sessionState);
}

function listCalls(page, limit) {
  const offset = (page - 1) * limit;
  const calls = db.prepare(`
    SELECT id, call_sid, worker_phone, start_time, end_time, duration_seconds,
           session_state, final_intent, model_used, recording_url, silence_count,
           ttfa_ms, stt_ms, llm_ms, tts_ms, deepgram_cost, created_at
    FROM calls ORDER BY created_at DESC LIMIT ? OFFSET ?
  `).all(limit, offset);
  const total = db.prepare('SELECT COUNT(*) as count FROM calls').get().count;
  return { calls, total, page, limit };
}

function getCallBySid(callSid) {
  return db.prepare('SELECT * FROM calls WHERE call_sid = ?').get(callSid);
}

module.exports = {
  db,
  insertCallStart,
  updateCallEnd,
  updateCallLatency,
  updateRecordingUrl,
  updateCallSessionState,
  listCalls,
  getCallBySid,
};
