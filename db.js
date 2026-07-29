// Supabase tabanlı veri katmanı. Yerel SQLite (better-sqlite3) yerine geçer;
// çağrı kayıtları + karşılaştırma sonuçları instance'lar arasında paylaşılır.
// NOT: tüm export'lar artık ASYNC (SQLite'ta senkrondu) → çağıranlar await eklemeli.
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  throw new Error('SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in .env');
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

// ─── calls ───

async function insertCallStart({ callSid, streamSid, workerPhone, startTime, modelUsed }) {
  const { error } = await supabase.from('calls').upsert(
    {
      call_sid: callSid,
      stream_sid: streamSid || null,
      worker_phone: workerPhone || null,
      start_time: startTime,
      model_used: modelUsed,
      session_state: 'ACTIVE',
    },
    { onConflict: 'call_sid', ignoreDuplicates: true }   // eski "INSERT OR IGNORE"
  );
  if (error) console.warn('[DB] insertCallStart:', error.message);
}

async function updateCallEnd({ callSid, streamSid, workerPhone, endTime, durationSeconds, sessionState, finalIntent, recordingUrl, transcript, silenceCount, ttfaMs, sttMs, llmMs, ttsMs, deepgramCost }) {
  // Her zaman yazılan alanlar (eski COALESCE'siz kolonlar)
  const patch = {
    end_time: endTime,
    duration_seconds: durationSeconds,
    session_state: sessionState || 'UNKNOWN',
    final_intent: finalIntent || null,
    transcript: JSON.stringify(transcript || []),
    silence_count: silenceCount || 0,
  };
  // "null ise dokunma" alanları (eski COALESCE kolonları): sadece değer varsa üzerine yaz
  if (streamSid != null) patch.stream_sid = streamSid;
  if (workerPhone != null) patch.worker_phone = workerPhone;
  if (recordingUrl != null) patch.recording_url = recordingUrl;
  if (ttfaMs != null) patch.ttfa_ms = ttfaMs;
  if (sttMs != null) patch.stt_ms = sttMs;
  if (llmMs != null) patch.llm_ms = llmMs;
  if (ttsMs != null) patch.tts_ms = ttsMs;
  if (deepgramCost != null) patch.deepgram_cost = deepgramCost;

  const { error } = await supabase.from('calls').update(patch).eq('call_sid', callSid);
  if (error) console.warn('[DB] updateCallEnd:', error.message);
}

async function updateCallLatency({ callSid, sttMs, llmMs, ttsMs, deepgramCost, ttfaMs }) {
  const patch = {};
  if (ttfaMs != null) patch.ttfa_ms = ttfaMs;
  if (sttMs != null) patch.stt_ms = sttMs;
  if (llmMs != null) patch.llm_ms = llmMs;
  if (ttsMs != null) patch.tts_ms = ttsMs;
  if (deepgramCost != null) patch.deepgram_cost = deepgramCost;
  if (!Object.keys(patch).length) return;
  const { error } = await supabase.from('calls').update(patch).eq('call_sid', callSid);
  if (error) console.warn('[DB] updateCallLatency:', error.message);
}

async function updateRecordingUrl(callSid, recordingUrl) {
  const { error } = await supabase.from('calls').update({ recording_url: recordingUrl }).eq('call_sid', callSid);
  if (error) console.warn('[DB] updateRecordingUrl:', error.message);
}

async function updateCallSessionState(callSid, sessionState) {
  const { error } = await supabase.from('calls').upsert(
    { call_sid: callSid, session_state: sessionState },
    { onConflict: 'call_sid' }
  );
  if (error) console.warn('[DB] updateCallSessionState:', error.message);
}

async function listCalls(page, limit) {
  const from = (page - 1) * limit;
  const { data, count, error } = await supabase
    .from('calls')
    .select('*', { count: 'exact' })
    .order('created_at', { ascending: false })
    .range(from, from + limit - 1);
  if (error) { console.warn('[DB] listCalls:', error.message); return { calls: [], total: 0, page, limit }; }
  return { calls: data || [], total: count || 0, page, limit };
}

async function getCallBySid(callSid) {
  const { data, error } = await supabase.from('calls').select('*').eq('call_sid', callSid).maybeSingle();
  if (error) { console.warn('[DB] getCallBySid:', error.message); return null; }
  return data || null;
}

// ─── comparisons ───

async function insertComparison({ transcript, expected, runs, sttMs, sttSource, durationSec, deepgramCost, results }) {
  const { data, error } = await supabase.from('comparisons').insert({
    transcript: transcript || '',
    expected: expected || null,
    runs: runs || 1,
    stt_ms: sttMs != null ? sttMs : null,
    stt_source: sttSource || null,
    duration_sec: durationSec != null ? durationSec : null,
    deepgram_cost: deepgramCost != null ? deepgramCost : null,
    results_json: results || {},          // jsonb kolon → obje direkt saklanır
  }).select('id').maybeSingle();
  if (error) { console.warn('[DB] insertComparison:', error.message); return null; }
  return data ? data.id : null;
}

async function listComparisons(page, limit) {
  const from = (page - 1) * limit;
  const { data, count, error } = await supabase
    .from('comparisons')
    .select('*', { count: 'exact' })
    .order('created_at', { ascending: false })
    .range(from, from + limit - 1);
  if (error) { console.warn('[DB] listComparisons:', error.message); return { comparisons: [], total: 0, page, limit }; }
  const comparisons = (data || []).map(function (r) {
    return {
      id: r.id, created_at: r.created_at, transcript: r.transcript,
      expected: r.expected, runs: r.runs, stt_ms: r.stt_ms,
      stt_source: r.stt_source, duration_sec: r.duration_sec,
      deepgram_cost: r.deepgram_cost, results: r.results_json || {},
    };
  });
  return { comparisons, total: count || 0, page, limit };
}

module.exports = {
  insertCallStart,
  updateCallEnd,
  updateCallLatency,
  updateRecordingUrl,
  updateCallSessionState,
  listCalls,
  getCallBySid,
  insertComparison,
  listComparisons,
};