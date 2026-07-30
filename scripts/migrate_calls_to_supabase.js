/**
 * Supabase'e geçmeden önce yerel SQLite'a (calls.db) yazılmış çağrı kayıtlarını
 * ortak Supabase `calls` tablosuna taşır.
 *
 * Kullanım:  node scripts/migrate_calls_to_supabase.js
 *
 * Güvenli: call_sid üzerinden upsert + ignoreDuplicates → tekrar çalıştırılabilir,
 * Supabase'de zaten var olan bir kaydın üzerine YAZMAZ. Yerel calls.db'ye dokunmaz
 * (readonly açılır), yani kaynak veri her hâlükârda korunur.
 */
require('dotenv').config();
const path = require('path');
const Database = require('better-sqlite3');
const { createClient } = require('@supabase/supabase-js');

const DB_FILE = process.argv[2] || path.join(__dirname, '..', 'calls.db');
const { SUPABASE_URL, SUPABASE_SERVICE_KEY } = process.env;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('✗ SUPABASE_URL ve SUPABASE_SERVICE_KEY .env dosyasında tanımlı olmalı.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

(async function main() {
  let db;
  try {
    db = new Database(DB_FILE, { readonly: true, fileMustExist: true });
  } catch (e) {
    console.error('✗ SQLite açılamadı:', DB_FILE, '|', e.message);
    process.exit(1);
  }

  const rows = db.prepare(`
    SELECT call_sid, stream_sid, worker_phone, start_time, end_time, duration_seconds,
           session_state, final_intent, model_used, recording_url, transcript,
           silence_count, ttfa_ms, stt_ms, llm_ms, tts_ms, deepgram_cost, created_at
    FROM calls
    WHERE call_sid IS NOT NULL
    ORDER BY created_at ASC
  `).all();
  db.close();

  console.log(`Yerel kayıt sayısı: ${rows.length}`);
  if (!rows.length) {
    console.log('Taşınacak kayıt yok.');
    return;
  }

  const before = await supabase.from('calls').select('id', { count: 'exact', head: true });
  console.log(`Supabase mevcut kayıt: ${before.count ?? '?'}`);

  // id kolonu taşınmaz (Postgres identity kendi üretir); created_at korunur ki sıralama bozulmasın.
  const payload = rows.map((r) => ({
    call_sid: r.call_sid,
    stream_sid: r.stream_sid,
    worker_phone: r.worker_phone,
    start_time: r.start_time,
    end_time: r.end_time,
    duration_seconds: r.duration_seconds,
    session_state: r.session_state,
    final_intent: r.final_intent,
    model_used: r.model_used,
    recording_url: r.recording_url,
    transcript: r.transcript,
    silence_count: r.silence_count,
    ttfa_ms: r.ttfa_ms,
    stt_ms: r.stt_ms,
    llm_ms: r.llm_ms,
    tts_ms: r.tts_ms,
    deepgram_cost: r.deepgram_cost,
    created_at: r.created_at || undefined,
    instance_name: process.env.INSTANCE_NAME || null,
  }));

  const { error } = await supabase
    .from('calls')
    .upsert(payload, { onConflict: 'call_sid', ignoreDuplicates: true });

  if (error) {
    console.error('✗ Aktarım başarısız:', error.message);
    process.exit(1);
  }

  const after = await supabase.from('calls').select('id', { count: 'exact', head: true });
  console.log(`Supabase yeni kayıt sayısı: ${after.count ?? '?'}`);
  console.log(`✓ Tamamlandı — eklenen: ${(after.count ?? 0) - (before.count ?? 0)} (zaten var olanlar atlandı)`);
})();
