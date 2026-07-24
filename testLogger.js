const fs = require('fs');
const path = require('path');

const CSV_PATH = path.join(__dirname, 'noise_test_results.csv');
const HEADER = '﻿timestamp,audio_file,deepfilter,transcript,transcript_rest,response,ttfa_ms,total_ms,df_ms,snr_db\n';

function csvEscape(v) {
  const s = v == null ? '' : String(v);
  return '"' + s.replace(/"/g, '""').replace(/\r?\n/g, ' ') + '"';
}

// Kolon seti degistiginde eski dosyaya eklemeye devam etmek CSV'yi bozar —
// eskisini .bak olarak saklayip yeni header'la temiz baslıyoruz.
function ensureHeader() {
  if (!fs.existsSync(CSV_PATH)) { fs.writeFileSync(CSV_PATH, HEADER, 'utf8'); return; }
  const firstLine = fs.readFileSync(CSV_PATH, 'utf8').split('\n')[0] + '\n';
  if (firstLine === HEADER) return;
  const bak = CSV_PATH.replace(/\.csv$/, '') + '_' + Date.now() + '.bak.csv';
  fs.renameSync(CSV_PATH, bak);
  console.warn('[TestLog] kolonlar degisti — eski dosya saklandi:', bak);
  fs.writeFileSync(CSV_PATH, HEADER, 'utf8');
}

function appendTestRow(row) {
  try {
    ensureHeader();
    const line = [
      new Date().toISOString(),
      row.audioFile,
      row.deepfilter ? 'ON' : 'OFF',
      row.transcript,
      row.transcriptRest,
      row.response,
      row.ttfaMs  != null ? row.ttfaMs  : '',
      row.totalMs != null ? row.totalMs : '',
      row.dfMs    != null ? row.dfMs    : '',
      row.snrDb   != null ? row.snrDb.toFixed(1) : '',
    ].map(csvEscape).join(',') + '\n';
    fs.appendFileSync(CSV_PATH, line, 'utf8');
    console.log('[TestLog] satir yazildi ->', CSV_PATH);
  } catch (e) {
    console.error('[TestLog] yazma hatasi:', e.message);
  }
}

module.exports = { appendTestRow, CSV_PATH };
