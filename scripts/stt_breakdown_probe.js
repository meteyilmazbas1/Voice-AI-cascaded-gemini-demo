/**
 * One-off STT phase timing probe (mirrors transcribeCompareAudioDeepgram in server.js).
 * Usage: node scripts/stt_breakdown_probe.js
 */
require('dotenv').config();

const fs = require('fs');
const path = require('path');
const https = require('https');
const axios = require('axios');
const { spawn } = require('child_process');

const deepgramHttpsAgent = new https.Agent({ keepAlive: true, maxSockets: 10 });

const RUNS = 5;
const FILES = [
  { label: 'mp3', file: path.join('public', 'scenarios', 'safe_test.mp3'), hint: 'audio/mpeg' },
  { label: 'wav', file: path.join('public', 'scenarios', 'delayed_1sgurultu.wav'), hint: 'audio/wav' },
];

async function getAudioDurationSec(buf) {
  if (!buf || !buf.length) return null;

  try {
    if (buf.length >= 44 && buf.slice(0, 4).toString('ascii') === 'RIFF' && buf.slice(8, 12).toString('ascii') === 'WAVE') {
      const byteRate = buf.readUInt32LE(28);
      let dataSize = null;
      for (let off = 12; off + 8 <= buf.length; ) {
        const id = buf.slice(off, off + 4).toString('ascii');
        const size = buf.readUInt32LE(off + 4);
        if (id === 'data') { dataSize = size; break; }
        off += 8 + size + (size % 2);
      }
      if (byteRate > 0 && dataSize != null && dataSize > 0) {
        const sec = dataSize / byteRate;
        if (Number.isFinite(sec) && sec > 0) return sec;
      }
    }
  } catch (e) { /* fall through */ }

  const tmp = path.join(require('os').tmpdir(), 'cmp_audio_dur_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8) + '.bin');
  try {
    fs.writeFileSync(tmp, buf);
  } catch (e) {
    return null;
  }

  return new Promise(function (resolve) {
    const ff = spawn('ffprobe', [
      '-v', 'error',
      '-show_entries', 'format=duration',
      '-of', 'default=noprint_wrappers=1:nokey=1',
      tmp,
    ]);
    let stdout = '';
    let stderr = '';
    ff.stdout.on('data', function (d) { stdout += d.toString(); });
    ff.stderr.on('data', function (d) { stderr += d.toString(); });
    function cleanup(val) {
      try { fs.unlinkSync(tmp); } catch (e) {}
      resolve(val);
    }
    ff.on('error', function () {
      const ff2 = spawn('ffmpeg', ['-hide_banner', '-i', tmp, '-f', 'null', '-']);
      let err2 = '';
      ff2.stderr.on('data', function (d) { err2 += d.toString(); });
      ff2.on('error', function () { cleanup(null); });
      ff2.on('close', function () {
        const m = err2.match(/Duration:\s*(\d+):(\d+):(\d+\.?\d*)/);
        if (m) cleanup(parseFloat(m[1]) * 3600 + parseFloat(m[2]) * 60 + parseFloat(m[3]));
        else cleanup(null);
      });
    });
    ff.on('close', function () {
      const n = parseFloat(String(stdout).trim());
      if (Number.isFinite(n) && n > 0) {
        cleanup(n);
        return;
      }
      const m = stderr.match(/Duration:\s*(\d+):(\d+):(\d+\.?\d*)/);
      if (m) {
        cleanup(parseFloat(m[1]) * 3600 + parseFloat(m[2]) * 60 + parseFloat(m[3]));
      } else {
        cleanup(null);
      }
    });
  });
}

async function audioToPcm16kMono(inputBuffer) {
  return new Promise(function (resolve, reject) {
    const ffmpeg = spawn('ffmpeg', [
      '-hide_banner', '-loglevel', 'error',
      '-i', 'pipe:0',
      '-ar', '16000',
      '-ac', '1',
      '-f', 's16le',
      'pipe:1',
    ]);
    const chunks = [];
    ffmpeg.stdout.on('data', function (chunk) { chunks.push(chunk); });
    ffmpeg.stdout.on('end', function () { resolve(Buffer.concat(chunks)); });
    let errText = '';
    ffmpeg.stderr.on('data', function (d) { errText += d.toString(); });
    ffmpeg.on('error', function (e) {
      reject(new Error('ffmpeg failed: ' + e.message));
    });
    ffmpeg.on('close', function (code) {
      if (code !== 0 && chunks.length === 0) {
        reject(new Error('ffmpeg exit ' + code + (errText ? ': ' + errText.slice(0, 200) : '')));
      }
    });
    ffmpeg.stdin.write(inputBuffer);
    ffmpeg.stdin.end();
  });
}

function createCompareWavBuffer(pcm) {
  const dataSize = pcm.length;
  const buf = Buffer.alloc(44 + dataSize);
  buf.write('RIFF', 0);
  buf.writeUInt32LE(36 + dataSize, 4);
  buf.write('WAVE', 8);
  buf.write('fmt ', 12);
  buf.writeUInt32LE(16, 16);
  buf.writeUInt16LE(1, 20);
  buf.writeUInt16LE(1, 22);
  buf.writeUInt32LE(16000, 24);
  buf.writeUInt32LE(32000, 28);
  buf.writeUInt16LE(2, 32);
  buf.writeUInt16LE(16, 34);
  buf.write('data', 36);
  buf.writeUInt32LE(dataSize, 40);
  pcm.copy(buf, 44);
  return buf;
}

async function runOnce(buf, contentTypeHint, skipHttp) {
  const dgApiKey = process.env.DEEPGRAM_API_KEY;
  const t0 = Date.now();

  const head4 = buf.slice(0, 4).toString('ascii');
  const head3 = buf.slice(0, 3).toString('ascii');
  const b0 = buf.length > 0 ? buf[0] : 0;
  const b1 = buf.length > 1 ? buf[1] : 0;
  const isRiff = head4 === 'RIFF';
  const isMp3 = head3 === 'ID3' || (b0 === 0xff && (b1 & 0xe0) === 0xe0);
  const isWebm = (b0 === 0x1a && b1 === 0x45) ||
    (contentTypeHint && String(contentTypeHint).toLowerCase().indexOf('webm') !== -1);
  const isOgg = head4 === 'OggS' ||
    (contentTypeHint && String(contentTypeHint).toLowerCase().indexOf('ogg') !== -1);
  const t1 = Date.now();

  let audioBuffer = buf;
  let dgContentType = 'audio/wav';
  let didConvert = false;

  if (isWebm || isOgg || (!isRiff && !isMp3)) {
    const convertStart = Date.now();
    didConvert = true;
    try {
      const pcm = await audioToPcm16kMono(buf);
      if (pcm && pcm.length) {
        audioBuffer = createCompareWavBuffer(pcm);
        dgContentType = 'audio/wav';
      }
    } catch (convErr) {
      console.warn('[probe] ffmpeg convert failed:', convErr.message);
      if (isRiff) dgContentType = 'audio/wav';
      else if (isMp3) dgContentType = 'audio/mpeg';
      else if (isWebm) dgContentType = 'audio/webm';
      else if (isOgg) dgContentType = 'audio/ogg';
      else if (contentTypeHint) dgContentType = String(contentTypeHint).split(';')[0] || 'audio/wav';
    }
    void (Date.now() - convertStart);
  } else if (isRiff) {
    dgContentType = 'audio/wav';
  } else if (isMp3) {
    dgContentType = 'audio/mpeg';
  }
  const t2 = Date.now();

  // Large WAV → MP3 before upload (excluded from stt_ms). Skip if already MP3.
  let mp3ConvertMs = 0;
  const sendIsWav = dgContentType === 'audio/wav'
    && audioBuffer.length >= 12
    && audioBuffer.slice(0, 4).toString('ascii') === 'RIFF';
  if (sendIsWav && audioBuffer.length > 300000) {
    const mp3Start = Date.now();
    const originalLen = audioBuffer.length;
    try {
      const mp3Buf = await new Promise(function (resolve, reject) {
        const ffmpeg = spawn('ffmpeg', [
          '-hide_banner', '-loglevel', 'error',
          '-i', 'pipe:0',
          '-f', 'mp3',
          '-ar', '16000',
          '-ac', '1',
          '-b:a', '64k',
          'pipe:1',
        ]);
        const chunks = [];
        ffmpeg.stdout.on('data', function (d) { chunks.push(d); });
        let errText = '';
        ffmpeg.stderr.on('data', function (d) { errText += d.toString(); });
        ffmpeg.on('error', function (e) {
          reject(new Error('ffmpeg WAV->MP3 spawn failed: ' + e.message));
        });
        ffmpeg.on('close', function (code) {
          if (code !== 0 || chunks.length === 0) {
            return reject(new Error('ffmpeg WAV->MP3 failed: ' + (errText || ('exit ' + code)).slice(0, 200)));
          }
          resolve(Buffer.concat(chunks));
        });
        ffmpeg.stdin.write(audioBuffer);
        ffmpeg.stdin.end();
      });
      if (mp3Buf && mp3Buf.length && mp3Buf.length < originalLen) {
        audioBuffer = mp3Buf;
        dgContentType = 'audio/mpeg';
        mp3ConvertMs = Date.now() - mp3Start;
        console.log('[Compare Audio STT] WAV->MP3 compressed:',
          '(' + mp3ConvertMs + 'ms, excluded from stt_ms) |',
          'original:', originalLen, 'bytes ->', audioBuffer.length, 'bytes');
      } else {
        mp3ConvertMs = Date.now() - mp3Start;
      }
    } catch (mp3Err) {
      mp3ConvertMs = Date.now() - mp3Start;
      console.warn('[probe] WAV->MP3 failed, sending WAV:', mp3Err.message);
      dgContentType = 'audio/wav';
    }
  }

  let sttMs = null;
  let transcript = '';
  let durationSec = null;
  let durationExtractMs = 0;

  if (!skipHttp) {
    const sttStart = Date.now();
    const response = await axios.post(
      'https://api.deepgram.com/v1/listen?model=nova-3&language=en-US&punctuate=true&smart_format=true',
      audioBuffer,
      {
        headers: { Authorization: 'Token ' + dgApiKey, 'Content-Type': dgContentType },
        maxBodyLength: Infinity,
        maxContentLength: Infinity,
        transformRequest: [function (data) { return data; }],
        responseType: 'json',
        timeout: 60000,
        httpsAgent: deepgramHttpsAgent,
      }
    );
    sttMs = Date.now() - sttStart;
    const payload = response.data || {};
    const alt = ((payload.results || {}).channels || [])[0] || {};
    const best = (alt.alternatives || [])[0] || {};
    transcript = best.transcript || '';
    const durStart = Date.now();
    const metaDuration = payload.metadata && Number(payload.metadata.duration);
    if (Number.isFinite(metaDuration) && metaDuration > 0) durationSec = metaDuration;
    if (durationSec == null) {
      try { durationSec = await getAudioDurationSec(buf); } catch (e) { durationSec = null; }
    }
    durationExtractMs = Date.now() - durStart;
  }

  const totalWall = Date.now() - t0;
  const breakdown = {
    format_detect_ms: t1 - t0,
    pcm_convert_ms: t2 - t1,
    mp3_convert_ms: mp3ConvertMs,
    duration_extract_ms: durationExtractMs,
    deepgram_http_ms: sttMs,
    total_ms: totalWall,
    didConvert: didConvert,
    type: dgContentType,
    bytes_in: buf.length,
    bytes_sent: audioBuffer.length,
    duration_sec: durationSec,
    transcript: transcript,
  };

  console.log('[STT Breakdown]',
    'format_detect:', breakdown.format_detect_ms, 'ms',
    '| pcm_convert:', breakdown.pcm_convert_ms, 'ms' + (didConvert ? '' : ' (skipped)'),
    '| mp3_convert:', mp3ConvertMs, 'ms' + (mp3ConvertMs ? '' : ' (skipped)'),
    '| deepgram_http:', breakdown.deepgram_http_ms != null ? breakdown.deepgram_http_ms + ' ms' : 'SKIPPED',
    '| duration_extract:', breakdown.duration_extract_ms, 'ms (after HTTP, not in stt_ms)',
    '| TOTAL:', breakdown.total_ms, 'ms',
    '| bytes_in:', buf.length,
    '| bytes_sent:', audioBuffer.length,
    '| type:', dgContentType);

  return breakdown;
}

function avg(arr, key) {
  const vals = arr.map(function (r) { return r[key]; }).filter(function (v) { return v != null && Number.isFinite(v); });
  if (!vals.length) return null;
  return vals.reduce(function (a, b) { return a + b; }, 0) / vals.length;
}

function fmt(n) {
  if (n == null) return 'n/a';
  return n.toFixed(1);
}

async function main() {
  const dgApiKey = process.env.DEEPGRAM_API_KEY;
  const skipHttp = !dgApiKey || dgApiKey === 'your_deepgram_key';
  if (skipHttp) {
    console.warn('DEEPGRAM_API_KEY missing or placeholder — skipping Deepgram HTTP; timing format+duration only.');
  } else {
    console.log('DEEPGRAM_API_KEY present — running full breakdown including deepgram_http.');
  }

  const allResults = {};

  for (const entry of FILES) {
    const abs = path.resolve(entry.file);
    if (!fs.existsSync(abs)) {
      console.error('Missing file:', abs);
      continue;
    }
    const buf = fs.readFileSync(abs);
    console.log('\n=== ' + entry.label.toUpperCase() + ': ' + entry.file + ' (' + buf.length + ' bytes) ===');
    const runs = [];
    for (let i = 1; i <= RUNS; i++) {
      process.stdout.write('Run ' + i + '/' + RUNS + ' ... ');
      try {
        const r = await runOnce(buf, entry.hint, skipHttp);
        runs.push(r);
        if (r.transcript) console.log('  transcript:', JSON.stringify(r.transcript).slice(0, 80));
      } catch (e) {
        console.error('  FAILED:', e.message);
        runs.push({
          format_detect_ms: null,
          pcm_convert_ms: null,
          duration_extract_ms: null,
          deepgram_http_ms: null,
          total_ms: null,
          error: e.message,
        });
      }
    }
    allResults[entry.label] = runs;
  }

  console.log('\n========== AVERAGES (' + RUNS + ' runs each) ==========');
  console.log(
    pad('format', 8) +
    pad('format_detect', 16) +
    pad('pcm_convert', 14) +
    pad('mp3_convert', 14) +
    pad('deepgram_http', 16) +
    pad('duration_extract', 18) +
    pad('TOTAL', 12) +
    pad('bytes_sent', 12)
  );
  console.log('-'.repeat(110));

  for (const entry of FILES) {
    const runs = allResults[entry.label] || [];
    const ok = runs.filter(function (r) { return !r.error; });
    console.log(
      pad(entry.label, 8) +
      pad(fmt(avg(ok, 'format_detect_ms')) + ' ms', 16) +
      pad(fmt(avg(ok, 'pcm_convert_ms')) + ' ms', 14) +
      pad(fmt(avg(ok, 'mp3_convert_ms')) + ' ms', 14) +
      pad((avg(ok, 'deepgram_http_ms') != null ? fmt(avg(ok, 'deepgram_http_ms')) + ' ms' : 'n/a'), 16) +
      pad(fmt(avg(ok, 'duration_extract_ms')) + ' ms', 18) +
      pad(fmt(avg(ok, 'total_ms')) + ' ms', 12) +
      pad((avg(ok, 'bytes_sent') != null ? Math.round(avg(ok, 'bytes_sent')) : 'n/a'), 12)
    );
  }

  console.log('\nPer-run deepgram_http (ms):');
  for (const entry of FILES) {
    const runs = allResults[entry.label] || [];
    const vals = runs.map(function (r) {
      return r.deepgram_http_ms != null ? String(r.deepgram_http_ms) : (r.error ? 'ERR' : 'skip');
    });
    console.log('  ' + entry.label + ': ' + vals.join(', '));
  }

  console.log('\nPer-run transcripts (WAV quality check):');
  const wavRuns = allResults.wav || [];
  wavRuns.forEach(function (r, i) {
    console.log('  wav run ' + (i + 1) + ':', r.transcript ? JSON.stringify(r.transcript) : (r.error || 'n/a'));
  });
}

function pad(s, n) {
  s = String(s);
  if (s.length >= n) return s + ' ';
  return s + ' '.repeat(n - s.length);
}

main().catch(function (e) {
  console.error(e);
  process.exit(1);
});
