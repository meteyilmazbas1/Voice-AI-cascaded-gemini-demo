const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const WebSocket = require('ws');
require('dotenv').config();

const MODEL = process.env.GEMINI_MODEL || 'gemini-3.1-flash-live-preview';
const RUNS = Number(process.env.RUNS || 3);
const SAMPLE_RATE = 16000;
const CRITICAL_WORDS = [
  'help', 'hurt', 'hurts', 'pain', 'fell', 'fall', 'fallen', 'broke', 'broken',
  'dizzy', 'stuck', 'trapped', 'scared', 'afraid', 'worried', 'bleeding', 'injured'
];

function expectedFromFilename(filename) {
  const match = filename.match(/\((.*)\)\.[^.]+$/);
  return match ? match[1] : null;
}

function normalize(text) {
  return String(text || '')
    .toLowerCase()
    .replace(/[^a-z0-9' ]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function editDistance(a, b) {
  const row = Array.from({ length: b.length + 1 }, (_, i) => i);
  for (let i = 1; i <= a.length; i++) {
    let previous = row[0];
    row[0] = i;
    for (let j = 1; j <= b.length; j++) {
      const saved = row[j];
      row[j] = Math.min(row[j] + 1, row[j - 1] + 1, previous + (a[i - 1] === b[j - 1] ? 0 : 1));
      previous = saved;
    }
  }
  return row[b.length];
}

function wordErrorRate(expected, actual) {
  const reference = normalize(expected).split(' ').filter(Boolean);
  const hypothesis = normalize(actual).split(' ').filter(Boolean);
  return reference.length ? editDistance(reference, hypothesis) / reference.length : 0;
}

function convertToPcm(file) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    const errors = [];
    const ffmpeg = spawn('ffmpeg', ['-v', 'error', '-i', file, '-f', 's16le', '-acodec', 'pcm_s16le', '-ac', '1', '-ar', String(SAMPLE_RATE), 'pipe:1']);
    ffmpeg.stdout.on('data', chunk => chunks.push(chunk));
    ffmpeg.stderr.on('data', chunk => errors.push(chunk));
    ffmpeg.on('error', reject);
    ffmpeg.on('close', code => code === 0
      ? resolve(Buffer.concat(chunks))
      : reject(new Error(Buffer.concat(errors).toString() || `ffmpeg exited with ${code}`)));
  });
}

function transcribeWithGemini(pcm) {
  return new Promise((resolve, reject) => {
    const key = process.env.GEMINI_API_KEY;
    if (!key) return reject(new Error('GEMINI_API_KEY is missing'));

    const url = `wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=${key}`;
    const ws = new WebSocket(url);
    const started = Date.now();
    let transcript = '';
    let finished = false;
    const timeout = setTimeout(() => finish(new Error('Gemini timed out')), 20000);

    function finish(error) {
      if (finished) return;
      finished = true;
      clearTimeout(timeout);
      try { ws.close(); } catch (_) {}
      if (error) reject(error);
      else resolve({ transcript: transcript.trim(), latencyMs: Date.now() - started });
    }

    ws.on('open', () => {
      ws.send(JSON.stringify({
        setup: {
          model: `models/${MODEL}`,
          generation_config: { response_modalities: ['AUDIO'] },
          realtime_input_config: { automatic_activity_detection: { disabled: true } },
          input_audio_transcription: {},
          system_instruction: { parts: [{ text: 'The user speaks English. Listen carefully and answer briefly.' }] }
        }
      }));
    });

    ws.on('message', data => {
      let msg;
      try { msg = JSON.parse(data.toString()); } catch (_) { return; }

      if (msg.setupComplete) {
        const bytesPer100ms = SAMPLE_RATE * 2 / 10;
        let offset = 0;
        ws.send(JSON.stringify({ realtime_input: { activity_start: {} } }));
        const sendNext = () => {
          if (finished || ws.readyState !== WebSocket.OPEN) return;
          if (offset < pcm.length) {
            const chunk = pcm.subarray(offset, Math.min(offset + bytesPer100ms, pcm.length));
            offset += chunk.length;
            ws.send(JSON.stringify({ realtime_input: { audio: { data: chunk.toString('base64'), mimeType: `audio/pcm;rate=${SAMPLE_RATE}` } } }));
            setTimeout(sendNext, 100);
          } else {
            // Let automatic VAD observe a short silence before ending the stream.
            const silence = Buffer.alloc(bytesPer100ms);
            let silenceChunks = 6;
            const sendSilence = () => {
              if (silenceChunks-- > 0) {
                ws.send(JSON.stringify({ realtime_input: { audio: { data: silence.toString('base64'), mimeType: `audio/pcm;rate=${SAMPLE_RATE}` } } }));
                setTimeout(sendSilence, 100);
              } else {
                ws.send(JSON.stringify({ realtime_input: { activity_end: {} } }));
                ws.send(JSON.stringify({ realtime_input: { audio_stream_end: true } }));
              }
            };
            sendSilence();
          }
        };
        sendNext();
      }

      const input = msg.serverContent && msg.serverContent.inputTranscription;
      if (input && input.text) transcript += input.text;
      if (msg.serverContent && msg.serverContent.turnComplete) finish();
    });

    ws.on('error', finish);
    ws.on('close', () => {
      if (!finished && transcript) finish();
    });
  });
}

function criticalRecall(expected, actual) {
  const expectedWords = new Set(normalize(expected).split(' '));
  const actualWords = new Set(normalize(actual).split(' '));
  const relevant = CRITICAL_WORDS.filter(word => expectedWords.has(word));
  const found = relevant.filter(word => actualWords.has(word));
  return { expected: relevant, found, missed: relevant.filter(word => !actualWords.has(word)) };
}

async function main() {
  const files = fs.readdirSync(__dirname)
    .filter(name => /^gemini_haiku_compari(on|son).*\.(mp3|wav|m4a|aac|ogg|flac|webm)$/i.test(name))
    .map(name => ({ name, file: path.join(__dirname, name), expected: expectedFromFilename(name) }))
    .filter(item => item.expected);

  if (!files.length) throw new Error('No gemini_haiku_comparison(...) audio files found');
  const results = [];

  console.log(`\nGemini noisy-audio test: ${MODEL} (${RUNS} run(s) per file)\n`);
  for (const item of files) {
    const pcm = await convertToPcm(item.file);
    console.log(`Expected: ${item.expected}`);
    const runs = [];
    for (let run = 1; run <= RUNS; run++) {
      try {
        const result = await transcribeWithGemini(pcm);
        const wer = wordErrorRate(item.expected, result.transcript);
        const safety = criticalRecall(item.expected, result.transcript);
        runs.push({ ...result, wer, criticalWords: safety });
        console.log(`  ${run}. WER ${(wer * 100).toFixed(1)}% | ${result.latencyMs}ms | "${result.transcript}"`);
        if (safety.missed.length) console.log(`     MISSED CRITICAL: ${safety.missed.join(', ')}`);
      } catch (error) {
        runs.push({ error: error.message });
        console.log(`  ${run}. ERROR: ${error.message}`);
      }
    }
    results.push({ file: item.name, expected: item.expected, runs });
    console.log('');
  }

  const valid = results.flatMap(item => item.runs).filter(run => !run.error);
  const avgWer = valid.length ? valid.reduce((sum, run) => sum + run.wer, 0) / valid.length : null;
  const criticalExpected = valid.reduce((sum, run) => sum + run.criticalWords.expected.length, 0);
  const criticalFound = valid.reduce((sum, run) => sum + run.criticalWords.found.length, 0);
  const output = {
    timestamp: new Date().toISOString(), model: MODEL, runsPerFile: RUNS,
    summary: {
      successfulRuns: valid.length,
      averageWer: avgWer,
      criticalWordRecall: criticalExpected ? criticalFound / criticalExpected : null
    },
    results
  };
  fs.writeFileSync(path.join(__dirname, 'gemini_noisy_audio_results.json'), JSON.stringify(output, null, 2));
  console.log(`Average WER: ${avgWer === null ? 'n/a' : (avgWer * 100).toFixed(1) + '%'}`);
  console.log(`Critical-word recall: ${criticalExpected ? (criticalFound / criticalExpected * 100).toFixed(1) + '%' : 'n/a'}`);
  console.log('Saved: gemini_noisy_audio_results.json');
}

main().catch(error => { console.error(error); process.exitCode = 1; });
