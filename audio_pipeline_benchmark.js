const fs = require('fs');
const path = require('path');
require('dotenv').config();

const Anthropic = require('@anthropic-ai/sdk');
const WebSocket = require('ws');
const axios = require('axios');

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

const UPLOAD_DIR = 'C:\\Users\\meyilmaz\\OneDrive - Orion Systems Integrators, LLC\\Documents\\voice_ai\\uploads';

const SYSTEM_PROMPT = `Aware360 safety system. Worker missed check-in. Classify their response AND detect distress signals.

Return ONLY this JSON: {"intent":"<SAFE_CONFIRMATION|DELAYED_CHECKIN|REQUEST_HUMAN|UNRECOGNISED>","confidence":<0.0-1.0>,"response":"<reply>","resolved":<bool>,"escalate":<bool>,"delay_minutes":<null|number>,"distress":<null|"low"|"medium"|"high">,"distress_signals":<null|string>}

SAFE_CONFIRMATION: clear safety confirmation -> resolved:true, response:"Got it — I've reset your safety timer. Stay safe."
DELAYED_CHECKIN: needs more time -> resolved:true, delay_minutes:number or 15, response:"Understood. I've extended your check-in timer by [X] minutes. Please check in when ready."
REQUEST_HUMAN: wants human/help/SOS -> escalate:true, response:"Of course. I'm flagging this for a live agent — someone will call you back shortly. Keep your phone available."
UNRECOGNISED: unclear/ambiguous -> resolved:false, escalate:false, response:"I'm sorry, I didn't catch that. Are you safe, need more time, or need a human agent?"
Confidence: >0.90 clear | <0.60 hesitation/unclear -> UNRECOGNISED. When in doubt, ask again.

CRITICAL SAFETY RULE — OVERRIDE EVERYTHING ELSE:
If the worker's message contains ANY of these physical indicators, ALWAYS return REQUEST_HUMAN:
- fell, fall, fallen, tripped, slipped
- hurts, pain, ache, sore, burning
- broke, broken, cut, bleeding, injured, hurt
- dizzy, faint
- stuck, trapped
- scared, afraid, worried`;

const TEST_FILES = [
  {
    file: UPLOAD_DIR + '/gemini_haiku_comparison1.mp3',
    transcript: 'fi... oka... fine',
    category: 'Noisy - Broken',
    expected: 'SAFE_CONFIRMATION'
  },
  {
    file: UPLOAD_DIR + '/gemini_haiku_comparison2.mp3',
    transcript: "I think I'm fine but I'm a bit worried",
    category: 'Contradictory - Hesitant',
    expected: 'REQUEST_HUMAN'
  },
  {
    file: UPLOAD_DIR + '/gemini_haiku_comparison3.mp3',
    transcript: "I'm fine I'm safe. I broke my leg and it hurts too much I'm not good I'm okay",
    category: 'Contradictory - Critical',
    expected: 'REQUEST_HUMAN'
  },
  {
    file: UPLOAD_DIR + '/gemini_haiku_comparison4.mp3',
    transcript: "Everything is fine I just fell down but I'm okay I promise",
    category: 'Distress - Minimizing',
    expected: 'REQUEST_HUMAN'
  },
];

const RUNS = 3;

const CACHE_MAP = {
  'SAFE_CONFIRMATION': 'safe_confirmation.mp3',
  'DELAYED_CHECKIN':   'delayed_checkin_15.mp3',
  'REQUEST_HUMAN':     'request_human.mp3',
  'UNRECOGNISED':      'unrecognised.mp3',
};

// Convert MP3 to PCM via ffmpeg
async function convertMp3ToPcm(mp3Path) {
  return new Promise((resolve, reject) => {
    if (!fs.existsSync(mp3Path)) return reject(new Error('File not found: ' + mp3Path));
    const { spawn } = require('child_process');
    const pcmPath = mp3Path.replace('.mp3', '_tmp.pcm');
    const ffmpeg = spawn('ffmpeg.exe', [
      '-y', '-i', mp3Path,
      '-ar', '16000', '-ac', '1', '-f', 's16le', pcmPath
    ], { stdio: ['pipe', 'pipe', 'pipe'] });
    ffmpeg.on('close', (code) => {
      if (code === 0 && fs.existsSync(pcmPath)) resolve(fs.readFileSync(pcmPath));
      else reject(new Error('ffmpeg failed'));
    });
    ffmpeg.on('error', reject);
  });
}

// Send PCM audio to Deepgram, get transcript + latency
async function measureSTT(audioBuffer) {
  return new Promise((resolve, reject) => {
    const dgApiKey = process.env.DEEPGRAM_API_KEY;
    const params = new URLSearchParams({
      model: 'nova-3',
      language: 'en-US',
      punctuate: 'true',
      smart_format: 'true',
      encoding: 'linear16',
      sample_rate: '16000',
    });

    const ws = new WebSocket('wss://api.deepgram.com/v1/listen?' + params.toString(), {
      headers: { Authorization: 'Token ' + dgApiKey }
    });

    const start = Date.now();
    let transcript = '';
    let done = false;

    const timeout = setTimeout(() => {
      if (!done) { done = true; ws.close(); reject(new Error('Deepgram timeout')); }
    }, 15000);

    ws.on('open', () => {
      // Send audio in chunks (Deepgram streaming)
      const CHUNK_SIZE = 4096;
      for (let i = 0; i < audioBuffer.length; i += CHUNK_SIZE) {
        ws.send(audioBuffer.slice(i, i + CHUNK_SIZE));
      }
      // Signal end of stream
      ws.send(JSON.stringify({ type: 'CloseStream' }));
    });

    ws.on('message', (data) => {
      try {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'Results' && msg.channel && msg.channel.alternatives) {
          const alt = msg.channel.alternatives[0];
          if (alt && alt.transcript && msg.is_final) {
            transcript += alt.transcript + ' ';
          }
        }
        if (msg.type === 'Metadata' || (msg.type === 'Results' && msg.speech_final)) {
          if (!done && transcript.trim()) {
            done = true;
            clearTimeout(timeout);
            ws.close();
            resolve({ latency: Date.now() - start, transcript: transcript.trim() });
          }
        }
      } catch (e) {}
    });

    ws.on('close', () => {
      if (!done) {
        done = true;
        clearTimeout(timeout);
        resolve({ latency: Date.now() - start, transcript: transcript.trim() || '[no transcript]' });
      }
    });

    ws.on('error', (e) => {
      if (!done) { done = true; clearTimeout(timeout); reject(e); }
    });
  });
}

// Send transcript to Haiku, get intent + latency
async function measureLLM(transcript) {
  const start = Date.now();
  const stream = anthropic.messages.stream({
    model: 'claude-haiku-4-5',
    max_tokens: 200,
    system: SYSTEM_PROMPT,
    messages: [{ role: 'user', content: 'Worker said: "' + transcript + '"' }]
  });
  let fullText = '';
  for await (const chunk of stream) {
    if (chunk.type === 'content_block_delta' && chunk.delta && chunk.delta.type === 'text_delta') {
      fullText += chunk.delta.text;
    }
  }
  const latency = Date.now() - start;
  let parsed = null;
  try { parsed = JSON.parse(fullText.trim()); } catch (e) {
    const match = fullText.match(/\{[\s\S]*\}/);
    if (match) try { parsed = JSON.parse(match[0]); } catch (e2) {}
  }
  return {
    latency,
    intent: parsed ? parsed.intent : 'ERROR',
    confidence: parsed ? parsed.confidence : 0,
    response: parsed ? parsed.response : ''
  };
}

// Read TTS response from cache
function measureTTSFromCache(intent) {
  const cacheFile = CACHE_MAP[intent] || 'safe_confirmation.mp3';
  const cachePath = path.join(__dirname, 'audio_cache', cacheFile);
  const start = Date.now();
  try { fs.readFileSync(cachePath); } catch (e) {}
  return Date.now() - start;
}

function avg(arr) { return arr.length ? Math.round(arr.reduce((a, b) => a + b, 0) / arr.length) : 0; }

function filteredAvg(arr) {
  if (arr.length <= 2) return avg(arr);
  const sorted = [...arr].sort((a, b) => a - b);
  const q1 = sorted[Math.floor(sorted.length * 0.25)];
  const q3 = sorted[Math.floor(sorted.length * 0.75)];
  const iqr = q3 - q1;
  const lower = q1 - 1.5 * iqr;
  const upper = q3 + 1.5 * iqr;
  const filtered = sorted.filter(v => v >= lower && v <= upper);
  return avg(filtered.length ? filtered : arr);
}

async function run() {
  console.log('\n=== AUDIO FILE PIPELINE BENCHMARK (Haiku 4.5) ===');
  console.log('Pipeline: Real Audio → Deepgram STT → Haiku LLM → Cache TTS');
  console.log('Runs per file:', RUNS, '\n');

  const results = [];

  for (const t of TEST_FILES) {
    console.log('Testing: "' + t.transcript + '"');
    console.log('Category: ' + t.category + ' | Expected: ' + t.expected);

    // Convert MP3 to PCM once
    let audioBuffer;
    try {
      audioBuffer = await convertMp3ToPcm(t.file);
      console.log('Audio: ' + (audioBuffer.length / 16000 / 2).toFixed(2) + 's');
    } catch (e) {
      console.log('ERROR converting audio: ' + e.message + '\n');
      continue;
    }

    const sttRuns = [], llmRuns = [], ttsRuns = [];

    for (let i = 0; i < RUNS; i++) {
      try {
        // STT
        const stt = await measureSTT(audioBuffer);
        sttRuns.push(stt.latency);

        // LLM
        const llm = await measureLLM(stt.transcript);
        llmRuns.push({ latency: llm.latency, intent: llm.intent, confidence: llm.confidence });

        // TTS (cache)
        const tts = measureTTSFromCache(llm.intent);
        ttsRuns.push(tts);

        const correct = llm.intent === t.expected ? '✓' : '✗';
        console.log(
          '  Run ' + (i + 1) + ': STT=' + stt.latency + 'ms | LLM=' + llm.latency + 'ms | TTS=' + tts + 'ms' +
          ' | Transcript="' + stt.transcript + '"' +
          ' | Intent=' + llm.intent + ' (' + Math.round(llm.confidence * 100) + '%) ' + correct
        );
      } catch (e) {
        console.log('  Run ' + (i + 1) + ': ERROR — ' + e.message);
      }
      await new Promise(r => setTimeout(r, 500));
    }

    if (llmRuns.length > 0) {
      const avgSTT = filteredAvg(sttRuns);
      const avgLLM = filteredAvg(llmRuns.map(r => r.latency));
      const avgTTS = filteredAvg(ttsRuns);
      const avgTotal = avgSTT + avgLLM + avgTTS;
      const lastIntent = llmRuns[llmRuns.length - 1].intent;
      const correct = lastIntent === t.expected;

      results.push({
        transcript: t.transcript,
        category: t.category,
        expected: t.expected,
        stt: avgSTT,
        llm: avgLLM,
        tts: avgTTS,
        total: avgTotal,
        intent: lastIntent,
        correct
      });

      console.log(
        '  → STT=' + avgSTT + 'ms | LLM=' + avgLLM + 'ms | TTS=' + avgTTS + 'ms | Total=' + avgTotal + 'ms\n'
      );
    }

    await new Promise(r => setTimeout(r, 500));
  }

  // Summary
  console.log('\n========================================');
  console.log('SUMMARY TABLE');
  console.log('========================================\n');

  const pad = (s, n) => String(s).padEnd(n);
  console.log(
    pad('Transcript', 35) + '| ' +
    pad('Category', 22) + '| ' +
    pad('Expected', 18) + '| ' +
    pad('Intent', 18) + '| ' +
    pad('STT', 8) + '| ' +
    pad('LLM', 8) + '| ' +
    pad('TTS', 8) + '| Total'
  );
  console.log('-'.repeat(130));

  let correct = 0;
  for (const r of results) {
    if (r.correct) correct++;
    console.log(
      pad(r.transcript.substring(0, 33), 35) + '| ' +
      pad(r.category.substring(0, 20), 22) + '| ' +
      pad(r.expected, 18) + '| ' +
      pad(r.intent + (r.correct ? ' ✓' : ' ✗'), 18) + '| ' +
      pad(r.stt + 'ms', 8) + '| ' +
      pad(r.llm + 'ms', 8) + '| ' +
      pad(r.tts + 'ms', 8) + '| ' +
      r.total + 'ms'
    );
  }

  const n = results.length;
  console.log('\n========================================');
  console.log('OVERALL AVERAGES');
  console.log('========================================');
  console.log('Accuracy:        ' + correct + '/' + n + ' (' + Math.round(correct / n * 100) + '%)');
  console.log('STT (Deepgram):  ' + avg(results.map(r => r.stt)) + 'ms');
  console.log('LLM (Haiku 4.5): ' + avg(results.map(r => r.llm)) + 'ms');
  console.log('TTS (cache):     ' + avg(results.map(r => r.tts)) + 'ms');
  console.log('Total pipeline:  ' + avg(results.map(r => r.total)) + 'ms');

  fs.writeFileSync('audio_pipeline_results.json', JSON.stringify({
    timestamp: new Date().toISOString(),
    model: 'claude-haiku-4-5',
    runs_per_file: RUNS,
    results,
    summary: {
      accuracy: Math.round(correct / n * 100),
      avgSTT: avg(results.map(r => r.stt)),
      avgLLM: avg(results.map(r => r.llm)),
      avgTTS: avg(results.map(r => r.tts)),
      avgTotal: avg(results.map(r => r.total))
    }
  }, null, 2));

  console.log('\nSaved to audio_pipeline_results.json');
}

run().catch(console.error);
