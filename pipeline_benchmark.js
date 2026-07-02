const fs = require('fs');
const path = require('path');
require('dotenv').config();

const Anthropic = require('@anthropic-ai/sdk');
const WebSocket = require('ws');
const axios = require('axios');

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

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

const TEST_TRANSCRIPTS = [
  { text: "I'm fine", category: "Net - Safe" },
  { text: "Yes", category: "Net - Safe (short)" },
  { text: "I need a human agent please", category: "Net - Human" },
  { text: "Running late, give me 15 minutes", category: "Net - Delayed" },
  { text: "I'm fine I'm safe. I broke my leg and it hurts too much I'm not good I'm okay", category: "Contradictory - Critical" },
  { text: "I'm fine but I need help", category: "Contradictory - Simple" },
  { text: "Yes I'm okay but something feels wrong", category: "Contradictory - Vague" },
  { text: "All good, actually wait no I'm not okay", category: "Contradictory - Retraction" },
  { text: "I think I'm fine but I'm a bit worried", category: "Contradictory - Hesitant" },
  { text: "I broke my leg I need help", category: "Distress - Injury" },
  { text: "I'm stuck and a bit scared", category: "Distress - Medium" },
  { text: "SOS help me please", category: "Distress - Emergency" },
  { text: "I'm okay just a little dizzy and my head hurts", category: "Distress - Subtle" },
  { text: "Everything is fine I just fell down but I'm okay I promise", category: "Distress - Minimizing" },
  { text: "almost done just a bit more time", category: "Delayed - Vague" },
  { text: "Give me like maybe 10 or 15 minutes I think", category: "Delayed - Vague Time" },
  { text: "I'm running behind but should be done soon", category: "Delayed - Vague" },
  { text: "fi... oka... fine", category: "Noisy - Broken" },
  { text: "I'm fi-- yes I'm good --static-- okay", category: "Noisy - Mixed" },
  { text: "I think I'm okay maybe", category: "Ambiguous - Hesitant" },
  { text: "ye", category: "Too Short" },
];

const RUNS = 3;

// Simulate Deepgram STT latency by measuring how fast it processes the text via WebSocket
async function measureSTT(text) {
  return new Promise((resolve) => {
    const dgApiKey = process.env.DEEPGRAM_API_KEY;
    const params = new URLSearchParams({
      model: 'nova-3', language: 'en-US',
      punctuate: 'true', smart_format: 'true'
    });
    const ws = new WebSocket('wss://api.deepgram.com/v1/listen?' + params.toString(), {
      headers: { Authorization: 'Token ' + dgApiKey }
    });

    const start = Date.now();
    let done = false;

    ws.on('open', () => {
      // Send text as raw audio simulation — use listen endpoint with prerecorded
      ws.close();
    });

    // Use prerecorded endpoint instead for realistic STT timing
    ws.on('close', () => {
      if (!done) { done = true; resolve(Date.now() - start); }
    });

    ws.on('error', () => {
      if (!done) { done = true; resolve(Date.now() - start); }
    });

    setTimeout(() => { if (!done) { done = true; resolve(null); } }, 5000);
  });
}

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
  return { latency, intent: parsed ? parsed.intent : 'ERROR', response: parsed ? parsed.response : '' };
}

const CACHE_MAP = {
  'SAFE_CONFIRMATION': 'safe_confirmation.mp3',
  'DELAYED_CHECKIN':  'delayed_checkin_15.mp3',
  'REQUEST_HUMAN':    'request_human.mp3',
  'UNRECOGNISED':     'unrecognised.mp3',
};

function measureTTSFromCache(intent) {
  const cacheFile = CACHE_MAP[intent] || 'safe_confirmation.mp3';
  const cachePath = path.join(__dirname, 'audio_cache', cacheFile);
  const start = Date.now();
  fs.readFileSync(cachePath);
  return Date.now() - start;
}

// Measure Deepgram via prerecorded API (most accurate way without real audio file)
async function measureDeepgramPing() {
  const dgApiKey = process.env.DEEPGRAM_API_KEY;
  const start = Date.now();
  try {
    await axios.get('https://api.deepgram.com/v1/auth/token', {
      headers: { Authorization: 'Token ' + dgApiKey }
    });
  } catch (e) {}
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
  console.log('\n=== CASCADED PIPELINE (Haiku 4.5) — FULL LATENCY BENCHMARK ===');
  console.log('Measuring: LLM + TTS (STT simulated via network ping)');
  console.log('Runs per transcript:', RUNS, '\n');

  // Measure Deepgram network round-trip as STT proxy
  console.log('Measuring Deepgram network latency (proxy for STT)...');
  const sttPings = [];
  for (let i = 0; i < 5; i++) {
    sttPings.push(await measureDeepgramPing());
    await new Promise(r => setTimeout(r, 300));
  }
  const sttProxy = avg(sttPings);
  console.log('Deepgram network proxy: ' + sttProxy + 'ms (avg of 5 pings)\n');

  const results = [];

  for (const t of TEST_TRANSCRIPTS) {
    process.stdout.write('Testing: "' + t.text + '"\n');
    const llmRuns = [], ttsRuns = [];

    for (let i = 0; i < RUNS; i++) {
      try {
        const llm = await measureLLM(t.text);
        llmRuns.push({ latency: llm.latency, intent: llm.intent, response: llm.response });
        const tts = measureTTSFromCache(llm.intent);
        ttsRuns.push(tts);
        console.log('  Run ' + (i + 1) + ': LLM=' + llm.latency + 'ms | TTS(cache)=' + tts + 'ms | Intent=' + llm.intent);
      } catch (e) {
        console.log('  Run ' + (i + 1) + ': ERROR — ' + e.message);
      }
      await new Promise(r => setTimeout(r, 500));
    }

    if (llmRuns.length > 0) {
      const avgLLM = filteredAvg(llmRuns.map(r => r.latency));
      const avgTTS = filteredAvg(ttsRuns);
      const avgTotal = sttProxy + avgLLM + avgTTS;
      const lastIntent = llmRuns[llmRuns.length - 1].intent;

      results.push({
        transcript: t.text,
        category: t.category,
        stt: sttProxy,
        llm: avgLLM,
        tts: avgTTS,
        total: avgTotal,
        intent: lastIntent
      });
      console.log('  → STT=' + sttProxy + 'ms | LLM=' + avgLLM + 'ms | TTS=' + avgTTS + 'ms | Total=' + avgTotal + 'ms\n');
    }
    await new Promise(r => setTimeout(r, 300));
  }

  console.log('\n========================================');
  console.log('SUMMARY TABLE');
  console.log('========================================\n');

  const pad = (s, n) => String(s).padEnd(n);
  console.log(pad('Transcript', 35) + '| ' + pad('Category', 22) + '| ' + pad('STT', 8) + '| ' + pad('LLM', 8) + '| ' + pad('TTS', 8) + '| ' + 'Total');
  console.log('-'.repeat(100));

  for (const r of results) {
    console.log(
      pad(r.transcript.substring(0, 33), 35) + '| ' +
      pad(r.category.substring(0, 20), 22) + '| ' +
      pad(r.stt + 'ms', 8) + '| ' +
      pad(r.llm + 'ms', 8) + '| ' +
      pad(r.tts + 'ms', 8) + '| ' +
      r.total + 'ms'
    );
  }

  const n = results.length;
  const avgSTT = sttProxy;
  const avgLLM = avg(results.map(r => r.llm));
  const avgTTS = avg(results.map(r => r.tts));
  const avgTotal = avg(results.map(r => r.total));

  console.log('\n========================================');
  console.log('OVERALL AVERAGES');
  console.log('========================================');
  console.log('STT (Deepgram proxy): ' + avgSTT + 'ms');
  console.log('LLM (Haiku 4.5):      ' + avgLLM + 'ms');
  console.log('TTS (cache read):     ' + avgTTS + 'ms');
  console.log('Total pipeline:       ' + avgTotal + 'ms');

  fs.writeFileSync('pipeline_latency_results.json', JSON.stringify({
    timestamp: new Date().toISOString(),
    model: 'claude-haiku-4-5',
    runs_per_transcript: RUNS,
    stt_proxy_ms: sttProxy,
    results,
    summary: { avgSTT, avgLLM, avgTTS, avgTotal }
  }, null, 2));
  console.log('\nSaved to pipeline_latency_results.json');
}

run().catch(console.error);
