require('dotenv').config();

const fs = require('fs');
const path = require('path');

const express = require('express');
const WebSocket = require('ws');
const http = require('http');
const axios = require('axios');
const Anthropic = require('@anthropic-ai/sdk');
const twilio = require('twilio');
const { spawn } = require('child_process');

const app = express();
const server = http.createServer(app);

// Two separate WebSocket servers on different paths
const browserWss = new WebSocket.Server({ noServer: true });
const twilioWss = new WebSocket.Server({ noServer: true });

// Route WebSocket upgrades by path
server.on('upgrade', function(request, socket, head) {
  const pathname = request.url;
  if (pathname === '/twilio') {
    twilioWss.handleUpgrade(request, socket, head, function(ws) {
      twilioWss.emit('connection', ws, request);
    });
  } else {
    browserWss.handleUpgrade(request, socket, head, function(ws) {
      browserWss.emit('connection', ws, request);
    });
  }
});

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: false }));
app.use(express.static(path.join(__dirname, 'public')));

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
const { listCalls, getCallBySid, insertCallStart, updateCallEnd, updateCallLatency, updateRecordingUrl } = require('./db');
const { createDeepFilter, preloadDeepFilter, isDeepFilterReady } = require('./deepFilterWrapper');
const { createNoiseEstimator } = require("./noiseEstimator");
const { createSileroVad } = require("./sileroVad");
const { frameStatsMulaw, frameStatsPcm16 } = require("./audioLevel");
const { off } = require('cluster');

// TEMP: DeepFilter bypass — mic PCM goes straight to Deepgram.
// Re-enable with DEEPFILTER_ENABLED=true in .env (or flip default below).
const DEEPFILTER_ENABLED = process.env.DEEPFILTER_ENABLED === 'true';

const CACHE_DIR = path.join(__dirname, 'audio_cache');
function loadAudioCache() {
  const cache = {};
  if (!fs.existsSync(CACHE_DIR)) return cache;
  fs.readdirSync(CACHE_DIR).forEach(file => {
    if (file.endsWith('.mp3')) {
      const key = file.replace('.mp3', '');
      cache[key] = fs.readFileSync(path.join(CACHE_DIR, file)).toString('base64');
      console.log('[Cache] Loaded: ' + key);
    }
  });
  return cache;
}
const audioCache = loadAudioCache();

// Mulaw cache — MP3'leri server başlarken önceden dönüştür
const mulawCache = {};

async function loadMulawCache() {
  if (!fs.existsSync(CACHE_DIR)) return;
  const files = fs.readdirSync(CACHE_DIR).filter(f => f.endsWith('.mp3'));
  console.log('[Mulaw Cache] Converting', files.length, 'MP3 files...');
  for (const file of files) {
    const key = file.replace('.mp3', '');
    try {
      const mp3Buffer = fs.readFileSync(path.join(CACHE_DIR, file));
      const mulawBuffer = await mp3ToMulaw(mp3Buffer);
      mulawCache[key] = mulawBuffer;
      console.log('[Mulaw Cache] Loaded:', key, '(' + mulawBuffer.length + ' bytes)');
    } catch (e) {
      console.error('[Mulaw Cache] Failed to convert:', key, e.message);
    }
  }
  console.log('[Mulaw Cache] Ready —', Object.keys(mulawCache).length, 'files loaded');
}

const COMPARE_RESPONSES = {
  safe: "Got it — I've reset your safety timer. Stay safe.",
  delayed: "Understood. I've extended your check-in timer. Please check in when ready.",
  request_human: "Of course. I'm flagging this for a live agent — someone will call you back shortly.",
  unsafe: "Of course. I'm flagging this for a live agent — someone will call you back shortly.",
  ambiguous: "I'm sorry, I didn't catch that. Are you safe, need more time, or need a human agent?",
};

const COMPARE_INTENT_PROMPT = `Classify the worker's response to a safety check-in call. Reply with ONLY valid JSON, nothing else:
{"intent":"<safe|delayed|request_human|unsafe|ambiguous>","confidence":<0.0-1.0>,"distress":<null|"low"|"medium"|"high">}

safe: clear safety confirmation ("I'm fine", "yes", "all good")
delayed: needs more time ("give me 10 minutes", "running late")
request_human: wants human agent ("get me someone", "human please")
unsafe: injury/distress/danger ("I fell", "I'm hurt", "SOS") — always escalate even if worker also claims to be safe
ambiguous: unclear, contradictory, or unrecognised

Confidence: >0.90 clear | <0.60 hesitation/unclear.`;

// Live compare: same intent-only prompt as Claude compare (no language/persona fluff)
const COMPARE_LIVE_PROMPT = COMPARE_INTENT_PROMPT;

const WORKER_INTRO = `Hello, this is the Aware360 Monitoring Center. We noticed you missed your scheduled check-in. Are you okay? Please say "I'm safe", "I need more time", or "connect me to an agent".`;

const INTENT_CACHE_MAP = {
  'SAFE_CONFIRMATION': 'safe_confirmation',
  'DELAYED_CHECKIN': 'delayed_checkin_15',
  'REQUEST_HUMAN': 'request_human',
  'UNRECOGNISED': 'unrecognised',
};

const CACHE_RESPONSES = {
  safe_confirmation: "Got it — I've reset your safety timer. Stay safe.",
  delayed_checkin_15: "Understood. I've extended your check-in timer. Please check in when ready.",
  request_human: "Of course. I'm flagging this for a live agent — someone will call you back shortly. Keep your phone available.",
  unrecognised: "I'm sorry, I didn't catch that. Are you safe, need more time, or need a human agent?",
};

const INTENT_PROMPT = `You are an intent classifier for a Voice AI Safety Assistant on the Aware360 platform. A field worker triggered a missed check-in alert. Read the worker's message and classify it into exactly one intent.

Intents:
- safe = worker says they are safe/fine/ok, with NO mention of injury, pain, or danger
- delayed = running late, needs more time, give me X minutes
- request_human = explicitly wants a live person/agent/human
- unsafe = ANY mention of injury, pain, hurt, bleeding, trapped, stuck, danger, fell/fall, accident, needs help, SOS — even if the worker also says they are "okay" or "fine"
- ambiguous = silent, unclear, off-topic, or cannot tell

PRIORITY: If the message contains ANY unsafe signal (injury, pain, danger, "help", "SOS"), classify as unsafe — even if the worker also says "I'm safe". request_human beats everything except unsafe.

Respond with ONLY one word, nothing else, no punctuation:
safe OR delayed OR request_human OR unsafe OR ambiguous`;

const MASKED_WORD_PROMPT = `You are a transcript-trust auditor for a Voice AI Safety Assistant. A field worker triggered a safety alert and is answering a safety check call. Noise bursts overlapped parts of the worker's utterance, so parts of the transcript may be misheard or missing.

You will receive:
- Agent question: what the assistant last asked the worker.
- Transcript: the utterance. [word] marks a word the noise overlapped (may be misheard). [NOISE 0.8s] marks lost audio — between words, or at the very start/end of the utterance (a trailing [NOISE] right after an unfinished clause often hides the words that decide the classification).
- Masked items: the overlapped words and missing spots.

The call outcome is decided by classifying the utterance as one of: safe, delayed, request_human, unsafe, ambiguous.

Decide: does the masked or missing content genuinely block the classification, OR does it hide a concrete detail the system needs to act on it (e.g. the NUMBER of minutes for delayed — the safety timer cannot be extended without it)?
- For each [word], imagine realistic alternatives (similar-sounding words: "stepped"/"slipped", "fine"/"fell"; negations: "can"/"can't").
- For each [NOISE], imagine realistic words that could fill the gap, given the agent's question.
- Weigh each alternative against the INTACT words: an alternative only counts if the whole utterance stays coherent with it. If the intact words already form a complete, consistent account that fits exactly one classification, the meaning is recovered — reply "unimportant".
- Reply "important" if a coherent reading of the full utterance changes the classification — especially toward unsafe — or if the masked content hides a required actionable detail, or if so much is masked that the overall meaning is not recoverable.

Tie-break: if after weighing coherence you still cannot tell, reply "important" — but a filling that contradicts the intact context is not genuine uncertainty.

Respond with exactly one word, nothing else: important or unimportant`;

async function classifyMaskedWordImportance(transcript, maskedWords, agentQuestion) {
  const response = await anthropic.messages.create({
    model: 'claude-haiku-4-5',
    max_tokens: 4,
    temperature: 0,
    system: [{ type: 'text', text: MASKED_WORD_PROMPT, cache_control: { type: 'ephemeral' } }],
    messages: [{ role: 'user', content: 'Agent question: "' + (agentQuestion || '(unknown)') + '"\nTranscript: "' + transcript + '"\nMasked items: ' + maskedWords.join(', ') }],
  }, { timeout: 2500, maxRetries: 0 });
  const raw = ((response.content[0] && response.content[0].text) || '').trim().toLowerCase();
  return { important: raw.startsWith('important') };
}

const VALID_INTENTS_SET = new Set(['safe', 'delayed', 'request_human', 'unsafe', 'ambiguous']);
const DURATION_REGEX = /(\d+)\s*(min(?:ute)?s?|hours?|hrs?|h)\b/i;

const DEEPGRAM_BASE_PARAMS = {
  model: 'nova-3', language: 'en-US', punctuate: 'true',
  smart_format: 'true', interim_results: 'true',
  utterance_end_ms: '1000', vad_events: 'true',
  endpointing: '800',  // 800ms silence before speech_final (avoids mid-sentence cuts)
};

const DEEPGRAM_KEYTERMS = '&keyterm=help&keyterm=SOS&keyterm=emergency&keyterm=hurt&keyterm=pain&keyterm=injured&keyterm=injury&keyterm=stuck&keyterm=trapped&keyterm=dizzy&keyterm=bleeding&keyterm=fallen&keyterm=fell&keyterm=scared&keyterm=afraid&keyterm=agent&keyterm=human&keyterm=danger&keyterm=accident&keyterm=not&keyterm=no&keyterm=don%27t&keyterm=can%27t&keyterm=unsafe';

function fastPathIntent(transcript) {
  const t = transcript.toLowerCase().trim();
  if (/\b(hurt|pain|fell|fall|broke|bleeding|dizzy|injured|stuck|trapped|help|sos|emergency|worried|scared|afraid|nervous)\b/.test(t)) return null;
  if (/^(yes|yeah|yep|yup|i'?m\s+(fine|safe|good|okay|ok)|fine|safe|all\s+good|i'?m\s+ok)\.?$/i.test(t)) {
    return { intent: 'SAFE_CONFIRMATION', confidence: 0.95, response: CACHE_RESPONSES.safe_confirmation, resolved: true, escalate: false, delay_minutes: null, distress: null, distress_signals: null };
  }
  if (/\b(connect|agent|human|representative|operator)\b/i.test(t)) {
    return { intent: 'REQUEST_HUMAN', confidence: 0.9, response: CACHE_RESPONSES.request_human, resolved: false, escalate: true, delay_minutes: null, distress: null, distress_signals: null };
  }
  if (/\b(more\s+time|need\s+time|extend|delay|minute|minutes|later)\b/i.test(t)) {
    return { intent: 'DELAYED_CHECKIN', confidence: 0.9, response: CACHE_RESPONSES.delayed_checkin_15, resolved: true, escalate: false, delay_minutes: 15, distress: null, distress_signals: null };
  }
  return null;
}

function applyCachedResponse(result) {
  const cacheKey = INTENT_CACHE_MAP[result.intent];
  if (cacheKey && CACHE_RESPONSES[cacheKey]) {
    result.response = CACHE_RESPONSES[cacheKey];
  }
  return result;
}

const PUBLIC_URL = process.env.PUBLIC_URL;
if (!PUBLIC_URL) throw new Error('PUBLIC_URL is not set in .env');
const WS_URL = PUBLIC_URL.replace('https://', 'wss://').replace('http://', 'ws://');

function buildTwilioWebhookUrl(req, pathname) {
  // Prefer NGROK_HOST if set; otherwise PUBLIC_URL (full https://… base used by make_call.js)
  let base;
  if (process.env.NGROK_HOST) {
    base = 'https://' + String(process.env.NGROK_HOST).replace(/^https?:\/\//, '');
  } else {
    base = PUBLIC_URL;
  }
  base = base.replace(/\/$/, '');
  const qs = req.originalUrl && req.originalUrl.indexOf('?') !== -1
    ? req.originalUrl.slice(req.originalUrl.indexOf('?'))
    : '';
  return base + pathname + qs;
}

function validateTwilioSignature(req, res, url) {
  const authToken = process.env.TWILIO_AUTH_TOKEN;
  if (!authToken) {
    console.warn('[Twilio] TWILIO_AUTH_TOKEN not set — rejecting request');
    res.status(403).send('Forbidden');
    return false;
  }
  const signature = req.headers['x-twilio-signature'];
  if (!signature) {
    console.warn('[Twilio] Missing signature — rejecting request');
    res.status(403).send('Forbidden');
    return false;
  }
  const params = req.body && typeof req.body === 'object' ? req.body : {};
  const valid = twilio.validateRequest(authToken, signature, url, params);
  if (!valid) {
    console.warn('[Twilio] Invalid signature — rejecting request from', req.ip);
    res.status(403).send('Forbidden');
    return false;
  }
  return true;
}

// ─── TwiML webhook ───
app.post('/incoming-call', function(req, res) {
  const url = buildTwilioWebhookUrl(req, '/incoming-call');
  if (!validateTwilioSignature(req, res, url)) return;

  const from = (req.body && req.body.From) ? req.body.From : 'unknown';
  const to = (req.body && req.body.To) ? req.body.To : 'unknown';
  const direction = (req.body && req.body.Direction) ? req.body.Direction : '';
  const worker = direction.startsWith('outbound') ? to : from;
  console.log('[Twilio] Call:', direction, '| From:', from, '| To:', to, '| Worker:', worker);
  const wsUrl = WS_URL + '/twilio';
  res.type('text/xml');
  res.send(`<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="${wsUrl}">
      <Parameter name="caller" value="${from}"/>
      <Parameter name="worker" value="${worker}"/>
    </Stream>
  </Connect>
</Response>`);
});

// ─── Shared functions ───
async function classifyIntent(transcript, model, opts) {
  if (!(opts && opts.skipFastPath)) {
    const fast = fastPathIntent(transcript);
    if (fast) return fast;
  }

  // Streaming: measure TTFT (first text chunk), return only after full JSON accumulates.
  const started = Date.now();
  let firstChunkAt = null;
  let fullText = '';
  let usage = { input_tokens: 0, output_tokens: 0 };

  const stream = anthropic.messages.stream({
    model: model || 'claude-haiku-4-5',
    max_tokens: 100,
    temperature: 0,
    system: COMPARE_INTENT_PROMPT,
    messages: [{ role: 'user', content: 'Worker said: "' + transcript + '"' }]
  });

  for await (const chunk of stream) {
    if (chunk.type === 'content_block_delta' && chunk.delta?.type === 'text_delta') {
      if (firstChunkAt == null) firstChunkAt = Date.now();
      fullText += chunk.delta.text;
    }
    if (chunk.type === 'message_start' && chunk.message?.usage) {
      usage.input_tokens = chunk.message.usage.input_tokens || 0;
      usage.cache_read_input_tokens = chunk.message.usage.cache_read_input_tokens || 0;
      const cacheRead = chunk.message.usage.cache_read_input_tokens || 0;
      const cacheCreate = chunk.message.usage.cache_creation_input_tokens || 0;
      if (cacheRead > 0) console.log('[Cache] HIT — cache_read_tokens:', cacheRead);
      else if (cacheCreate > 0) console.log('[Cache] MISS — cache_creation_tokens:', cacheCreate);
    }
    if (chunk.type === 'message_delta' && chunk.usage) {
      usage.output_tokens = chunk.usage.output_tokens || 0;
    }
  }

  // Prefer final message usage (authoritative input_tokens + output_tokens)
  try {
    const finalMsg = await stream.finalMessage();
    if (finalMsg && finalMsg.usage) {
      usage = parseClaudeUsage(finalMsg.usage);
      if (finalMsg.usage.cache_read_input_tokens) {
        usage.cache_read_input_tokens = finalMsg.usage.cache_read_input_tokens;
      }
    } else {
      usage = parseClaudeUsage(usage);
    }
  } catch (e) {
    usage = parseClaudeUsage(usage);
  }

  const parsed = parseIntentJson(fullText);
  parsed._usage = usage;
  parsed._ttft_ms = firstChunkAt != null ? firstChunkAt - started : Date.now() - started;
  parsed._total_ms = Date.now() - started;
  return parsed;
}

async function classifyIntentFast(transcript) {
  const durationMatch = transcript.match(DURATION_REGEX);
  const duration = durationMatch ? durationMatch[1] + ' ' + durationMatch[2] : null;

  const response = await anthropic.messages.create({
    model: 'claude-haiku-4-5',
    max_tokens: 8,
    temperature: 0,
    system: INTENT_PROMPT,
    messages: [{ role: 'user', content: transcript }],
  });

  const raw = ((response.content[0] && response.content[0].text) || '').trim().toLowerCase();
  const word = raw.split(/[^a-z_]+/).find(function(w) { return VALID_INTENTS_SET.has(w); });
  const intent = word || 'ambiguous';

  console.log('[Intent Fast]', intent, duration ? '| duration: ' + duration : '');
  return { intent: intent, duration: duration };
}

function intentToResult(intent, duration) {
  const intentMap = {
    safe: {
      intent: 'SAFE_CONFIRMATION',
      classifiedIntent: 'SAFE_CONFIRMATION',
      response: CACHE_RESPONSES.safe_confirmation,
      resolved: true,
      escalate: false,
      confidence: 0.95,
      distress: null,
      distress_signals: null,
      delay_minutes: null,
    },
    delayed: {
      intent: 'DELAYED_CHECKIN',
      classifiedIntent: 'DELAYED_CHECKIN',
      response: CACHE_RESPONSES.delayed_checkin_15,
      resolved: true,
      escalate: false,
      confidence: 0.9,
      distress: null,
      distress_signals: null,
      delay_minutes: duration ? parseInt(duration, 10) : 15,
    },
    request_human: {
      intent: 'REQUEST_HUMAN',
      classifiedIntent: 'REQUEST_HUMAN',
      response: CACHE_RESPONSES.request_human,
      resolved: false,
      escalate: true,
      confidence: 0.95,
      distress: null,
      distress_signals: null,
      delay_minutes: null,
    },
    unsafe: {
      intent: 'REQUEST_HUMAN',
      classifiedIntent: 'REQUEST_HUMAN',
      response: 'I can hear this may be urgent. I am connecting you to a live agent right now. Please stay on the line.',
      resolved: false,
      escalate: true,
      confidence: 0.95,
      distress: 'high',
      distress_signals: 'unsafe',
      delay_minutes: null,
    },
    ambiguous: {
      intent: 'UNRECOGNISED',
      classifiedIntent: 'UNRECOGNISED',
      response: CACHE_RESPONSES.unrecognised,
      resolved: false,
      escalate: false,
      confidence: 0.3,
      distress: null,
      distress_signals: null,
      delay_minutes: null,
    },
  };

  return intentMap[intent] || intentMap.ambiguous;
}

async function synthesizeSpeech(text) {
  const apiKey = process.env.ELEVENLABS_API_KEY;
  const voiceId = process.env.ELEVENLABS_VOICE_ID || 'EXAVITQu4vr4xnSDxMaL';
  const response = await axios.post(
    'https://api.elevenlabs.io/v1/text-to-speech/' + voiceId,
    { text: text, model_id: 'eleven_flash_v2_5', voice_settings: { stability: 0.5, similarity_boost: 0.75 } },
    { headers: { 'xi-api-key': apiKey, 'Content-Type': 'application/json', 'Accept': 'audio/mpeg' }, responseType: 'arraybuffer' }
  );
  return Buffer.from(response.data);
}

async function mp3ToMulaw(mp3Buffer) {
  return new Promise(function(resolve, reject) {
    const ffmpeg = spawn('ffmpeg', [
      '-i', 'pipe:0',
      '-ar', '8000',
      '-ac', '1',
      '-acodec', 'pcm_mulaw',
      '-f', 'mulaw',
      'pipe:1'
    ]);
    const chunks = [];
    ffmpeg.stdout.on('data', function(chunk) { chunks.push(chunk); });
    ffmpeg.stdout.on('end', function() { resolve(Buffer.concat(chunks)); });
    ffmpeg.stderr.on('data', function() {});
    ffmpeg.stdin.write(mp3Buffer);
    ffmpeg.stdin.end();
    ffmpeg.on('error', function(e) {
      console.error('[ffmpeg] Error:', e.message);
      reject(new Error('ffmpeg failed: ' + e.message));
    });
  });
}

async function* streamTTS(text, apiKey, voiceId) {
  const response = await axios.post(
    'https://api.elevenlabs.io/v1/text-to-speech/' + voiceId + '/stream',
    { text: text, model_id: 'eleven_flash_v2_5', voice_settings: { stability: 0.5, similarity_boost: 0.75 }, optimize_streaming_latency: 4 },
    { headers: { 'xi-api-key': apiKey, 'Content-Type': 'application/json', 'Accept': 'audio/mpeg' }, responseType: 'stream' }
  );
  const CHUNK_SIZE = 4096;
  let buffer = Buffer.alloc(0);
  for await (const chunk of response.data) {
    buffer = Buffer.concat([buffer, chunk]);
    while (buffer.length >= CHUNK_SIZE) {
      yield buffer.slice(0, CHUNK_SIZE).toString('base64');
      buffer = buffer.slice(CHUNK_SIZE);
    }
  }
  if (buffer.length > 0) yield buffer.toString('base64');
}

function extractSpoken(text) {
  try {
    const parsed = JSON.parse(text);
    return parsed.response || text;
  } catch (e) {
    return text;
  }
}

function applyDistressLogic(result) {
  if (result.distress === 'high') {
    result.intent = 'REQUEST_HUMAN';
    result.escalate = true;
    result.resolved = false;
    result.response = 'I can hear this may be urgent. I am connecting you to a live agent right now. Please stay on the line.';
    console.log('[DISTRESS HIGH] Auto-escalating | signals:', result.distress_signals);
  } else if (result.distress === 'medium' && !(result.intent === 'SAFE_CONFIRMATION' && result.confidence > 0.85)) {
    result.intent = 'REQUEST_HUMAN';
    result.escalate = true;
    result.resolved = false;
    result.response = 'I want to make sure you are okay. I am connecting you to a live agent who can help. Please stay on the line.';
    console.log('[DISTRESS MEDIUM] Auto-escalating | signals:', result.distress_signals);
  }
  return result;
}

// ─── Twilio WebSocket ───
twilioWss.on('connection', function(ws) {
  console.log('[Twilio WS] Connected');
  let streamSid = null;
  let callSid = null;
  let deepgramWs = null;
  let sessionState = 'GREETING';
  let unrecognisedCount = 0;
  let lastMediaAt = 0;
  let callLastConfidence = null;
  let safetyConfirmPending = false;
  let silenceTimer = null;
  let callSttMs = null;
  let callLlmMs = null;
  let callTtsMs = null;
  let callStartTime = new Date().toISOString();
  let callFinalIntent = null;
  let callTranscript = [];
  let workerPhone = null;
  const noise = createNoiseEstimator({ frameMs: 20 });
  const vad = createSileroVad({ sampleRate: 8000 });
  let dgClockMs = 0;
  let agentPlaybackUntil = 0;
  let uttWords = [];

  function clearSilenceTimer() { if (silenceTimer) { clearTimeout(silenceTimer); silenceTimer = null; } }

  function sendAudioToTwilio(audioBuffer) {
    if (ws.readyState !== WebSocket.OPEN || !streamSid) return;
    const durMs = audioBuffer.length / 8;
    agentPlaybackUntil = Math.max(agentPlaybackUntil, Date.now()) + durMs;
    ws.send(JSON.stringify({
      event: 'media',
      streamSid: streamSid,
      media: { payload: audioBuffer.toString('base64') }
    }));
  }

  async function speakText(text, cacheKey, trackLatency) {
    const ttsStart = Date.now();
    try {
      // Önce mulaw cache'e bak
      if (cacheKey && mulawCache[cacheKey]) {
        console.log('[Twilio Cache] Hit:', cacheKey);
        const elapsed = Date.now() - ttsStart;
        if (trackLatency) callTtsMs = elapsed;
        sendAudioToTwilio(mulawCache[cacheKey]);
        callTranscript.push({ role: 'ai', text: text, time: new Date().toISOString() });
        return;
      }

      // Cache miss — ElevenLabs'tan canlı üret
      console.log('[Twilio Cache] Miss:', cacheKey, '— generating from ElevenLabs');
      const mp3Buffer = await synthesizeSpeech(text);
      const elapsed = Date.now() - ttsStart;
      if (trackLatency) callTtsMs = elapsed;
      console.log('[Twilio TTS] Generated in', elapsed + 'ms, bytes:', mp3Buffer.length);
      const mulawBuffer = await mp3ToMulaw(mp3Buffer);
      sendAudioToTwilio(mulawBuffer);
      callTranscript.push({ role: 'ai', text: text, time: new Date().toISOString() });
    } catch (e) {
      console.error('[Twilio TTS Error]', e.message);
    }
  }

  async function handleTwilioTranscript(finalTranscript) {
    if (!finalTranscript || finalTranscript.trim() === '') return;
    const speechSpans = uttWords.map(function(w) { return { startMs: w.start * 1000, endMs: w.end * 1000, word: w.word }; });
    uttWords = [];
    const m = noise.endTurn({ speechSpans: speechSpans.length ? speechSpans : null });
    console.log("[Noise] snr=" + (m.snrDb != null ? m.snrDb.toFixed(1) : "n/a") + "floor=" + 
    (m.floorDbfs != null ? m.floorDbfs.toFixed(1) : "n/a") + "noisy=" + m.noisy);
    console.log('[Twilio Transcript]', finalTranscript);
    callTranscript.push({ role: 'worker', text: finalTranscript, time: new Date().toISOString() });
    sessionState = 'PROCESSING';
    try {
      const llmStart = Date.now();
      const { intent, duration } = await classifyIntentFast(finalTranscript);
      callLlmMs = Date.now() - llmStart;
      let result = intentToResult(intent, duration);

      const CONFIDENCE_THRESHOLD = 0.6;
      const criticalIntents = ['SAFE_CONFIRMATION', 'DELAYED_CHECKIN'];
      const lowConf = callLastConfidence !== null && callLastConfidence < CONFIDENCE_THRESHOLD;
      const noisyEnv = m.noisy;
      if ((lowConf || noisyEnv) && criticalIntents.includes(result.intent) && !safetyConfirmPending) {
        safetyConfirmPending = true;
        const confirmText = result.intent === "SAFE_CONFIRMATION" ? (noisyEnv ?
           "Sorry, it's a bit noisy. Can you confirm clearly that you are safe?" : 
           "Sorry, it's a bit hard to hear you. Can you confirm clearly that you are safe?")
           : "Sorry, I didn't catch that clearly. How many more minutes do you need?";
           console.log("[Confidence] lowConf=" + lowConf + "noisy=" + noisyEnv + "for", result.intent, "-asking to confirm");
           callLastConfidence = null;
           await speakText(confirmText, null, false);
           sessionState = "LISTENING";
           return;
      }
      safetyConfirmPending = false;
      callLastConfidence = null;

      if (!result.distress) applyCachedResponse(result);
      callFinalIntent = result.intent;
      console.log('[Twilio Intent]', result.intent, '| conf:', result.confidence, '| distress:', result.distress || 'none', '| LLM:', callLlmMs + 'ms');
      const cacheKey = result.distress ? null : INTENT_CACHE_MAP[result.intent];
      await speakText(result.response, cacheKey, true);
      if (result.intent === 'UNRECOGNISED') {
        unrecognisedCount++;
        if (unrecognisedCount > 1) {
          await speakText("I'm unable to process your response. A human agent will follow up shortly.", null, false);
          sessionState = 'ENDED'; return;
        }
      }
      sessionState = (result.resolved || result.escalate) ? 'ENDED' : 'LISTENING';
    } catch (err) {
      console.error('[Twilio Error]', err.message);
      sessionState = 'LISTENING';
    }
  }

  function startTwilioDeepgram() {
    const dgApiKey = process.env.DEEPGRAM_API_KEY;
    const params = new URLSearchParams({ ...DEEPGRAM_BASE_PARAMS, encoding: 'mulaw', sample_rate: '8000', channels: '1' });
    const dgUrl = 'wss://api.deepgram.com/v1/listen?' + params.toString() + DEEPGRAM_KEYTERMS;
    console.log('[Deepgram] URL:', dgUrl);
    deepgramWs = new WebSocket(dgUrl, {
      headers: { Authorization: 'Token ' + dgApiKey }
    });
    deepgramWs.on('open', function() { console.log('[Twilio Deepgram] Connected'); });
    deepgramWs.on('message', async function(data) {
      try {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'Results' && msg.channel && msg.channel.alternatives && msg.channel.alternatives[0]) {
          const alt = msg.channel.alternatives[0];
          const text = alt.transcript;
          if (msg.is_final && alt.words && alt.words.length) uttWords.push(...alt.words);
          if (text && text.trim()) {
            if (msg.is_final && typeof alt.confidence === 'number') {
              callLastConfidence = alt.confidence;
            }
            if (msg.is_final && msg.speech_final && sessionState === 'LISTENING') {
              clearSilenceTimer();
              const sttMs = lastMediaAt ? Date.now() - lastMediaAt : 300;
              callSttMs = sttMs;
              console.log('[Twilio STT]', sttMs + 'ms');
              await handleTwilioTranscript(text);
            }
          }
        }
      } catch (e) {}
    });
    deepgramWs.on('error', function(e) { console.error('[Twilio Deepgram] Error:', e.message); });
    deepgramWs.on('close', function() { console.log('[Twilio Deepgram] Closed'); });
  }

  ws.on('message', async function(data) {
    try {
      const msg = JSON.parse(data.toString());
      if (msg.event === 'connected') { console.log('[Twilio] Stream connected'); }
      if (msg.event === 'start') {
        console.log('[Twilio] Start event data:', JSON.stringify(msg.start));
        streamSid = msg.start.streamSid;
        callSid = msg.start.callSid || streamSid;
        workerPhone = (msg.start.customParameters && msg.start.customParameters.worker)
          || (msg.start.customParameters && msg.start.customParameters.caller)
          || null;
        callStartTime = new Date().toISOString();
        console.log('[Twilio] Stream started | SID:', streamSid, '| Call:', callSid);
        try {
          insertCallStart({
            callSid: callSid,
            streamSid: streamSid,
            workerPhone: workerPhone,
            startTime: callStartTime,
            modelUsed: 'claude-haiku-4-5',
          });
        } catch (e) {
          console.error('[Twilio] DB insert error:', e.message);
        }
        startTwilioDeepgram();
        sessionState = 'GREETING';
        setTimeout(async function() {
          await speakText(WORKER_INTRO, 'greeting', false);
          sessionState = 'LISTENING';
        }, 1000);
      }
      if (msg.event === 'media' && deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
        lastMediaAt = Date.now();
        const buf = Buffer.from(msg.media.payload, "base64");
        noise.setAgentSpeaking(Date.now() < agentPlaybackUntil + 200);
        vad.processMulaw(buf);
        noise.pushFrame({ ...frameStatsMulaw(buf), speechProb: vad.prob() }, dgClockMs);
        dgClockMs += buf.length / 8;
        if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) deepgramWs.send(buf);
      }
      if (msg.event === 'stop') {
        console.log('[Twilio] Stream stopped');
        if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) deepgramWs.close();
        clearSilenceTimer();
      }
    } catch (e) { console.error('[Twilio WS Error]', e.message); }
  });

  ws.on('close', function() {
    console.log('[Twilio WS] Disconnected');
    if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) deepgramWs.close();
    clearSilenceTimer();

    if (callSttMs != null || callLlmMs != null || callTtsMs != null) {
      console.log('[Twilio Latency] STT:', (callSttMs != null ? callSttMs : '—') + 'ms | LLM:', (callLlmMs != null ? callLlmMs : '—') + 'ms | TTS:', (callTtsMs != null ? callTtsMs : '—') + 'ms | Total:', ((callSttMs || 0) + (callLlmMs || 0) + (callTtsMs || 0)) + 'ms');
    }

    if (callSid) {
      try {
        const endTime = new Date().toISOString();
        const durationSeconds = Math.round((new Date(endTime) - new Date(callStartTime)) / 1000);
        updateCallEnd({
          callSid: callSid,
          streamSid: streamSid,
          workerPhone: workerPhone,
          endTime: endTime,
          durationSeconds: durationSeconds,
          sessionState: sessionState === 'ENDED' ? 'ENDED' : sessionState,
          finalIntent: callFinalIntent,
          transcript: callTranscript,
          silenceCount: 0,
          sttMs: callSttMs,
          llmMs: callLlmMs,
          ttsMs: callTtsMs,
        });
        updateCallLatency({ callSid: callSid, sttMs: callSttMs, llmMs: callLlmMs, ttsMs: callTtsMs });
        console.log('[Twilio] Call saved to DB:', callSid);
      } catch (e) {
        console.error('[Twilio] Latency DB update error:', e.message);
      }
    }
  });
});

// ─── Browser WebSocket ───
browserWss.on('connection', function(ws) {
  console.log('[WS] Client connected');
  let deepgramWs = null;
  let transcript = '';
  let sessionState = 'GREETING';
  let unrecognisedCount = 0;
  let audioBuffer = Buffer.alloc(0);
  let silenceTimer = null;
  let clientMimeType = 'audio/pcm;rate=16000';
  let isReceivingAudioFile = false;
  let audioFileChunks = [];
  let audioFileDurationSec = null;
  let audioFileName = null;
  let snrBlocked = false;
  let selectedModel = 'claude-haiku-4-5';
  let ttsMode = 'elevenlabs';
  let lastMediaAt = 0;
  let lastConfidence = null;
  let safetyConfirmPending = false;
  let clientE2eMs = null;
  let deepFilter = null;
  let dfPreferred = DEEPFILTER_ENABLED;  // TEMP: off unless DEEPFILTER_ENABLED=true
  let dfEnabled = DEEPFILTER_ENABLED;
  const noise = createNoiseEstimator({ frameMs: 20 });
  const vad = createSileroVad({ sampleRate: 16000 });
  let audioClockMs = 0;
  let vadChain = Promise.resolve();
  let uttWords = [];
  let lastAgentText = null;
  const SNR_HARD_MIN = 5;
  const LOWCONF_RATIO_MAX = 0.5;
  const SILENCE_MS = 4000;

  function send(obj) { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj)); }
  function clearSilenceTimer() { if (silenceTimer) { clearTimeout(silenceTimer); silenceTimer = null; } }

  function ensureDeepFilter() {
    if (!DEEPFILTER_ENABLED) {
      deepFilter = null;
      dfEnabled = false;
      return false;
    }
    if (!isDeepFilterReady()) {
      deepFilter = null;
      dfEnabled = false;
      return false;
    }
    if (!deepFilter) {
      deepFilter = createDeepFilter({ inputRate: 16000, outputRate: 16000 });
    }
    dfEnabled = !!dfPreferred;
    return dfEnabled;
  }

  // Activate Noise Cancel immediately on connect (no toggle required) — skipped when DEEPFILTER_ENABLED=false
  if (!DEEPFILTER_ENABLED) {
    console.log('[DeepFilter] DISABLED (DEEPFILTER_ENABLED=false) — raw mic → Deepgram');
  } else if (ensureDeepFilter()) {
    console.log('[DeepFilter] Session filter ready (active=true)');
  } else {
    console.warn('[DeepFilter] Model not ready — denoise inactive until model loads');
  }

  function sendAudioToDeepgram(buf) {
    if (!buf || !buf.length) return;
    if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) deepgramWs.send(buf);
    else audioBuffer = Buffer.concat([audioBuffer, buf]);
  }

  async function handleTranscript(finalTranscript, sttMs) {
    if (!finalTranscript || finalTranscript.trim() === '') return;
    const speechSpans = uttWords.map(function(w, i) {
      return { startMs: (w.start || 0) * 1000, endMs: (w.end || 0) * 1000, word: w.word, idx: i,
         conf: (w.confidence != null ? w.confidence : null) };
    });
    uttWords = [];
    const m = noise.endTurn({ speechSpans: speechSpans.length ? speechSpans : null });
    const totalWords = Math.max(speechSpans.length, 1);
    const hitIdx = new Set(m.collisions.filter(function(c) { 
      return c.type === "overlap" && c.idx != null; }).map(function(c) { return c.idx; }));
    const lowConfRatio = speechSpans.filter(function(s) { return s.conf != null && s.conf < 0.6; }).length / totalWords;
    const untrusted = m.speechFallback
     || (m.snrReliable && m.snrDb != null && m.snrDb <= SNR_HARD_MIN)
     || lowConfRatio >= LOWCONF_RATIO_MAX;
     console.log('[Noise] snr=' + (m.snrDb != null ? m.snrDb.toFixed(1) : 'n/a') + ' noisy=' + m.noisy
      + ' collisions=' + m.collisions.length + ' lowConfRatio=' + lowConfRatio.toFixed(2)
       + ' untrusted=' + untrusted); 
    if (snrBlocked) {
      console.log('[SNR] Transcript blocked due to bad SNR:', finalTranscript);
      snrBlocked = false;
      return;
    }
    console.log('[Transcript]', finalTranscript, '| STT:', sttMs + 'ms');
    send({ type: 'transcript', text: finalTranscript });
    if (sttMs) send({ type: 'stt_latency', ms: sttMs });
    sessionState = 'PROCESSING';
    send({ type: 'stage', stage: 'llm', status: 'processing' });
    const llmStart = Date.now();
    try {
      const { intent, duration } = await classifyIntentFast(finalTranscript);
      const llmMs = Date.now() - llmStart;
      let result = intentToResult(intent, duration);

            const CONFIDENCE_THRESHOLD = 0.6;
      const criticalIntents = ['SAFE_CONFIRMATION', 'DELAYED_CHECKIN'];
      const lowConf = lastConfidence !== null && lastConfidence < CONFIDENCE_THRESHOLD;

      let maskImportant = false;
      if (!untrusted && m.hasCollision && criticalIntents.includes(result.intent)) {
        const maskedItems = [...new Set(m.collisions.map(function(c) {
          return c.type === 'overlap' && c.word ? c.word
            : '[MISSING between "' + (c.prevWord || '<start>') + '" and "' + (c.nextWord || '<end>') + '"]';
        }))];
        const gaps = m.collisions.filter(function(c) { return c.type === 'gap'; })
          .sort(function(a, b) { return a.tMs - b.tMs; });
        const parts = [];
        let gi = 0;
        for (const s of speechSpans) {
          while (gi < gaps.length && gaps[gi].tMs < s.startMs) { parts.push('[NOISE ' + (gaps[gi].durationMs / 1000).toFixed(1) + 's]'); gi++; }
          if (s.word) parts.push(hitIdx.has(s.idx) ? '[' + s.word + ']' : s.word);
        }
        while (gi < gaps.length) { parts.push('[NOISE ' + (gaps[gi].durationMs / 1000).toFixed(1) + 's]'); gi++; }
        const annotated = parts.join(' ');
        try {
          const maskRes = await classifyMaskedWordImportance(annotated, maskedItems, lastAgentText);
          maskImportant = maskRes.important;
        } catch (e) { maskImportant = true; }
        console.log('[Mask] annotated="' + annotated + '" important=' + maskImportant);
      }

      const noisyTurn = m.noisy || untrusted || maskImportant;
      if ((lowConf || noisyTurn) && criticalIntents.includes(result.intent) && !safetyConfirmPending) {
        safetyConfirmPending = true;
        const confirmText = result.intent === 'SAFE_CONFIRMATION'
          ? (noisyTurn ? "Sorry, it's a bit noisy. Can you confirm clearly that you are safe?"
                       : "Sorry, it's a bit hard to hear you. Can you confirm clearly that you are safe?")
          : "Sorry, I didn't catch that clearly. How many more minutes do you need?";
        lastAgentText = confirmText;
        console.log('[Confidence] lowConf=' + lowConf + ' noisyTurn=' + noisyTurn + ' for', result.intent);
        send({ type: 'stage', stage: 'llm', status: 'done', ms: llmMs });
        send({ type: 'response', text: confirmText, audio: null, resolved: false, escalate: false });
        try {
          const apiKey = process.env.ELEVENLABS_API_KEY;
          const voiceId = process.env.ELEVENLABS_VOICE_ID || 'EXAVITQu4vr4xnSDxMaL';
          send({ type: 'audio_stream_start', text: confirmText, resolved: false, escalate: false });
          let chunkIndex = 0;
          let firstChunkSent = false;
          const ttsStart = Date.now();
          for await (const chunk of streamTTS(confirmText, apiKey, voiceId)) {
            if (!firstChunkSent) {
              send({ type: 'stage', stage: 'tts', status: 'done', ms: Date.now() - ttsStart });
              firstChunkSent = true;
            }
            send({ type: 'audio_chunk', chunk, index: chunkIndex++ });
          }
          send({ type: 'audio_stream_end' });
        } catch (e) {
          console.error('[Confirm TTS Error]', e.message);
        }
        sessionState = 'LISTENING';
        lastConfidence = null;
        return;
      }
      safetyConfirmPending = false;
      lastConfidence = null;

      if (!result.distress) applyCachedResponse(result);
      console.log('[Intent]', result.intent, '| conf:', result.confidence, '| distress:', result.distress || 'none', '| LLM:', llmMs + 'ms');
      lastAgentText = result.response;
      send({ type: 'intent', data: result, client_e2e_ms: clientE2eMs });
      clientE2eMs = null;
      send({ type: 'stage', stage: 'llm', status: 'done', ms: llmMs });
      send({ type: 'stage', stage: 'tts', status: 'processing' });
      const ttsStart = Date.now();
      if (ttsMode === 'browser') {
        send({ type: 'stage', stage: 'tts', status: 'done', ms: 0 });
        send({ type: 'response', text: result.response, audio: null, resolved: result.resolved, escalate: result.escalate });
      } else {
        const cacheKey = result.distress ? null : INTENT_CACHE_MAP[result.intent];
        const useCache = cacheKey && audioCache[cacheKey];
        if (useCache) {
          console.log('[Cache] Hit:', cacheKey);
          send({ type: 'stage', stage: 'tts', status: 'done', ms: Date.now() - ttsStart });
          send({ type: 'response', text: result.response, audio: audioCache[cacheKey], resolved: result.resolved, escalate: result.escalate });
        } else {
          const apiKey = process.env.ELEVENLABS_API_KEY;
          const voiceId = process.env.ELEVENLABS_VOICE_ID || 'EXAVITQu4vr4xnSDxMaL';
          const spoken = extractSpoken(result.response);
          try {
            send({ type: 'audio_stream_start', text: spoken, resolved: result.resolved, escalate: result.escalate });
            const sentences = spoken.match(/[^.!?]+[.!?]+/g) || [spoken];
            let chunkIndex = 0;
            let firstChunkSent = false;
            for (const sentence of sentences) {
              const clean = sentence.trim();
              if (!clean) continue;
              for await (const chunk of streamTTS(clean, apiKey, voiceId)) {
                if (!firstChunkSent) {
                  send({ type: 'stage', stage: 'tts', status: 'done', ms: Date.now() - ttsStart });
                  firstChunkSent = true;
                }
                send({ type: 'audio_chunk', chunk: chunk, index: chunkIndex++ });
              }
            }
            send({ type: 'audio_stream_end' });
          } catch (e) {
            console.error('[TTS Stream Error]', e.message);
            send({ type: 'stage', stage: 'tts', status: 'done', ms: 0 });
            send({ type: 'response', text: result.response, audio: null, resolved: result.resolved, escalate: result.escalate });
          }
        }
      }
      if (result.intent === 'UNRECOGNISED') {
        unrecognisedCount++;
        if (unrecognisedCount > 1) {
          send({ type: 'escalate', reason: 'max_unrecognised', message: "I'm unable to process your response. A human agent will follow up shortly." });
          sessionState = 'ENDED'; return;
        }
      }
      sessionState = (result.resolved || result.escalate) ? 'ENDED' : 'LISTENING';
    } catch (err) {
      console.error('[Error]', err.message);
      send({ type: 'error', message: err.message });
      sessionState = 'LISTENING';
    }
  }

  function startDeepgramStream() {
    const dgApiKey = process.env.DEEPGRAM_API_KEY;
    if (!dgApiKey || dgApiKey === 'your_deepgram_key') { send({ type: 'log', message: 'Deepgram API key not set — mock STT mode' }); return; }
    const isOgg = clientMimeType.includes('ogg');
    const isPcm = clientMimeType.includes('pcm') || clientMimeType.includes('linear16');
    const params = new URLSearchParams({ ...DEEPGRAM_BASE_PARAMS });
    if (isPcm) {
      params.set('encoding', 'linear16');
      params.set('sample_rate', '16000');
    } else if (isOgg) {
      params.set('encoding', 'opus');
      params.set('container', 'ogg');
    }
    const dgUrl = 'wss://api.deepgram.com/v1/listen?' + params.toString() + DEEPGRAM_KEYTERMS;
    console.log('[Deepgram] URL:', dgUrl);
    if (ensureDeepFilter()) {
      console.log('[DeepFilter] Session filter ready (enabled=' + dfEnabled + ')');
    } else {
      console.warn('[DeepFilter] Model not ready — denoise disabled for this session');
    }
    deepgramWs = new WebSocket(dgUrl, {
      headers: { Authorization: 'Token ' + dgApiKey }
    });
    deepgramWs.on('open', function() {
      console.log('[Deepgram] Connected');
      send({ type: 'stage', stage: 'stt', status: 'connected' });
      if (audioBuffer.length > 0) { deepgramWs.send(audioBuffer); audioBuffer = Buffer.alloc(0); }
    });
    deepgramWs.on('message', async function(data) {
      try {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'Results' && msg.channel && msg.channel.alternatives && msg.channel.alternatives[0]) {
          const alt = msg.channel.alternatives[0];
          const text = alt.transcript;
          if (msg.is_final && alt.words && alt.words.length) uttWords.push(...alt.words);
          if (text && text.trim()) {
            if (msg.is_final && typeof alt.confidence === 'number') {
              lastConfidence = alt.confidence;
            }
            send({ type: 'interim_transcript', text: text, is_final: msg.is_final });
            if (msg.is_final && msg.speech_final && sessionState === 'LISTENING') {
              clearSilenceTimer();
              const sttMs = lastMediaAt ? Date.now() - lastMediaAt : 300;
              transcript = text;
              const toProcess = transcript; transcript = '';
              await handleTranscript(toProcess, sttMs);
            }
          }
        }
        if (msg.type === 'UtteranceEnd' && transcript && sessionState === 'LISTENING') {
          clearSilenceTimer();
          const sttMs = lastMediaAt ? Date.now() - lastMediaAt : 300;
          const toProcess = transcript; transcript = '';
          await handleTranscript(toProcess, sttMs);
        }
      } catch (e) {}
    });
    deepgramWs.on('error', function(e) { console.error('[Deepgram] Error:', e.message); send({ type: 'stage', stage: 'stt', status: 'error' }); });
    deepgramWs.on('close', function(code, reason) { console.log('[Deepgram] Closed', code, reason ? reason.toString() : ''); send({ type: 'stage', stage: 'stt', status: 'disconnected' }); });
  }

  ws.on('error', function(e) { console.error('[WS Error]', e.message); });

  ws.on('message', async function(data, isBinary) {
    try {
      // Text frames = JSON control messages. Binary frames = audio (never JSON.parse).
      // Do NOT use data[0]===0x7b: PCM chunks can start with '{' by chance and throw Unexpected token.
      if (!isBinary) {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'mime_type') { clientMimeType = msg.mimeType; }
        if (msg.type === 'set_model') { selectedModel = msg.model; console.log('[Model]', selectedModel); }
        if (msg.type === 'tts_mode') { ttsMode = msg.mode; }
        if (msg.type === 'toggle_denoise') {
          if (!DEEPFILTER_ENABLED) {
            dfPreferred = false;
            dfEnabled = false;
            console.log('[DeepFilter] toggle ignored — DEEPFILTER_ENABLED=false (forced bypass)');
            send({ type: 'log', message: 'Noise cancellation disabled (server bypass for transcript test)' });
          } else {
            dfPreferred = msg.enabled !== false; // default ON unless explicitly false
            ensureDeepFilter();
            if (!dfPreferred) dfEnabled = false;
            console.log('[DeepFilter]', dfPreferred ? 'preferred ON' : 'preferred OFF', '| active=', dfEnabled);
            send({ type: 'log', message: 'Noise cancellation ' + (dfEnabled ? 'enabled' : 'disabled') });
          }
        }
        if (msg.type === 'client_metrics') {
          clientE2eMs = msg.total;
          console.log('[Client E2E]', clientE2eMs + 'ms');
          send({ type: 'client_e2e', ms: clientE2eMs });
        }
        if (msg.type === 'start') {
          send({ type: 'log', message: 'Session started — Missed Check-In alert for Industrial Worker' });
          sessionState = 'GREETING';
          send({ type: 'stage', stage: 'stt', status: 'connecting' });
          send({ type: 'stage', stage: 'tts', status: 'processing' });
          startDeepgramStream();
          send({
            type: 'log',
            message: dfEnabled
              ? 'Noise cancellation enabled (DeepFilterNet3)'
              : 'Noise cancellation unavailable — raw PCM to Deepgram',
          });
          let introAudio = null;
          if (ttsMode === 'elevenlabs') {
            if (audioCache['greeting']) {
              console.log('[Cache] Using cached greeting');
              introAudio = audioCache['greeting'];
            } else {
              try {
                const apiKey = process.env.ELEVENLABS_API_KEY;
                const voiceId = process.env.ELEVENLABS_VOICE_ID || 'EXAVITQu4vr4xnSDxMaL';
                const chunks = [];
                for await (const chunk of streamTTS(WORKER_INTRO, apiKey, voiceId)) chunks.push(chunk);
                introAudio = chunks.join('');
              } catch (e) { console.error('[Greeting TTS Error]', e.message); }
            }
          }
          send({ type: 'stage', stage: 'tts', status: 'done' });
          send({ type: 'greeting', text: WORKER_INTRO, audio: introAudio });
                    lastAgentText = WORKER_INTRO;
          sessionState = 'LISTENING';
        }
        if (msg.type === 'snr_block') {
          console.log('[SNR] Block received — ignoring current transcript');
          snrBlocked = true;
          sessionState = 'LISTENING';
        }

        if (msg.type === 'mock_transcript' && sessionState === 'LISTENING') await handleTranscript(msg.text, null);

        if (msg.type === 'snr_retry' && sessionState === 'LISTENING') {
          console.log('[SNR] Retry requested — re-prompting worker');
          const retryText = "I'm sorry, there was too much background noise. Could you please repeat your response?";
          const apiKey = process.env.ELEVENLABS_API_KEY;
          const voiceId = process.env.ELEVENLABS_VOICE_ID || 'EXAVITQu4vr4xnSDxMaL';
          try {
            send({ type: 'audio_stream_start', text: retryText, resolved: false, escalate: false });
            let firstChunk = true; let chunkIndex = 0;
            for await (const chunk of streamTTS(retryText, apiKey, voiceId)) {
              if (firstChunk) { firstChunk = false; }
              send({ type: 'audio_chunk', chunk, index: chunkIndex++ });
            }
            send({ type: 'audio_stream_end' });
          } catch(e) {
            send({ type: 'response', text: retryText, audio: null, resolved: false, escalate: false });
          }
        }

        if (msg.type === 'audio_file_start' && sessionState === 'LISTENING') {
          const dur = (typeof msg.duration === 'number' && Number.isFinite(msg.duration))
            ? msg.duration
            : (msg.duration != null && Number.isFinite(parseFloat(msg.duration)) ? parseFloat(msg.duration) : null);
          audioFileDurationSec = dur;
          audioFileName = msg.filename || null;
          console.log('[Server] Audio file upload started:', msg.filename, dur != null ? (dur + 's') : 'duration=n/a');
          isReceivingAudioFile = true;
          audioFileChunks = [];
          send({
            type: 'audio_file_start_ack',
            filename: msg.filename || null,
            duration: audioFileDurationSec,
            size: msg.size != null ? msg.size : null,
            mime: msg.mime || null,
          });
          send({
            type: 'log',
            message: 'Receiving audio file: ' + msg.filename
              + (audioFileDurationSec != null ? (' (' + audioFileDurationSec + 's)') : ''),
          });
        }

        if (msg.type === 'audio_file_end' && isReceivingAudioFile) {
          isReceivingAudioFile = false;
          if (audioFileChunks.length === 0) return;
          const fullBuffer = Buffer.concat(audioFileChunks);
          audioFileChunks = [];
          const fileDurationSec = audioFileDurationSec;
          const fileName = audioFileName;

          // Add WAV header to raw PCM (16-bit, 16kHz, mono)
          function createWavBuffer(pcmBuffer) {
            const sampleRate = 16000;
            const numChannels = 1;
            const bitsPerSample = 16;
            const byteRate = sampleRate * numChannels * bitsPerSample / 8;
            const blockAlign = numChannels * bitsPerSample / 8;
            const dataSize = pcmBuffer.length;
            const header = Buffer.alloc(44);
            header.write('RIFF', 0);
            header.writeUInt32LE(36 + dataSize, 4);
            header.write('WAVE', 8);
            header.write('fmt ', 12);
            header.writeUInt32LE(16, 16);
            header.writeUInt16LE(1, 20);
            header.writeUInt16LE(numChannels, 22);
            header.writeUInt32LE(sampleRate, 24);
            header.writeUInt32LE(byteRate, 28);
            header.writeUInt16LE(blockAlign, 32);
            header.writeUInt16LE(bitsPerSample, 34);
            header.write('data', 36);
            header.writeUInt32LE(dataSize, 40);
            return Buffer.concat([header, pcmBuffer]);
          }

          const head4 = fullBuffer.slice(0, 4).toString('ascii');
          const head3 = fullBuffer.slice(0, 3).toString('ascii');
          const b0 = fullBuffer.length > 0 ? fullBuffer[0] : 0;
          const b1 = fullBuffer.length > 1 ? fullBuffer[1] : 0;
          const isRiff = head4 === 'RIFF';
          // MPEG frame sync: 11 set bits — cover common mp3 sync words
          const isMp3 = head3 === 'ID3' || (b0 === 0xff && (b1 & 0xe0) === 0xe0);
          let contentType;
          let uploadBuffer;
          if (isRiff) {
            contentType = 'audio/wav';
            uploadBuffer = fullBuffer;
          } else if (isMp3) {
            contentType = 'audio/mpeg';
            uploadBuffer = fullBuffer;
          } else {
            // Raw PCM16 @ 16kHz mono — wrap with WAV header
            contentType = 'audio/wav';
            uploadBuffer = createWavBuffer(fullBuffer);
          }
          console.log('[Audio Detect] First 4 ascii:', JSON.stringify(head4), '| hex:', fullBuffer.slice(0, 4).toString('hex'));
          console.log('[Audio Detect] size:', fullBuffer.length, '| RIFF:', isRiff, '| MP3:', isMp3, '| Content-Type:', contentType);
          if (isRiff && fullBuffer.length >= 44) {
            console.log('[Audio Detect] WAV fmt — rate:', fullBuffer.readUInt32LE(24),
              '| ch:', fullBuffer.readUInt16LE(22),
              '| bits:', fullBuffer.readUInt16LE(34));
          }
                    let fileStartMs = null;
          if (!isMp3) {
            const sr = isRiff ? fullBuffer.readUInt32LE(24) : 16000;
            const ch = isRiff ? fullBuffer.readUInt16LE(22) : 1;
            const bits = isRiff ? fullBuffer.readUInt16LE(34) : 16;
            let dataOff = 0;
            if (isRiff) {
              dataOff = 44;
              for (let i = 12; i + 8 <= fullBuffer.length;) {
                const id = fullBuffer.toString("ascii", i, i + 4);
                const sz = fullBuffer.readUInt32LE(i + 4);
                if (id === "data") { dataOff = i + 8; break; }
                i += 8 + sz + (sz % 2);
              }
            }
            const pcm = fullBuffer.subarray(dataOff);
            if (bits === 16 && ch >= 1 && pcm.length >= 2 * ch) {
              // stereo -> mono ortalama, sonra 16k'ya lineer resample
              const n = Math.floor(pcm.length / 2 / ch);
              const monoF = new Float32Array(n);
              for (let i = 0; i < n; i++) {
                let s = 0;
                for (let c = 0; c < ch; c++) s += pcm.readInt16LE((i * ch + c) * 2);
                monoF[i] = s / ch;
              }
              const outN = sr === 16000 ? n : Math.floor(n * 16000 / sr);
              const mono = Buffer.alloc(outN * 2);
              for (let i = 0; i < outN; i++) {
                const src = i * sr / 16000;
                const i0 = Math.floor(src), frac = src - i0;
                const v = monoF[i0] * (1 - frac) + (monoF[Math.min(i0 + 1, n - 1)] || 0) * frac;
                mono.writeInt16LE(Math.max(-32768, Math.min(32767, Math.round(v))), i * 2);
              }
              const fileVad = createSileroVad({ sampleRate: 16000 });
              const FRAME_BYTES = 640;
              await vadChain;
              fileStartMs = audioClockMs;
              for (let off = 0; off + 2 <= mono.length; off += FRAME_BYTES) {
                const sub = mono.subarray(off, Math.min(off + FRAME_BYTES, mono.length));
                const t = audioClockMs;
                audioClockMs += sub.length / 32;
                fileVad.processPcm16(sub);
                const p = await fileVad.flush();
                noise.pushFrame({ ...frameStatsPcm16(sub), speechProb: p }, t);
              }
            }
          }
          console.log('[Server] Audio file received — Deepgram REST (axios)');
          send({ type: 'log', message: 'Sending to Deepgram STT (' + contentType + ')...' });
          send({ type: 'stage', stage: 'stt', status: 'processing' });
          try {
            const dgApiKey = process.env.DEEPGRAM_API_KEY;
            const response = await axios.post(
              'https://api.deepgram.com/v1/listen?model=nova-3&language=en-US&punctuate=true&smart_format=true',
              uploadBuffer,
              {
                headers: {
                  Authorization: 'Token ' + dgApiKey,
                  'Content-Type': contentType,
                },
                maxBodyLength: Infinity,
                maxContentLength: Infinity,
                transformRequest: [function(data) { return data; }],
                responseType: 'json',
              }
            );
            console.log('[Deepgram File] Status:', response.status);
            console.log('[Deepgram File] Response:', JSON.stringify(response.data).substring(0, 500));
            const alt = response.data && response.data.results
              && response.data.results.channels
              && response.data.results.channels[0]
              && response.data.results.channels[0].alternatives
              && response.data.results.channels[0].alternatives[0];
            const fileTranscript = alt && alt.transcript;
            if (fileTranscript && String(fileTranscript).trim()) {
              console.log('[Deepgram] File transcript:', fileTranscript,
                fileDurationSec != null ? ('| duration: ' + fileDurationSec + 's') : '');
              send({
                type: 'stage',
                stage: 'stt',
                status: 'done',
                duration: fileDurationSec,
                filename: fileName,
              });
              send({
                type: 'audio_file_result',
                filename: fileName,
                duration: fileDurationSec,
                transcript: String(fileTranscript).trim(),
              });
              if (fileStartMs != null && alt.words && alt.words.length) {
                const offSec = fileStartMs / 1000;
                uttWords = alt.words.map(function(w) {
                  return {
                    word: w.punctuated_word || w.word,
                    start: (w.start || 0) + offSec,
                    end: (w.end || 0) + offSec,
                    confidence: (w.confidence != null ? w.confidence : null),
                  };
                });
              }
              if (typeof alt.confidence === "number") lastConfidence = alt.confidence;
              await handleTranscript(fileTranscript, null);
            } else {
              console.warn('[Deepgram] Empty transcript. alt=', JSON.stringify(alt));
              send({
                type: 'log',
                message: 'No transcript from Deepgram'
                  + (fileDurationSec != null ? (' (file ' + fileDurationSec + 's)') : ''),
              });
              send({
                type: 'stage',
                stage: 'stt',
                status: 'error',
                duration: fileDurationSec,
                filename: fileName,
              });
            }
          } catch (e) {
            console.error('[Deepgram File Error]', e.message);
            if (e.response) console.error('[Deepgram File Error] body:', JSON.stringify(e.response.data).substring(0, 400));
            send({ type: 'log', message: 'Deepgram error: ' + e.message });
            send({
              type: 'stage',
              stage: 'stt',
              status: 'error',
              duration: fileDurationSec,
              filename: fileName,
            });
          }
        }

        if (msg.type === 'stop') {
          if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) deepgramWs.close();
          clearSilenceTimer(); sessionState = 'ENDED';
        }
      } else {
        if (isReceivingAudioFile) {
          audioFileChunks.push(Buffer.from(data));
        } else {
          lastMediaAt = Date.now();
          const raw = Buffer.from(data);
          const SUB_BYTES = 640;
          for (let off = 0; off < raw.length; off += SUB_BYTES) {
            const sub = raw.subarray(off, Math.min(off + SUB_BYTES, raw.length));
            if (sub.length < 2) break;
            const t = audioClockMs;
            audioClockMs += sub.length / 32;
            const stats = frameStatsPcm16(sub);
            vadChain = vadChain.then(async function() {
              vad.processPcm16(sub);
              const p = await vad.flush();
              noise.pushFrame({ ...stats, speechProb: p}, t);
            }).catch(function(e) { console.error("[VAD] chain error:", e.message); });
          }
          if (DEEPFILTER_ENABLED && dfEnabled && deepFilter) {
            console.log('[DeepFilter] raw size:', raw.length);
            const DF_CHUNK = 640; // 320 samples @ 16 kHz PCM16 ≈ 20 ms
            if (raw.length <= DF_CHUNK) {
              deepFilter.processPcm16(raw).then(function(cleanBuf) {
                console.log('[DeepFilter] processPcm16 resolved | in:', raw.length, '| out:', cleanBuf ? cleanBuf.length : 0);
                sendAudioToDeepgram(cleanBuf);
              }).catch(function(e) {
                console.error('[DeepFilter] process error:', e.message);
                sendAudioToDeepgram(raw);
              });
            } else {
              // Large WS frames → feed DeepFilter in 640-byte slices (same as VAD framing)
              const nChunks = Math.ceil(raw.length / DF_CHUNK);
              console.log('[DeepFilter] splitting into', nChunks, 'x', DF_CHUNK, 'byte chunks');
              let chain = Promise.resolve();
              for (let off = 0; off < raw.length; off += DF_CHUNK) {
                const chunk = Buffer.from(raw.subarray(off, Math.min(off + DF_CHUNK, raw.length)));
                if (chunk.length < 2) break;
                chain = chain.then(function() {
                  return deepFilter.processPcm16(chunk).then(function(cleanBuf) {
                    console.log('[DeepFilter] processPcm16 resolved | chunk in:', chunk.length, '| out:', cleanBuf ? cleanBuf.length : 0);
                    if (cleanBuf && cleanBuf.length) sendAudioToDeepgram(cleanBuf);
                  });
                }).catch(function(e) {
                  console.error('[DeepFilter] process error (chunk):', e.message);
                  sendAudioToDeepgram(chunk);
                });
              }
            }
          } else {
            sendAudioToDeepgram(raw);
          }
        }
      }
    } catch (e) { console.error('[WS Message Error]', e.message, e.stack); }
  });

  ws.on('close', function() {
    console.log('[WS] Client disconnected');
    clearSilenceTimer();
    if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) deepgramWs.close();
  });
});

// ─── Model Comparison Dashboard ───
const TOKEN_COSTS = {
  'claude-haiku-4-5': { input: 0.80, output: 4.00 },
  'claude-sonnet-4-6': { input: 3.00, output: 15.00 },
  'claude-opus-4-6': { input: 15.00, output: 75.00 },
  'claude-opus-4-7': { input: 15.00, output: 75.00 },
  'claude-opus-4-8': { input: 15.00, output: 75.00 },
  'gemini-3.1-flash-live-preview': { input: 2.10, output: 8.50 },
  'gemini-2.0-flash': { input: 2.10, output: 8.50 },
  'gemini-2.0-flash-001': { input: 2.10, output: 8.50 },
  'gemini-3-flash-preview': { input: 2.10, output: 8.50 },
};

const COMPARE_MODEL_META = {
  'claude-haiku-4-5': { type: 'claude', label: 'Claude Haiku 4.5' },
  'claude-sonnet-4-6': { type: 'claude', label: 'Claude Sonnet 4.6' },
  'gemini-3.1-flash-live-preview': { type: 'gemini', label: 'Gemini 3.1 Flash Live' },
};

const TEST_SCENARIOS = [
  { text: "I'm fine", category: "Net cevaplar", expected: "SAFE_CONFIRMATION" },
  { text: "Yes", category: "Net cevaplar", expected: "SAFE_CONFIRMATION" },
  { text: "I need a human agent please", category: "Net cevaplar", expected: "REQUEST_HUMAN" },
  { text: "Running late, give me 15 minutes", category: "Net cevaplar", expected: "DELAYED_CHECKIN" },
  { text: "almost done just a bit more time", category: "Belirsiz", expected: "DELAYED_CHECKIN" },
  { text: "Give me like maybe 10 or 15 minutes I think", category: "Belirsiz", expected: "DELAYED_CHECKIN" },
  { text: "I'm running behind but should be done soon", category: "Belirsiz", expected: "DELAYED_CHECKIN" },
  { text: "eeee sanırım iyiyim", category: "Belirsiz", expected: "UNRECOGNISED" },
  { text: "I think I'm okay maybe", category: "Belirsiz", expected: "UNRECOGNISED" },
  { text: "ye", category: "Belirsiz", expected: "UNRECOGNISED" },
  { text: "I broke my leg I need help", category: "Distress", expected: "REQUEST_HUMAN" },
  { text: "I'm stuck and a bit scared", category: "Distress", expected: "REQUEST_HUMAN" },
  { text: "SOS help me please", category: "Distress", expected: "REQUEST_HUMAN" },
  { text: "I'm okay just a little dizzy and my head hurts", category: "Distress", expected: "REQUEST_HUMAN" },
  { text: "Everything is fine I just fell down but I'm okay I promise", category: "Distress", expected: "REQUEST_HUMAN" },
  { text: "fi... oka... fine", category: "Gürültülü", expected: "SAFE_CONFIRMATION" },
  { text: "I'm fi-- yes I'm good --static-- okay", category: "Gürültülü", expected: "SAFE_CONFIRMATION" },
  { text: "I'm fine I'm safe. I broke my leg and it hurts too much I'm not good I'm okay", category: "Çelişkili", expected: "REQUEST_HUMAN" },
  { text: "I'm fine but I need help", category: "Çelişkili", expected: "REQUEST_HUMAN" },
  { text: "Yes I'm okay but something feels wrong", category: "Çelişkili", expected: "REQUEST_HUMAN" },
  { text: "All good, actually wait no I'm not okay", category: "Çelişkili", expected: "REQUEST_HUMAN" },
  { text: "I think I'm fine but I'm a bit worried", category: "Çelişkili", expected: "REQUEST_HUMAN" },
];

function calcTokenCost(model, inputTokens, outputTokens) {
  const rates = TOKEN_COSTS[model] || { input: 0, output: 0 };
  return (inputTokens / 1e6) * rates.input + (outputTokens / 1e6) * rates.output;
}

/** Sum tokenCount for a single modality in Gemini *TokensDetails arrays. */
function sumGeminiModalityTokens(details, modality) {
  if (!Array.isArray(details) || !details.length) return null;
  const want = String(modality || '').toUpperCase();
  let sum = 0;
  for (let i = 0; i < details.length; i++) {
    const d = details[i];
    if (!d) continue;
    const m = String(d.modality || d.Modality || '').toUpperCase();
    if (m !== want) continue;
    sum += Number(d.tokenCount != null ? d.tokenCount : d.token_count) || 0;
  }
  return sum;
}

/**
 * Normalize Gemini usageMetadata for billing/compare.
 * input_tokens: TEXT modality only from promptTokensDetails (AUDIO excluded).
 * output_tokens: raw responseTokenCount (unchanged; do not filter by modality).
 */
function parseGeminiUsageMetadata(usageMetadata) {
  const u = usageMetadata || {};
  let input = Number(u.promptTokenCount) || 0;
  // Prefer responseTokenCount for Live; fall back to candidatesTokenCount for REST
  const output = Number(u.responseTokenCount) || Number(u.candidatesTokenCount) || 0;

  if (Array.isArray(u.promptTokensDetails) && u.promptTokensDetails.length) {
    console.log('[Compare] Gemini promptTokensDetails:', JSON.stringify(u.promptTokensDetails));
    // TEXT only for input — do not include AUDIO in cost
    input = sumGeminiModalityTokens(u.promptTokensDetails, 'TEXT') || 0;
  }

  return {
    input_tokens: input,
    output_tokens: output,
    total_tokens: input + output,
  };
}

function parseClaudeUsage(usage) {
  const u = usage || {};
  const input = Number(u.input_tokens) || 0;
  const output = Number(u.output_tokens) || 0;
  return {
    input_tokens: input,
    output_tokens: output,
    total_tokens: input + output,
    cache_read_input_tokens: Number(u.cache_read_input_tokens) || 0,
  };
}

function parseIntentJson(text) {
  let clean = (text || '').trim();
  if (!clean) throw new Error('Empty model response');
  clean = clean.replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/i, '').trim();
  try { return JSON.parse(clean); } catch (e) {
    const match = clean.match(/\{[\s\S]*\}/);
    if (match) {
      try { return JSON.parse(match[0]); } catch (e2) {}
    }
    throw new Error('Invalid JSON: ' + clean.slice(0, 120));
  }
}

function avgNum(arr) {
  if (!arr.length) return 0;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}

function isAnthropicOverloadedError(err) {
  if (!err) return false;
  if (err.status === 529 || err.statusCode === 529) return true;
  const type = (err.error && err.error.type) || err.type || '';
  if (String(type).toLowerCase() === 'overloaded_error') return true;
  const msg = String(err.message || '').toLowerCase();
  return msg.includes('overloaded_error') || msg.includes('overloaded');
}

/** One retry on Anthropic 529 / overloaded_error (comparison path). */
async function withAnthropicCompareRetry(fn) {
  try {
    return await fn();
  } catch (err) {
    if (!isAnthropicOverloadedError(err)) throw err;
    console.log('[Compare] Anthropic overloaded — retrying in 5s...');
    await new Promise(function(r) { setTimeout(r, 5000); });
    return await fn();
  }
}

async function classifyWithClaudeOnce(transcript, model, opts) {
  const realSttMs = opts && opts.realSttMs != null ? opts.realSttMs : null;
  const sttMs = realSttMs != null ? realSttMs : 300;
  const sttSource = realSttMs != null ? 'real' : 'estimated';

  const llmStart = Date.now();
  const result = await withAnthropicCompareRetry(function() {
    return classifyIntent(transcript, model, { skipFastPath: true });
  });
  // Prefer TTFT (first stream chunk); fall back to wall clock if no chunk seen.
  const llmMs = result._ttft_ms != null ? result._ttft_ms : (Date.now() - llmStart);
  console.log('[Compare LLM]', model, 'TTFT', llmMs + 'ms',
    result._total_ms != null ? '(total ' + result._total_ms + 'ms)' : '',
    result._usage?.cache_read_input_tokens ? '(cache hit)' : '(cache miss)');

  const usage = result._usage || { input_tokens: 0, output_tokens: 0, total_tokens: 0 };
  delete result._usage;
  delete result._ttft_ms;
  delete result._total_ms;
  const applied = applyCompareDistress(enrichCompareFromMinimal(result, transcript));

  const inputTokens = usage.input_tokens || 0;
  const outputTokens = usage.output_tokens || 0;
  const totalTokens = usage.total_tokens != null ? usage.total_tokens : (inputTokens + outputTokens);

  let ttsMs = null;
  try {
    const cacheKey = INTENT_CACHE_MAP[applied.intent];
    if (cacheKey && audioCache[cacheKey]) {
      // Production path uses pre-baked MP3; count as ~1ms cache hit, not ElevenLabs RTT.
      ttsMs = 1;
      console.log('[Compare TTS] Cache hit:', cacheKey, '→ 1ms');
    } else {
      const apiKey = process.env.ELEVENLABS_API_KEY;
      const voiceId = process.env.ELEVENLABS_VOICE_ID || 'EXAVITQu4vr4xnSDxMaL';
      if (apiKey && apiKey !== 'your_elevenlabs_key') {
        const ttsStart = Date.now();
        await axios.post(
          'https://api.elevenlabs.io/v1/text-to-speech/' + voiceId,
          { text: applied.response || 'Got it.', model_id: 'eleven_flash_v2_5', voice_settings: { stability: 0.5, similarity_boost: 0.75 } },
          { headers: { 'xi-api-key': apiKey, 'Content-Type': 'application/json', 'Accept': 'audio/mpeg' }, responseType: 'arraybuffer', timeout: 8000 }
        );
        ttsMs = Date.now() - ttsStart;
        console.log('[Compare TTS] ElevenLabs miss:', cacheKey || 'none', '→', ttsMs + 'ms');
      }
    }
  } catch (e) {}

  return {
    latency: (sttMs || 0) + llmMs + (ttsMs || 0),
    stt_ms: sttMs,
    stt_source: sttSource,
    llm_ms: llmMs,
    tts_ms: ttsMs,
    intent: applied.intent,
    confidence: applied.confidence,
    response: applied.response,
    resolved: !!applied.resolved,
    escalate: !!applied.escalate,
    delay_minutes: applied.delay_minutes ?? null,
    distress: applied.distress || null,
    distress_signals: applied.distress_signals || null,
    input_tokens: inputTokens,
    output_tokens: outputTokens,
    total_tokens: totalTokens,
  };
}

// Intent compare uses real Gemini Live WebSocket (AUDIO TTFA). REST generateContent
// remains for freeform / benchmark / explicit geminiMode:'rest' fallback.
// Each UI id maps to REST fallbacks (quota) and Live model names for compare.
const GEMINI_REST_MODEL_MAP = {
  'gemini-3.1-flash-live-preview': ['gemini-2.5-flash'],
};

// UI Live model ids → Live API model names
const GEMINI_LIVE_MODEL_MAP = {
  'gemini-3.1-flash-live-preview': 'gemini-3.1-flash-live-preview',
};

const GEMINI_INTENT_PROMPT = COMPARE_INTENT_PROMPT;

// Free tier is often ~20 RPM; serialize + space requests so parallel compare runs don't 429.
const GEMINI_MIN_INTERVAL_MS = 1000;
const GEMINI_MAX_RETRIES = 2;
let geminiQueue = Promise.resolve();
let lastGeminiCallAt = 0;

function sleepMs(ms) {
  return new Promise(function(resolve) { setTimeout(resolve, ms); });
}

function parseGeminiRetryMs(message) {
  const m = String(message || '').match(/retry in ([\d.]+)\s*s/i);
  if (m) return Math.ceil(parseFloat(m[1]) * 1000) + 500;
  return 10000;
}

function isGeminiRateLimitError(message) {
  const msg = String(message || '').toLowerCase();
  return msg.includes('quota') || msg.includes('rate limit') || msg.includes('resource_exhausted') || msg.includes('429');
}

function enqueueGeminiCall(fn) {
  const run = geminiQueue.then(async function() {
    const wait = Math.max(0, GEMINI_MIN_INTERVAL_MS - (Date.now() - lastGeminiCallAt));
    if (wait > 0) await sleepMs(wait);
    lastGeminiCallAt = Date.now();
    return fn();
  });
  geminiQueue = run.then(function() {}, function() {});
  return run;
}

function resolveGeminiRestModels(model) {
  if (GEMINI_REST_MODEL_MAP[model]) return GEMINI_REST_MODEL_MAP[model];
  if (model.indexOf('gemini-') === 0) return [model];
  return null;
}

function normalizeGeminiCompareIntent(raw) {
  const t = String(raw || '').toLowerCase().trim();
  if (t === 'safe' || t === 'safe_confirmation') return 'safe';
  if (t === 'delayed' || t === 'delayed_checkin') return 'delayed';
  if (t === 'request_human' || t === 'request human') return 'request_human';
  if (t === 'unsafe') return 'unsafe';
  if (t === 'ambiguous' || t === 'unrecognised' || t === 'unrecognized') return 'ambiguous';
  return 'ambiguous';
}

function enrichCompareFromMinimal(parsed, transcript) {
  const intentKey = normalizeGeminiCompareIntent(parsed.intent);
  const durationMatch = transcript.match(DURATION_REGEX);
  const duration = durationMatch ? durationMatch[1] + ' ' + durationMatch[2] : null;
  let distress = parsed.distress || null;
  if (intentKey === 'unsafe' && !distress) distress = 'high';

  const base = intentToResult(intentKey, duration);
  return {
    intent: base.intent,
    confidence: typeof parsed.confidence === 'number' ? parsed.confidence : base.confidence,
    response: COMPARE_RESPONSES[intentKey] || COMPARE_RESPONSES.ambiguous,
    resolved: ['safe', 'delayed'].includes(intentKey),
    escalate: ['request_human', 'unsafe'].includes(intentKey),
    delay_minutes: intentKey === 'delayed' ? (base.delay_minutes ?? 15) : null,
    distress: distress,
    distress_signals: parsed.distress_signals || (distress === 'high' ? 'unsafe' : null),
  };
}

function applyCompareDistress(result) {
  if (result.distress === 'high') {
    result.intent = 'REQUEST_HUMAN';
    result.escalate = true;
    result.resolved = false;
    result.response = COMPARE_RESPONSES.request_human;
  } else if (result.distress === 'medium' && !(result.intent === 'SAFE_CONFIRMATION' && result.confidence > 0.85)) {
    result.intent = 'REQUEST_HUMAN';
    result.escalate = true;
    result.resolved = false;
    result.response = COMPARE_RESPONSES.request_human;
  }
  return result;
}

async function classifyWithGeminiOnce(transcript, model, opts) {
  opts = opts || {};
  const apiKey = process.env.GEMINI_API_KEY;
  if (!apiKey || apiKey === 'your_gemini_key') {
    throw new Error('GEMINI_API_KEY is not set');
  }

  const candidates = resolveGeminiRestModels(model);
  if (!candidates || !candidates.length) throw new Error('Unsupported Gemini model: ' + model);

  let lastError = null;

  for (let i = 0; i < candidates.length; i++) {
    const restModel = candidates[i];
    const url = 'https://generativelanguage.googleapis.com/v1beta/models/' + restModel + ':generateContent?key=' + apiKey;

    for (let attempt = 0; attempt <= GEMINI_MAX_RETRIES; attempt++) {
      try {
        const result = await enqueueGeminiCall(async function() {
          const start = Date.now();
          const generationConfig = {
            responseMimeType: 'application/json',
            temperature: 0,
            maxOutputTokens: 100,
          };
          // 2.5/3.x think by default — disable when used as fallback.
          if (String(restModel).indexOf('2.5') !== -1 || String(restModel).indexOf('gemini-3') === 0) {
            generationConfig.thinkingConfig = { thinkingBudget: 0 };
          }
          const response = await axios.post(url, {
            contents: [{
              role: 'user',
              parts: [{ text: GEMINI_INTENT_PROMPT + '\n\nWorker said: "' + transcript + '"' }],
            }],
            generationConfig: generationConfig,
          }, {
            headers: { 'Content-Type': 'application/json' },
            timeout: 20000,
          });
          const latency = Date.now() - start;
          const data = response.data || {};
          const text = (((data.candidates || [])[0] || {}).content || {}).parts
            ? data.candidates[0].content.parts.map(function(p) { return p.text || ''; }).join('')
            : '';
          const usage = parseGeminiUsageMetadata(data.usageMetadata);
          const parsed = parseIntentJson(text);
          const mapped = applyCompareDistress(enrichCompareFromMinimal(parsed, transcript));
          console.log('[Compare] Gemini REST:', restModel, latency + 'ms', '| intent:', mapped.intent,
            '| tokens in/out/total:', usage.input_tokens + '/' + usage.output_tokens + '/' + usage.total_tokens);
          return {
            latency: latency,
            intent: mapped.intent,
            confidence: mapped.confidence,
            response: mapped.response,
            resolved: !!mapped.resolved,
            escalate: !!mapped.escalate,
            delay_minutes: mapped.delay_minutes ?? null,
            distress: mapped.distress || null,
            distress_signals: mapped.distress_signals || null,
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            total_tokens: usage.total_tokens,
            mode: 'rest',
            _rest_model: restModel,
          };
        });

        if (i > 0 || attempt > 0) {
          console.log('[Compare] Gemini REST ok:', model, '→', restModel, attempt > 0 ? '(retry ' + attempt + ')' : '');
        }
        return result;
      } catch (err) {
        const apiMsg = err.response && err.response.data && err.response.data.error && err.response.data.error.message;
        lastError = new Error(apiMsg || err.message || 'Gemini generateContent failed');
        const msg = lastError.message || '';

        if (isGeminiRateLimitError(msg) && attempt < GEMINI_MAX_RETRIES) {
          const waitMs = parseGeminiRetryMs(msg);
          console.log('[Compare] Gemini rate limit on', restModel, '— waiting', waitMs + 'ms (attempt ' + (attempt + 1) + '/' + GEMINI_MAX_RETRIES + ')');
          await sleepMs(waitMs);
          lastGeminiCallAt = 0;
          continue;
        }

        const switchModel = msg.toLowerCase().includes('not found')
          || msg.toLowerCase().includes('not supported')
          || isGeminiRateLimitError(msg);
        if (!switchModel || i === candidates.length - 1) throw lastError;
        console.log('[Compare] Gemini REST fallback', restModel, '→', candidates[i + 1], ':', msg.slice(0, 80));
        break;
      }
    }
  }

  throw lastError || new Error('Gemini generateContent failed');
}

function detectIntentFromLiveOutput(outputText, transcript) {
  const raw = String(outputText || '').trim();
  // Prefer JSON if the model somehow emits it
  const jsonMatch = raw.match(/\{[\s\S]*?"intent"[\s\S]*?\}/);
  if (jsonMatch) {
    try {
      const parsed = JSON.parse(jsonMatch[0]);
      const intentKey = normalizeGeminiCompareIntent(parsed.intent);
      return {
        intentKey: intentKey,
        distress: parsed.distress || (intentKey === 'unsafe' ? 'high' : null),
        confidence: typeof parsed.confidence === 'number' ? parsed.confidence : 0.90,
      };
    } catch (e) { /* fall through to keywords */ }
  }

  const lower = raw.toLowerCase();
  const worker = String(transcript || '').toLowerCase();

  // Ambiguous canned reply mentions "human agent" — check before request_human keywords
  if (/didn't catch|did not catch|are you safe, need more time/.test(lower)) {
    return { intentKey: 'ambiguous', distress: null, confidence: 0.90 };
  }
  if (/reset|stay safe|safety timer|got it/.test(lower)) {
    return { intentKey: 'safe', distress: null, confidence: 0.90 };
  }
  if (/extend|check-in timer|understood/.test(lower)) {
    return { intentKey: 'delayed', distress: null, confidence: 0.90 };
  }
  // Same spoken reply for request_human and unsafe — disambiguate via worker transcript
  if (/flagging|live agent|call you back/.test(lower)) {
    if (/\b(hurt|pain|fell|fall|emergenc|sos|bleeding|injur|trapped|stuck|help)\b/.test(worker)) {
      return { intentKey: 'unsafe', distress: 'high', confidence: 0.90 };
    }
    return { intentKey: 'request_human', distress: null, confidence: 0.90 };
  }

  // Fallback: classify from worker transcript if speech was empty/unclear
  if (/\b(hurt|pain|fell|fall|emergenc|sos|bleeding|injur|trapped|stuck)\b/.test(worker)) {
    return { intentKey: 'unsafe', distress: 'high', confidence: 0.85 };
  }
  if (/\b(agent|human|representative|operator)\b/.test(worker)) {
    return { intentKey: 'request_human', distress: null, confidence: 0.85 };
  }
  if (/\b(more\s+time|need\s+time|extend|minute|minutes|later|running late)\b/.test(worker)) {
    return { intentKey: 'delayed', distress: null, confidence: 0.85 };
  }
  if (/\b(fine|safe|okay|ok|good|yes|yeah)\b/.test(worker)) {
    return { intentKey: 'safe', distress: null, confidence: 0.85 };
  }
  return { intentKey: 'ambiguous', distress: null, confidence: 0.90 };
}

async function audioToPcm16kMono(inputBuffer) {
  return new Promise(function(resolve, reject) {
    const ffmpeg = spawn('ffmpeg', [
      '-hide_banner', '-loglevel', 'error',
      '-i', 'pipe:0',
      '-ar', '16000',
      '-ac', '1',
      '-f', 's16le',
      'pipe:1'
    ]);
    const chunks = [];
    ffmpeg.stdout.on('data', function(chunk) { chunks.push(chunk); });
    ffmpeg.stdout.on('end', function() { resolve(Buffer.concat(chunks)); });
    let errText = '';
    ffmpeg.stderr.on('data', function(d) { errText += d.toString(); });
    ffmpeg.on('error', function(e) {
      reject(new Error('ffmpeg failed: ' + e.message));
    });
    ffmpeg.on('close', function(code) {
      if (code !== 0 && chunks.length === 0) {
        reject(new Error('ffmpeg exit ' + code + (errText ? ': ' + errText.slice(0, 200) : '')));
      }
    });
    ffmpeg.stdin.write(inputBuffer);
    ffmpeg.stdin.end();
  });
}

/** Probe audio duration (seconds) via ffmpeg stderr Duration: HH:MM:SS.xx */
async function getAudioDurationSec(buf) {
  return new Promise(function(resolve) {
    const ff = spawn('ffmpeg', ['-hide_banner', '-i', 'pipe:0', '-f', 'null', '-']);
    let stderr = '';
    ff.stderr.on('data', function(d) { stderr += d.toString(); });
    ff.on('error', function() { resolve(null); });
    ff.on('close', function() {
      const m = stderr.match(/Duration:\s*(\d+):(\d+):(\d+\.?\d*)/);
      if (m) {
        resolve(parseFloat(m[1]) * 3600 + parseFloat(m[2]) * 60 + parseFloat(m[3]));
      } else {
        resolve(null);
      }
    });
    try {
      ff.stdin.write(buf);
      ff.stdin.end();
    } catch (e) {
      resolve(null);
    }
  });
}

/**
 * Real Gemini Live WebSocket compare.
 * - Text path: transcript → clientContent → AUDIO out, latency = TTFA
 * - Audio path (opts.audioBuffer | opts.audioPcm): wait setupComplete, then PCM
 *   chunks (8192 bytes, 20ms gap) with mimeType audio/pcm;rate=16000, then audioStreamEnd
 * - opts.sttOnly: resolve on first inputTranscription (do not wait for turnComplete / audio out)
 */
async function classifyWithGeminiLive(transcript, model, opts) {
  opts = opts || {};
  const audioBuffer = opts.audioBuffer || opts.audioPcm || null;
  const sttOnly = !!opts.sttOnly;
  const apiKey = process.env.GEMINI_API_KEY;
  if (!apiKey || apiKey === 'your_gemini_key') {
    throw new Error('GEMINI_API_KEY is not set');
  }
  const liveModel = GEMINI_LIVE_MODEL_MAP[model] || model;
  const geminiUrl = 'wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=' + apiKey;

  console.log('[Gemini Live Compare] Connecting', liveModel,
    sttOnly ? '| STT-only' : '',
    audioBuffer ? ('| audioBuffer PCM ' + audioBuffer.length + ' bytes') : ('| transcript: ' + String(transcript || '').slice(0, 80)));

  return new Promise(function(resolve, reject) {
    const ws = new WebSocket(geminiUrl);
    let setupDone = false;
    let turnStartTime = null;
    let audioSendStart = null;
    let firstAudioMs = null;
    let firstInputTranscriptAt = null;
    let outputText = '';
    let inputTranscript = '';
    let promptTokens = 0;
    let responseTokens = 0;
    let settled = false;
    const timeoutMs = audioBuffer ? 25000 : 15000;

    const timeout = setTimeout(function() {
      if (settled) return;
      settled = true;
      try { ws.close(); } catch (e) {}
      console.error('[Gemini Live Compare] Timeout (' + (timeoutMs / 1000) + 's)');
      reject(new Error('Gemini Live timeout (' + (timeoutMs / 1000) + 's)'));
    }, timeoutMs);

    function finishOk(payload) {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      try { ws.close(); } catch (e) {}
      resolve(payload);
    }

    function finishErr(err) {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      try { ws.close(); } catch (e) {}
      reject(err);
    }

    // PCM16 mono 16kHz — same mimeType as server_gemini.js
    const PCM_MIME = 'audio/pcm;rate=16000';

    function sendPcmChunk(pcmSlice) {
      ws.send(JSON.stringify({
        realtimeInput: {
          audio: {
            data: pcmSlice.toString('base64'),
            mimeType: PCM_MIME
          }
        }
      }));
    }

    async function sendAudioBuffer(pcm) {
      if (!setupDone) {
        throw new Error('sendAudioBuffer called before setupComplete');
      }
      // STT clock starts immediately before any PCM leaves the socket
      audioSendStart = Date.now();
      if (!Buffer.isBuffer(pcm)) {
        pcm = Buffer.from(pcm);
      }
      const CHUNK_SIZE = 8192;
      const totalChunks = Math.ceil(pcm.length / CHUNK_SIZE) || 1;
      console.log('[Gemini Live Compare] setupComplete OK — sending PCM audio',
        '| audioSendStart set',
        '| bytes:', pcm.length,
        '| chunks:', totalChunks,
        '| chunkSize:', CHUNK_SIZE,
        '| mimeType:', PCM_MIME,
        sttOnly ? '| sttOnly' : '');
      for (let offset = 0; offset < pcm.length; offset += CHUNK_SIZE) {
        if (settled || ws.readyState !== WebSocket.OPEN) return;
        const slice = pcm.subarray(offset, Math.min(offset + CHUNK_SIZE, pcm.length));
        sendPcmChunk(slice);
        if (offset + CHUNK_SIZE < pcm.length) {
          await new Promise(function(r) { setTimeout(r, 20); });
        }
      }
      if (settled || ws.readyState !== WebSocket.OPEN) return;
      turnStartTime = Date.now();
      ws.send(JSON.stringify({ realtimeInput: { audioStreamEnd: true } }));
      console.log('[Gemini Live Compare] Audio sent (' + pcm.length + ' bytes) — audioStreamEnd');
    }

    ws.on('open', function() {
      // VAD config mirrors server_gemini.js so Live knows how to segment audio
      const setupMsg = {
        setup: {
          model: 'models/' + liveModel,
          generation_config: {
            response_modalities: ['AUDIO'],
            speech_config: {
              voice_config: {
                prebuilt_voice_config: { voice_name: 'Charon' }
              }
            }
          },
          realtime_input_config: {
            automatic_activity_detection: {
              disabled: false,
              end_of_speech_sensitivity: 'END_SENSITIVITY_LOW',
              start_of_speech_sensitivity: 'START_SENSITIVITY_LOW',
              silence_duration_ms: 1500,
              prefix_padding_ms: 200,
            }
          },
          input_audio_transcription: {},
          output_audio_transcription: {},
          system_instruction: {
            parts: [{ text: COMPARE_LIVE_PROMPT }]
          }
        }
      };
      console.log('[Gemini Live Compare] WS open — sending setup (VAD enabled, audio waits for setupComplete)');
      console.log('[Gemini Live Compare] setup.realtime_input_config:',
        JSON.stringify(setupMsg.setup.realtime_input_config));
      ws.send(JSON.stringify(setupMsg));
    });

    ws.on('message', function(data) {
      try {
        if (settled) return;
        const raw = Buffer.isBuffer(data) ? data.toString('utf8') : data.toString();
        const msg = JSON.parse(raw);
        console.log('[Gemini Live Compare RAW]', JSON.stringify(msg).substring(0, 200));

        if ((msg.setupComplete || msg.setup_complete) && !setupDone) {
          setupDone = true;
          if (audioBuffer) {
            sendAudioBuffer(audioBuffer).catch(finishErr);
          } else {
            turnStartTime = Date.now();
            console.log('[Gemini Live Compare] Setup complete — sending transcript');
            ws.send(JSON.stringify({
              client_content: {
                turns: [{
                  role: 'user',
                  parts: [{ text: 'Worker said: "' + transcript + '"' }]
                }],
                turn_complete: true
              }
            }));
          }
          return;
        }

        if (msg.error) {
          console.error('[Gemini Live Compare] API error:', JSON.stringify(msg.error).substring(0, 300));
        }

        const serverContent = msg.serverContent || msg.server_content || null;
        const inputTxObj = serverContent && (serverContent.inputTranscription || serverContent.input_transcription);
        const inputTxText = inputTxObj && inputTxObj.text;

        // Resolve STT as soon as inputTranscription arrives — never wait for turnComplete
        if (inputTxText) {
          inputTranscript += inputTxText;
          if (firstInputTranscriptAt === null) {
            firstInputTranscriptAt = Date.now();
            console.log('[Gemini Live Compare] inputTranscription start:', String(inputTxText).slice(0, 80));
          }
          if (sttOnly) {
            if (!audioSendStart || firstInputTranscriptAt == null) return;
            const sttMs = Math.max(0, firstInputTranscriptAt - audioSendStart);
            const tx = inputTranscript.trim();
            console.log('[Gemini Live Compare] STT-only resolve on inputTranscription:', sttMs + 'ms',
              '| firstInputTranscriptAt - audioSendStart');
            clearTimeout(timeout);
            try { ws.close(); } catch (e) {}
            if (!settled) {
              settled = true;
              resolve({
                transcript: tx,
                sttMs: sttMs,
                stt_ms: sttMs,
                stt_source: 'gemini_live',
                stt_engine: 'gemini',
                inputTranscript: tx,
                input_transcript: tx,
                latency: sttMs,
                ttfa: null,
                llm_ms: null,
                mode: 'stt_only',
                intent: null,
                confidence: null,
                response: '',
                input_tokens: 0,
                output_tokens: 0,
                total_tokens: 0,
              });
            }
            return; // ignore subsequent messages (turnComplete / audio)
          }
        }

        if (sttOnly) return; // STT-only: ignore turnComplete / generationComplete / audio out

        function markFirstAudio(reason) {
          if (firstAudioMs === null && turnStartTime) {
            firstAudioMs = Date.now() - turnStartTime;
            console.log('[Gemini Live Compare] TTFA:', firstAudioMs + 'ms (' + reason + ')');
          }
        }

        if (serverContent && serverContent.modelTurn && serverContent.modelTurn.parts) {
          for (let i = 0; i < serverContent.modelTurn.parts.length; i++) {
            const part = serverContent.modelTurn.parts[i];
            const inline = part.inlineData || part.inline_data;
            const mime = inline && (inline.mimeType || inline.mime_type);
            if (inline && mime && String(mime).indexOf('audio/') === 0) {
              markFirstAudio('modelTurn.audio');
            }
            if (part.text) outputText += part.text;
          }
        }

        // First spoken-word transcription is a solid TTFA proxy if audio chunks aren't parsed
        const outTx = serverContent && (serverContent.outputTranscription || serverContent.output_transcription);
        if (outTx && outTx.text) {
          markFirstAudio('outputTranscription');
          outputText += outTx.text;
        }

        if (serverContent && (serverContent.generationComplete || serverContent.generation_complete)) {
          markFirstAudio('generationComplete');
        }

        if (msg.usageMetadata) {
          const usage = parseGeminiUsageMetadata(msg.usageMetadata);
          promptTokens = usage.input_tokens;
          responseTokens = usage.output_tokens;
          console.log('[Gemini Live Compare] usage in(TEXT)/out(responseTokenCount):',
            promptTokens + '/' + responseTokens,
            '| raw promptTokenCount:', msg.usageMetadata.promptTokenCount,
            '| raw responseTokenCount:', msg.usageMetadata.responseTokenCount);
        }

        if (serverContent && (serverContent.turnComplete || serverContent.turn_complete)) {
          const workerText = (inputTranscript || transcript || '').trim();
          const detected = detectIntentFromLiveOutput(outputText, workerText);
          const mapped = applyCompareDistress(enrichCompareFromMinimal({
            intent: detected.intentKey,
            confidence: detected.confidence != null ? detected.confidence : 0.90,
            distress: detected.distress,
          }, workerText));
          // Never use full turnComplete duration as TTFA — that inflates long replies (~4–5s)
          const ttfa = firstAudioMs != null ? firstAudioMs : null;
          if (ttfa == null) {
            console.warn('[Gemini Live Compare] No TTFA mark before turnComplete — llm_ms left null');
          }
          let sttMs = null;
          if (audioBuffer && audioSendStart) {
            const sttEnd = firstInputTranscriptAt || Date.now();
            sttMs = Math.max(0, sttEnd - audioSendStart);
          }
          const totalLatency = (sttMs || 0) + (ttfa || 0);

          console.log('[Gemini Live Compare] Done | TTFA:', ttfa != null ? ttfa + 'ms' : 'n/a',
            sttMs != null ? ('| STT:', sttMs + 'ms') : '',
            '| turnComplete elapsed:', turnStartTime ? (Date.now() - turnStartTime) + 'ms (not used as TTFA)' : 'n/a',
            '| inputTranscript:', workerText.slice(0, 100),
            '| intent:', mapped.intent);

          finishOk({
            latency: totalLatency,
            ttfa: ttfa,
            llm_ms: ttfa,
            stt_ms: sttMs,
            stt_source: audioBuffer ? 'gemini_live' : null,
            stt_engine: audioBuffer ? 'gemini' : null,
            intent: mapped.intent,
            confidence: mapped.confidence,
            response: mapped.response || outputText.trim(),
            resolved: !!mapped.resolved,
            escalate: !!mapped.escalate,
            delay_minutes: mapped.delay_minutes ?? null,
            distress: mapped.distress || null,
            distress_signals: mapped.distress_signals || null,
            input_tokens: promptTokens,
            output_tokens: responseTokens,
            total_tokens: promptTokens + responseTokens,
            mode: audioBuffer ? 'live_audio' : 'live_text',
            output_text: outputText.trim(),
            inputTranscript: inputTranscript.trim(),
            input_transcript: inputTranscript.trim(),
            transcript: workerText,
          });
        }
      } catch (e) {
        console.error('[Gemini Live Compare] Parse error:', e.message);
      }
    });

    ws.on('error', function(e) {
      console.error('[Gemini Live Compare] WS error:', e.message);
      finishErr(e);
    });

    ws.on('close', function(code, reason) {
      const reasonStr = reason ? reason.toString() : '';
      console.log('[Gemini Live Compare] Closed', code, reasonStr);
      clearTimeout(timeout);
      if (!settled) {
        finishErr(new Error(
          sttOnly
            ? ('Gemini Live STT closed before inputTranscription (code=' + code + (reasonStr ? ', reason=' + reasonStr : '') + ')')
            : ('Gemini Live connection closed before turnComplete (code=' + code + (reasonStr ? ', reason=' + reasonStr : '') + ')')
        ));
      }
    });
  });
}

async function classifyOnce(transcript, model, opts) {
  opts = opts || {};
  if (model.indexOf('claude-') === 0) return classifyWithClaudeOnce(transcript, model, opts);
  if (resolveGeminiRestModels(model) || GEMINI_LIVE_MODEL_MAP[model]) {
    if (opts.geminiMode === 'rest') return classifyWithGeminiOnce(transcript, model, opts);
    console.log('[Compare] Gemini path: Live WebSocket', model, (opts.audioBuffer || opts.audioPcm) ? '(audio)' : '(text)');
    return classifyWithGeminiLive(transcript, model, opts);
  }
  throw new Error('Unsupported model: ' + model);
}

function normalizeClientSttMs(value) {
  const n = typeof value === 'string' ? parseFloat(value) : value;
  if (!Number.isFinite(n) || n < 0) return null;
  return Math.round(n);
}

async function runModelComparison(transcript, model, runs, expected, opts) {
  const meta = COMPARE_MODEL_META[model] || { label: model, type: 'unknown' };
  const runRows = [];
  const classifyOpts = opts || {};

  for (let i = 0; i < runs; i++) {
    try {
      const r = await classifyOnce(transcript, model, classifyOpts);
      const cost = calcTokenCost(model, r.input_tokens, r.output_tokens);
      const isGemini = String(model).indexOf('gemini') === 0;
      let sttMs = null;
      let llmMs = null;
      let ttsMs = null;
      let totalLatency = r.latency;
      let sttSource = null;

      if (isGemini) {
        // STT = Deepgram (opts.realSttMs) when audio→text path; Gemini = text→audio TTFA
        const deepgramStt = classifyOpts.realSttMs != null ? classifyOpts.realSttMs : null;
        sttMs = deepgramStt != null ? deepgramStt : (r.stt_ms != null ? r.stt_ms : null);
        sttSource = deepgramStt != null
          ? (classifyOpts.sttSource || 'deepgram')
          : (r.stt_source || (sttMs != null ? 'gemini_live' : null));
        llmMs = r.ttfa != null ? r.ttfa : (r.llm_ms != null ? r.llm_ms : null);
        totalLatency = (sttMs || 0) + (llmMs || 0);
        console.log('[Compare Gemini]', model,
          '| STT:', sttMs != null ? sttMs + 'ms' : 'n/a',
          '| Gemini TTFA:', llmMs != null ? llmMs + 'ms' : 'n/a',
          '| Total:', totalLatency + 'ms');
      } else {
        sttMs = r.stt_ms ?? null;
        sttSource = r.stt_source || null;
        llmMs = r.llm_ms != null ? r.llm_ms : null;
        ttsMs = r.tts_ms ?? null;
        totalLatency = sttMs != null
          ? (sttMs || 0) + (llmMs || 0) + (ttsMs || 0)
          : ((llmMs || 0) + (ttsMs || 0) || r.latency);
      }

      runRows.push({
        run: i + 1,
        ok: true,
        latency: totalLatency,
        stt_ms: sttMs,
        stt_source: sttSource,
        stt_engine: classifyOpts.sttEngine
          || r.stt_engine
          || (sttSource === 'deepgram' ? 'deepgram' : (isGemini && sttMs != null ? 'gemini' : null)),
        llm_ms: llmMs,
        gemini_ms: isGemini ? llmMs : undefined,
        tts_ms: ttsMs,
        intent: r.intent,
        confidence: r.confidence,
        response: r.response,
        resolved: r.resolved,
        escalate: r.escalate,
        delay_minutes: r.delay_minutes,
        distress: r.distress,
        distress_signals: r.distress_signals,
        input_tokens: r.input_tokens,
        output_tokens: r.output_tokens,
        total_tokens: r.total_tokens != null ? r.total_tokens : ((r.input_tokens || 0) + (r.output_tokens || 0)),
        cost: cost,
        mode: r.mode || (isGemini ? 'live_audio' : null),
        transcript: r.transcript || r.input_transcript || r.inputTranscript || null,
        input_transcript: r.input_transcript || r.inputTranscript || null,
      });
    } catch (err) {
      runRows.push({
        run: i + 1,
        ok: false,
        error: err.message || String(err),
        latency: 0,
      });
    }
    if (i < runs - 1) {
      await new Promise(function(r) { setTimeout(r, 3000); });
    }
  }

  const okRuns = runRows.filter(function(r) { return r.ok; });
  if (!okRuns.length) {
    return {
      ok: false,
      model: model,
      label: meta.label,
      error: runRows[runRows.length - 1].error || 'All runs failed',
      runs: runRows,
    };
  }

  const lastOk = okRuns[okRuns.length - 1];
  const latencies = okRuns.map(function(r) { return r.latency; });
  const avgLatencies = okRuns.slice(0, 3).map(function(r) { return r.latency; });
  const confidences = okRuns.map(function(r) { return r.confidence; });
  const inputTokens = okRuns.map(function(r) { return r.input_tokens; });
  const outputTokens = okRuns.map(function(r) { return r.output_tokens; });
  const totalTokens = okRuns.map(function(r) {
    return r.total_tokens != null ? r.total_tokens : ((r.input_tokens || 0) + (r.output_tokens || 0));
  });
  const costs = okRuns.map(function(r) { return r.cost; });
  const sttTimes = okRuns.map(function(r) { return r.stt_ms; }).filter(function(v) { return v !== null && v !== undefined; });
  const llmTimes = okRuns.map(function(r) { return r.llm_ms; }).filter(function(v) { return v !== null && v !== undefined; });
  const ttsTimes = okRuns.map(function(r) { return r.tts_ms; }).filter(function(v) { return v !== null && v !== undefined; });
  const isGemini = String(model).indexOf('gemini') === 0;
  const avgInput = avgNum(inputTokens);
  const avgOutput = avgNum(outputTokens);
  const avgTotal = avgNum(totalTokens);

  return {
    ok: true,
    model: model,
    label: meta.label,
    intent: lastOk.intent,
    avg_confidence: avgNum(confidences),
    avg_latency: Math.round(avgNum(avgLatencies)),
    avg_stt_ms: sttTimes.length ? Math.round(avgNum(sttTimes)) : null,
    stt_source: lastOk.stt_source || null,
    avg_llm_ms: llmTimes.length ? Math.round(avgNum(llmTimes)) : (isGemini ? lastOk.llm_ms : null),
    avg_tts_ms: isGemini ? null : (ttsTimes.length ? Math.round(avgNum(ttsTimes)) : null),
    min_latency: Math.min.apply(null, latencies),
    max_latency: Math.max.apply(null, latencies),
    avg_input_tokens: Math.round(avgInput),
    avg_output_tokens: Math.round(avgOutput),
    avg_tokens: Math.round(avgTotal),
    est_cost: avgNum(costs),
    distress: lastOk.distress,
    distress_signals: lastOk.distress_signals,
    correct: expected ? lastOk.intent === expected : null,
    success_runs: okRuns.length,
    total_runs: runs,
    mode: lastOk.mode || (isGemini ? 'live_audio' : null),
    runs: runRows,
  };
}

app.get('/compare-scenarios', function(req, res) {
  res.json({ scenarios: TEST_SCENARIOS });
});

app.post('/transcribe', express.raw({ type: ['audio/*', 'application/octet-stream'], limit: '15mb' }), async function(req, res) {
  try {
    const dgApiKey = process.env.DEEPGRAM_API_KEY;
    if (!dgApiKey) return res.status(500).json({ error: 'DEEPGRAM_API_KEY is not set' });
    if (!req.body || !req.body.length) return res.status(400).json({ error: 'audio body required' });
    const contentType = req.headers['content-type'] || 'audio/wav';
    const response = await axios.post(
      'https://api.deepgram.com/v1/listen?model=nova-2&language=en-US&punctuate=true&smart_format=true',
      req.body,
      { headers: { Authorization: 'Token ' + dgApiKey, 'Content-Type': contentType }, maxBodyLength: Infinity }
    );
    const transcript = response.data?.results?.channels?.[0]?.alternatives?.[0]?.transcript || '';
    res.json({ transcript: transcript });
  } catch (err) {
    res.status(500).json({ error: err.message || 'Transcription failed' });
  }
});

function createCompareWavBuffer(pcmBuffer) {
  const sampleRate = 16000;
  const numChannels = 1;
  const bitsPerSample = 16;
  const byteRate = sampleRate * numChannels * bitsPerSample / 8;
  const blockAlign = numChannels * bitsPerSample / 8;
  const dataSize = pcmBuffer.length;
  const header = Buffer.alloc(44);
  header.write('RIFF', 0);
  header.writeUInt32LE(36 + dataSize, 4);
  header.write('WAVE', 8);
  header.write('fmt ', 12);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20);
  header.writeUInt16LE(numChannels, 22);
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(byteRate, 28);
  header.writeUInt16LE(blockAlign, 32);
  header.writeUInt16LE(bitsPerSample, 34);
  header.write('data', 36);
  header.writeUInt32LE(dataSize, 40);
  return Buffer.concat([header, pcmBuffer]);
}

/** Shared Deepgram Nova-3 REST STT for comparison audio (same logic as /compare-audio-stt). */
async function transcribeCompareAudioDeepgram(buf, contentTypeHint) {
  const dgApiKey = process.env.DEEPGRAM_API_KEY;
  if (!dgApiKey || dgApiKey === 'your_deepgram_key') {
    throw new Error('DEEPGRAM_API_KEY is not set');
  }
  if (!buf || !buf.length) {
    throw new Error('audio body required');
  }

  const head4 = buf.slice(0, 4).toString('ascii');
  const head3 = buf.slice(0, 3).toString('ascii');
  const b0 = buf.length > 0 ? buf[0] : 0;
  const b1 = buf.length > 1 ? buf[1] : 0;
  const isRiff = head4 === 'RIFF';
  const isMp3 = head3 === 'ID3' || (b0 === 0xff && (b1 & 0xe0) === 0xe0);
  // EBML / WebM / Matroska
  const isWebm = (b0 === 0x1a && b1 === 0x45) ||
    (contentTypeHint && String(contentTypeHint).toLowerCase().indexOf('webm') !== -1);
  const isOgg = head4 === 'OggS' ||
    (contentTypeHint && String(contentTypeHint).toLowerCase().indexOf('ogg') !== -1);

  let audioBuffer = buf;
  let dgContentType = 'audio/wav';

  // Normalize tricky containers to WAV via ffmpeg so Deepgram doesn't return empty
  if (isWebm || isOgg || (!isRiff && !isMp3)) {
    try {
      console.log('[Compare Audio STT] Converting to PCM/WAV via ffmpeg...',
        isWebm ? 'webm' : isOgg ? 'ogg' : (contentTypeHint || 'unknown'), buf.length, 'bytes');
      const pcm = await audioToPcm16kMono(buf);
      if (pcm && pcm.length) {
        audioBuffer = createCompareWavBuffer(pcm);
        dgContentType = 'audio/wav';
      }
    } catch (convErr) {
      console.warn('[Compare Audio STT] ffmpeg convert failed, sending raw:', convErr.message);
      if (isRiff) dgContentType = 'audio/wav';
      else if (isMp3) dgContentType = 'audio/mpeg';
      else if (isWebm) dgContentType = 'audio/webm';
      else if (isOgg) dgContentType = 'audio/ogg';
      else if (contentTypeHint) dgContentType = String(contentTypeHint).split(';')[0] || 'audio/wav';
    }
  } else if (isRiff) {
    dgContentType = 'audio/wav';
  } else if (isMp3) {
    dgContentType = 'audio/mpeg';
  }

  const sttStart = Date.now();
  const response = await axios.post(
    'https://api.deepgram.com/v1/listen?model=nova-3&language=en-US&punctuate=true&smart_format=true',
    audioBuffer,
    {
      headers: { Authorization: 'Token ' + dgApiKey, 'Content-Type': dgContentType },
      maxBodyLength: Infinity,
      maxContentLength: Infinity,
      transformRequest: [function(data) { return data; }],
      responseType: 'json',
      timeout: 60000,
    }
  );
  const sttMs = Date.now() - sttStart;
  const alt = (((response.data || {}).results || {}).channels || [])[0] || {};
  const best = (alt.alternatives || [])[0] || {};
  const transcript = best.transcript || '';
  const confidence = typeof best.confidence === 'number' ? best.confidence : 0;

  console.log('[Compare Audio STT]', sttMs + 'ms |', dgContentType, '|', JSON.stringify(transcript).slice(0, 120));
  return { transcript: transcript, sttMs: sttMs, confidence: confidence, engine: 'deepgram' };
}

/** Deepgram REST STT for per-model comparison audio uploads. Returns transcript + sttMs. */
app.post('/compare-audio-stt', express.raw({ type: '*/*', limit: '50mb' }), async function(req, res) {
  try {
    if (!req.body || !req.body.length) {
      return res.status(400).json({ error: 'audio body required' });
    }
    const buf = Buffer.isBuffer(req.body) ? req.body : Buffer.from(req.body);
    const durationSec = await getAudioDurationSec(buf);
    const deepgramCost = durationSec != null ? (durationSec / 60) * 0.0043 : null;
    console.log('[Compare Audio STT] duration_sec:', durationSec, '| deepgram_cost:', deepgramCost);

    const result = await transcribeCompareAudioDeepgram(buf, req.headers['content-type']);
    res.json({
      transcript: result.transcript,
      sttMs: result.sttMs,
      confidence: result.confidence,
      engine: 'deepgram',
      duration_sec: durationSec,
      deepgram_cost: deepgramCost,
      // aliases for older frontend fields
      duration: durationSec,
    });
  } catch (e) {
    const apiMsg = e.response && e.response.data && (e.response.data.err_msg || e.response.data.error);
    console.error('[Compare Audio STT] Error:', e.message, apiMsg || '');
    res.status(500).json({ error: (apiMsg && String(apiMsg)) || e.message || 'STT failed' });
  }
});

/**
 * Gemini Live STT only (inputTranscription). No Deepgram.
 * Query: ?model=gemini-3.1-flash-live-preview
 */
app.post('/compare-gemini-stt', express.raw({ type: '*/*', limit: '50mb' }), async function(req, res) {
  try {
    const model = (req.query && req.query.model) || req.headers['x-compare-model'] || 'gemini-3.1-flash-live-preview';
    if (!req.body || !req.body.length) {
      return res.status(400).json({ error: 'audio body required' });
    }
    const buf = Buffer.isBuffer(req.body) ? req.body : Buffer.from(req.body);

    // Prefer ffmpeg duration; fall back to client X-Audio-Duration header
    let durationSec = await getAudioDurationSec(buf);
    if (durationSec == null) {
      const durationHeader = req.headers['x-audio-duration'];
      if (durationHeader != null && Number.isFinite(parseFloat(durationHeader))) {
        durationSec = parseFloat(durationHeader);
      }
    }
    const deepgramCost = durationSec != null ? (durationSec / 60) * 0.0043 : null;

    console.log('[Compare Gemini STT] Converting to PCM16k...', buf.length, 'bytes | model:', model,
      '| duration_sec:', durationSec, '| deepgram_cost:', deepgramCost);
    const pcm = await audioToPcm16kMono(buf);
    if (!pcm || !pcm.length) {
      return res.status(400).json({ error: 'Audio conversion produced empty PCM' });
    }
    const r = await classifyWithGeminiLive(null, model, { audioBuffer: pcm, sttOnly: true });
    const transcript = r.inputTranscript || r.input_transcript || r.transcript || '';
    console.log('[Compare Gemini STT]', (r.stt_ms || 0) + 'ms |', JSON.stringify(transcript).slice(0, 120));
    res.json({
      transcript: transcript,
      sttMs: r.stt_ms != null ? r.stt_ms : null,
      confidence: null,
      engine: 'gemini',
      duration_sec: durationSec,
      deepgram_cost: deepgramCost,
      duration: durationSec,
    });
  } catch (e) {
    console.error('[Compare Gemini STT] Error:', e.message);
    res.status(500).json({ error: e.message || 'Gemini STT failed' });
  }
});

/**
 * Gemini Live compare with audio file:
 * Deepgram Nova-3 STT → transcript text → Gemini Live (text in, audio out TTFA).
 * Query: ?model=gemini-3.1-flash-live-preview
 */
app.post('/compare-gemini-audio', express.raw({ type: '*/*', limit: '50mb' }), async function(req, res) {
  try {
    const model = (req.query && req.query.model) || req.headers['x-compare-model'] || 'gemini-3.1-flash-live-preview';
    if (!COMPARE_MODEL_META[model] && String(model).indexOf('gemini-') !== 0) {
      return res.status(400).json({ error: 'Unsupported Gemini model: ' + model });
    }
    if (!req.body || !req.body.length) {
      return res.status(400).json({ error: 'audio body required' });
    }
    const buf = Buffer.isBuffer(req.body) ? req.body : Buffer.from(req.body);

    console.log('[Compare Gemini Audio] Deepgram STT first...', buf.length, 'bytes | model:', model);
    const stt = await transcribeCompareAudioDeepgram(buf, req.headers['content-type']);
    const transcript = (stt.transcript || '').trim();
    if (!transcript) {
      return res.status(400).json({ error: 'Deepgram returned empty transcript' });
    }
    const sttMs = stt.sttMs;

    console.log('[Compare Gemini Audio] STT (Deepgram):', sttMs + 'ms — sending transcript to Gemini Live (text)');
    const r = await classifyWithGeminiLive(transcript, model, {});
    const geminiMs = r.ttfa != null ? r.ttfa : (r.llm_ms != null ? r.llm_ms : r.latency);
    const totalLatency = (sttMs || 0) + (geminiMs || 0);
    console.log('[Compare Gemini Audio] Gemini TTFA:', geminiMs + 'ms | Total:', totalLatency + 'ms');

    const cost = calcTokenCost(model, r.input_tokens, r.output_tokens);
    const meta = COMPARE_MODEL_META[model] || { label: model, type: 'gemini' };

    res.json({
      ok: true,
      model: model,
      label: meta.label,
      intent: r.intent,
      avg_confidence: r.confidence,
      avg_latency: totalLatency,
      avg_stt_ms: sttMs,
      avg_llm_ms: geminiMs,
      avg_tts_ms: null,
      min_latency: totalLatency,
      max_latency: totalLatency,
      avg_input_tokens: r.input_tokens || 0,
      avg_output_tokens: r.output_tokens || 0,
      avg_tokens: (r.input_tokens || 0) + (r.output_tokens || 0),
      est_cost: cost,
      distress: r.distress,
      distress_signals: r.distress_signals,
      correct: null,
      success_runs: 1,
      total_runs: 1,
      mode: 'deepgram_stt_gemini_live_text',
      stt_source: 'deepgram',
      stt_engine: 'deepgram',
      stt_ms: sttMs,
      gemini_ms: geminiMs,
      llm_ms: geminiMs,
      transcript: transcript,
      input_transcript: transcript,
      inputTranscript: transcript,
      stt_confidence: stt.confidence,
      runs: [{
        run: 1,
        ok: true,
        latency: totalLatency,
        stt_ms: sttMs,
        stt_source: 'deepgram',
        stt_engine: 'deepgram',
        llm_ms: geminiMs,
        gemini_ms: geminiMs,
        tts_ms: null,
        intent: r.intent,
        confidence: r.confidence,
        response: r.response,
        resolved: r.resolved,
        escalate: r.escalate,
        delay_minutes: r.delay_minutes,
        distress: r.distress,
        distress_signals: r.distress_signals,
        input_tokens: r.input_tokens,
        output_tokens: r.output_tokens,
        total_tokens: (r.input_tokens || 0) + (r.output_tokens || 0),
        cost: cost,
        mode: 'deepgram_stt_gemini_live_text',
        transcript: transcript,
        input_transcript: transcript,
      }],
    });
  } catch (e) {
    console.error('[Compare Gemini Audio] Error:', e.message);
    res.status(500).json({ ok: false, error: e.message || 'Gemini audio compare failed' });
  }
});

// Single-run classify for public/benchmark.html (one model per request, no aggregation).
app.post('/benchmark-classify', async function(req, res) {
  const transcript = (req.body && req.body.transcript || '').trim();
  const models = (req.body && req.body.models) || [];

  if (!transcript) return res.status(400).json({ error: 'transcript is required' });
  if (!Array.isArray(models) || models.length === 0) {
    return res.status(400).json({ error: 'models[] is required' });
  }

  const results = {};
  // Seri çalıştır — Gemini queue'su paralel isteklerde kilitlenip timeout'a giriyor
  for (const model of models) {
    const start = Date.now();
    try {
      const r = await classifyOnce(transcript, model, { geminiMode: 'rest' });
      results[model] = {
        ok: true,
        latency: r.latency,
        intent: r.intent,
        confidence: r.confidence,
        distress: r.distress,
        distress_signals: r.distress_signals,
        response: r.response,
        resolved: r.resolved,
        escalate: r.escalate,
        delay_minutes: r.delay_minutes,
        input_tokens: r.input_tokens,
        output_tokens: r.output_tokens,
        mode: r.mode || 'rest',
      };
    } catch (err) {
      results[model] = {
        ok: false,
        error: err.message || String(err),
        latency: Date.now() - start,
      };
    }
  }

  res.json({ transcript: transcript, results: results });
});

app.get('/compare-stt-config', function(req, res) {
  const apiKey = process.env.DEEPGRAM_API_KEY;
  if (!apiKey || apiKey === 'your_deepgram_key') {
    return res.status(503).json({ error: 'DEEPGRAM_API_KEY is not set' });
  }
  const params = new URLSearchParams({
    ...DEEPGRAM_BASE_PARAMS,
    utterance_end_ms: '1000',
    vad_events: 'true',
    encoding: 'linear16',
    channels: '1',
  });
  res.json({
    apiKey: apiKey,
    params: params.toString() + DEEPGRAM_KEYTERMS,
  });
});

app.post('/compare-intent', async function(req, res) {
  let transcript = (req.body && req.body.transcript || '').trim();
  const models = (req.body && req.body.models) || [];
  let runs = parseInt(req.body && req.body.runs, 10);
  const expected = (req.body && req.body.expected) || null;
  // Mic/audio-file Deepgram latency from client; fall back to legacy stt_ms key
  let realSttMs = normalizeClientSttMs(
    (req.body && req.body.real_stt_ms != null) ? req.body.real_stt_ms : (req.body && req.body.stt_ms)
  );
  const compareOpts = realSttMs != null ? { realSttMs: realSttMs, geminiMode: 'live' } : { geminiMode: 'live' };
  let deepgramFromAudio = false;
  let durationSec = null;
  let deepgramCost = null;

  // Optional audio: Deepgram Nova-3 STT → transcript text → Gemini Live (no audio buffer to Live)
  const audioB64 = req.body && (req.body.audio_base64 || req.body.audioBuffer);
  const clientTranscript = transcript;
  if (audioB64 && typeof audioB64 === 'string') {
    try {
      const rawAudio = Buffer.from(audioB64, 'base64');
      console.log('[Compare Intent] Audio received — Deepgram STT first...', rawAudio.length, 'bytes');
      durationSec = await getAudioDurationSec(rawAudio);
      deepgramCost = durationSec != null ? (durationSec / 60) * 0.0043 : null;
      console.log('[Compare Intent] duration_sec:', durationSec, '| deepgram_cost:', deepgramCost);

      const stt = await transcribeCompareAudioDeepgram(rawAudio, req.body.audio_mime || 'audio/wav');
      if ((stt.transcript || '').trim()) {
        transcript = stt.transcript.trim();
        realSttMs = stt.sttMs;
        compareOpts.realSttMs = stt.sttMs;
        compareOpts.sttEngine = 'deepgram';
        compareOpts.sttSource = 'deepgram';
        compareOpts.sttConfidence = stt.confidence;
        deepgramFromAudio = true;
        console.log('[Compare Intent] STT (Deepgram):', stt.sttMs + 'ms | transcript:', JSON.stringify(transcript).slice(0, 100));
      } else if (clientTranscript) {
        // Deepgram empty (common on some webm/mic blobs) — keep panel transcript
        console.warn('[Compare Intent] Deepgram empty — falling back to client transcript');
        transcript = clientTranscript;
        if (realSttMs != null) {
          compareOpts.realSttMs = realSttMs;
          compareOpts.sttEngine = compareOpts.sttEngine || 'deepgram';
          compareOpts.sttSource = 'client_fallback';
        }
      } else {
        return res.status(400).json({ error: 'Deepgram returned empty transcript' });
      }
    } catch (e) {
      console.error('[Compare Intent] Deepgram STT error:', e.message);
      if (clientTranscript) {
        console.warn('[Compare Intent] Deepgram failed — falling back to client transcript');
        transcript = clientTranscript;
      } else {
        return res.status(400).json({ error: 'Deepgram STT failed: ' + (e.message || String(e)) });
      }
    }
  }

  if (!transcript) {
    return res.status(400).json({ error: 'transcript is required (or audio_base64 for Deepgram→Gemini)' });
  }
  if (!Array.isArray(models) || models.length === 0) {
    return res.status(400).json({ error: 'models[] is required' });
  }

  if (!Number.isFinite(runs) || runs < 1) runs = 1;
  if (runs > 20) runs = 20;

  const invalid = models.filter(function(m) {
    return !(COMPARE_MODEL_META[m] || m.indexOf('claude-') === 0 || resolveGeminiRestModels(m));
  });
  if (invalid.length) {
    return res.status(400).json({ error: 'Unsupported models: ' + invalid.join(', ') });
  }

  // Claude models run serially (Haiku then Sonnet); Gemini can stay parallel.
  const claudeModels = models.filter(function(m) { return String(m).indexOf('claude-') === 0; });
  const otherModels = models.filter(function(m) { return String(m).indexOf('claude-') !== 0; });

  function runOne(model) {
    const opts = Object.assign({}, compareOpts);
    // Never send audio buffer to Live — Gemini gets text only (TTFA)
    delete opts.audioBuffer;
    delete opts.audioPcm;
    return runModelComparison(transcript, model, runs, expected, opts)
      .catch(function(err) {
        return {
          ok: false,
          model: model,
          label: (COMPARE_MODEL_META[model] || {}).label || model,
          error: err.message || String(err),
          runs: [],
        };
      });
  }

  const otherSettled = await Promise.all(otherModels.map(runOne));
  const claudeSettled = [];
  for (let i = 0; i < claudeModels.length; i++) {
    claudeSettled.push(await runOne(claudeModels[i]));
  }

  const settled = otherSettled.concat(claudeSettled);

  const results = {};
  for (const row of settled) results[row.model] = row;

  // Attach duration/cost onto each model result when audio was transcribed here
  if (durationSec != null || deepgramCost != null) {
    Object.keys(results).forEach(function(model) {
      const row = results[model];
      if (!row) return;
      if (durationSec != null) row.duration_sec = durationSec;
      if (deepgramCost != null) row.deepgram_cost = deepgramCost;
      (row.runs || []).forEach(function(run) {
        if (!run || !run.ok) return;
        if (durationSec != null) run.duration_sec = durationSec;
        if (deepgramCost != null) run.deepgram_cost = deepgramCost;
      });
    });
  }

  res.json({
    transcript: transcript,
    expected: expected,
    runs: runs,
    real_stt_ms: realSttMs,
    stt_ms: realSttMs != null ? realSttMs : 300,
    stt_source: realSttMs != null ? (deepgramFromAudio ? 'deepgram' : 'real') : 'estimated',
    stt_engine: deepgramFromAudio ? 'deepgram' : undefined,
    audio_used: deepgramFromAudio,
    duration_sec: durationSec,
    deepgram_cost: deepgramCost,
    results: results,
  });
});

const FREEFORM_PROMPT = `You are an automated safety monitoring system for Aware360, a lone-worker safety platform. A worker missed their check-in and you called them. They said the following. Respond naturally and concisely as the monitoring system would — confirm their status, extend their timer, or connect them to a human agent as appropriate. Keep your response under 20 words. Output only your spoken reply, no JSON, no preamble.`;

async function classifyFreeformOnce(transcript, model) {
  const start = Date.now();
  let response = '';
  let inputTokens = 0;
  let outputTokens = 0;

  if (model.indexOf('claude-') === 0) {
    await withAnthropicCompareRetry(async function() {
      response = '';
      inputTokens = 0;
      outputTokens = 0;
      const stream = anthropic.messages.stream({
        model: model,
        max_tokens: 150,
        system: FREEFORM_PROMPT,
        messages: [{ role: 'user', content: 'Worker said: "' + transcript + '"' }]
      });
      for await (const chunk of stream) {
        if (chunk.type === 'content_block_delta' && chunk.delta?.type === 'text_delta') {
          response += chunk.delta.text;
        }
      }
      try {
        const finalMsg = await stream.finalMessage();
        const usage = parseClaudeUsage(finalMsg && finalMsg.usage);
        inputTokens = usage.input_tokens;
        outputTokens = usage.output_tokens;
      } catch (e) {
        inputTokens = 0;
        outputTokens = 0;
      }
    });
  } else {
    const candidates = resolveGeminiRestModels(model);
    if (!candidates) throw new Error('Unsupported Gemini model: ' + model);
    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey || apiKey === 'your_gemini_key') throw new Error('GEMINI_API_KEY is not set');

    let lastError = null;
    for (let i = 0; i < candidates.length; i++) {
      const restModel = candidates[i];
      const url = 'https://generativelanguage.googleapis.com/v1beta/models/' + restModel + ':generateContent?key=' + apiKey;
      try {
        const data = await enqueueGeminiCall(async function() {
          const geminiRes = await axios.post(url, {
            system_instruction: { parts: [{ text: FREEFORM_PROMPT }] },
            contents: [{ role: 'user', parts: [{ text: 'Worker said: "' + transcript + '"' }] }],
            generationConfig: { temperature: 0.3, maxOutputTokens: 400 },
          }, { headers: { 'Content-Type': 'application/json' }, timeout: 20000 });
          return geminiRes.data || {};
        });
        response = (((data.candidates || [])[0] || {}).content || {}).parts
          ? data.candidates[0].content.parts.map(function(p) { return p.text || ''; }).join('')
          : '';
        const usage = parseGeminiUsageMetadata(data.usageMetadata);
        inputTokens = usage.input_tokens;
        outputTokens = usage.output_tokens;
        break;
      } catch (err) {
        const apiMsg = err.response && err.response.data && err.response.data.error && err.response.data.error.message;
        lastError = new Error(apiMsg || err.message || 'Gemini generateContent failed');
        if (i === candidates.length - 1) throw lastError;
      }
    }
  }

  return {
    latency: Date.now() - start,
    response: response.trim(),
    input_tokens: inputTokens,
    output_tokens: outputTokens,
    total_tokens: inputTokens + outputTokens,
    cost: calcTokenCost(model, inputTokens, outputTokens),
  };
}

async function runFreeformComparison(transcript, model, runs) {
  const meta = COMPARE_MODEL_META[model] || { label: model, type: 'unknown' };
  const runRows = [];

  for (let i = 0; i < runs; i++) {
    try {
      const r = await classifyFreeformOnce(transcript, model);
      runRows.push({
        run: i + 1,
        ok: true,
        latency: r.latency,
        response: r.response,
        input_tokens: r.input_tokens,
        output_tokens: r.output_tokens,
        total_tokens: r.total_tokens != null ? r.total_tokens : ((r.input_tokens || 0) + (r.output_tokens || 0)),
        cost: r.cost,
      });
    } catch (err) {
      runRows.push({
        run: i + 1,
        ok: false,
        error: err.message || String(err),
        latency: 0,
      });
    }
    if (i < runs - 1) await sleepMs(150);
  }

  const okRuns = runRows.filter(function(r) { return r.ok; });
  if (!okRuns.length) {
    return {
      ok: false,
      model: model,
      label: meta.label,
      error: runRows[runRows.length - 1].error || 'All runs failed',
      runs: runRows,
    };
  }

  const latencies = okRuns.map(function(r) { return r.latency; });
  const costs = okRuns.map(function(r) { return r.cost; });
  const inputTokens = okRuns.map(function(r) { return r.input_tokens; });
  const outputTokens = okRuns.map(function(r) { return r.output_tokens; });
  const totalTokens = okRuns.map(function(r) {
    return r.total_tokens != null ? r.total_tokens : ((r.input_tokens || 0) + (r.output_tokens || 0));
  });
  const avgInput = avgNum(inputTokens);
  const avgOutput = avgNum(outputTokens);

  return {
    ok: true,
    model: model,
    label: meta.label,
    last_response: okRuns[okRuns.length - 1].response,
    avg_latency: Math.round(avgNum(latencies)),
    min_latency: Math.min.apply(null, latencies),
    max_latency: Math.max.apply(null, latencies),
    avg_tokens: Math.round(avgNum(totalTokens)),
    avg_input_tokens: Math.round(avgInput),
    avg_output_tokens: Math.round(avgOutput),
    est_cost: avgNum(costs),
    runs: runRows,
  };
}

app.post('/compare-freeform', async function(req, res) {
  const transcript = (req.body && req.body.transcript || '').trim();
  const models = (req.body && req.body.models) || [];
  let runs = parseInt(req.body && req.body.runs, 10);

  if (!transcript) return res.status(400).json({ error: 'transcript is required' });
  if (!Array.isArray(models) || models.length === 0) {
    return res.status(400).json({ error: 'models[] is required' });
  }

  if (!Number.isFinite(runs) || runs < 1) runs = 1;
  if (runs > 20) runs = 20;

  const invalid = models.filter(function(m) {
    return !(COMPARE_MODEL_META[m] || m.indexOf('claude-') === 0 || resolveGeminiRestModels(m));
  });
  if (invalid.length) {
    return res.status(400).json({ error: 'Unsupported models: ' + invalid.join(', ') });
  }

  const settled = await Promise.all(models.map(function(model) {
    return runFreeformComparison(transcript, model, runs)
      .catch(function(err) {
        return {
          ok: false,
          model: model,
          label: (COMPARE_MODEL_META[model] || {}).label || model,
          error: err.message || String(err),
          runs: [],
        };
      });
  }));

  const results = {};
  for (const row of settled) results[row.model] = row;

  res.json({ transcript: transcript, runs: runs, results: results });
});

app.get('/api/calls', function(req, res) {
  const page = parseInt(req.query.page, 10) || 1;
  const limit = parseInt(req.query.limit, 10) || 20;
  try {
    res.json(listCalls(page, limit));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/calls/:callSid', function(req, res) {
  try {
    const call = getCallBySid(req.params.callSid);
    if (!call) return res.status(404).json({ error: 'Call not found' });
    call.transcript = call.transcript ? JSON.parse(call.transcript) : [];
    res.json(call);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/recording-status', function(req, res) {
  const url = buildTwilioWebhookUrl(req, '/recording-status');
  if (!validateTwilioSignature(req, res, url)) return;

  const { RecordingUrl, RecordingDuration, RecordingSid, CallSid, RecordingStatus } = req.body || {};
  console.log('[Recording] Status:', RecordingStatus, '| Duration:', RecordingDuration + 's', '| SID:', RecordingSid);
  if (RecordingStatus === 'completed' && RecordingUrl && CallSid) {
    const recordingUrl = RecordingUrl.endsWith('.mp3') ? RecordingUrl : RecordingUrl + '.mp3';
    try {
      updateRecordingUrl(CallSid, recordingUrl);
      console.log('[Recording] Saved URL for call:', CallSid);
      console.log('[Recording] URL:', recordingUrl);
    } catch (e) {
      console.error('[Recording] DB update error:', e.message);
    }
  }
  res.sendStatus(200);
});

app.get('/api/recordings/proxy', async function(req, res) {
  const accountSid = process.env.TWILIO_ACCOUNT_SID;
  const authToken = process.env.TWILIO_AUTH_TOKEN;
  const recordingUrl = req.query.url ? decodeURIComponent(req.query.url) : '';

  if (!accountSid || !authToken) {
    return res.status(500).json({ error: 'Twilio credentials not configured' });
  }
  if (!recordingUrl || !recordingUrl.startsWith('https://api.twilio.com/')) {
    return res.status(400).json({ error: 'Invalid recording URL' });
  }

  console.log('[Recording Proxy] URL:', recordingUrl);

  try {
    const response = await axios.get(recordingUrl, {
      auth: { username: accountSid, password: authToken },
      responseType: 'stream',
    });

    res.setHeader('Content-Type', 'audio/mpeg');
    response.data.pipe(res);
  } catch (err) {
    console.error('[Recording Proxy] Error:', err.response?.status, err.message);
    res.status(404).json({ error: 'Recording not found' });
  }
});

app.get('/health', function(req, res) {
  res.json({ status: 'ok', ngrok: PUBLIC_URL, twilio_ws: PUBLIC_URL.replace('https://', 'wss://') + '/twilio', apis: {
    deepgram: !!process.env.DEEPGRAM_API_KEY && process.env.DEEPGRAM_API_KEY !== 'your_deepgram_key',
    anthropic: !!process.env.ANTHROPIC_API_KEY && process.env.ANTHROPIC_API_KEY !== 'your_anthropic_key',
    elevenlabs: !!process.env.ELEVENLABS_API_KEY && process.env.ELEVENLABS_API_KEY !== 'your_elevenlabs_key',
    gemini: !!process.env.GEMINI_API_KEY && process.env.GEMINI_API_KEY !== 'your_gemini_key'
  }});
});

const PORT = process.env.PORT || 3000;
function startServer() {
  server.listen(PORT, function() {
    console.log('\n🎙️  Voice AI Demo: http://localhost:' + PORT);
    console.log('📞  Twilio webhook: ' + PUBLIC_URL + '/incoming-call');
    console.log('🔌  Twilio WS path: ' + PUBLIC_URL.replace('https://', 'wss://') + '/twilio');
    console.log('[DeepFilter] ' + (DEEPFILTER_ENABLED ? 'ENABLED' : 'DISABLED (raw mic → Deepgram)'));
    console.log('Cascaded Pipeline: Deepgram nova-2 -> Claude -> ElevenLabs\n');
  });
}
const bootTasks = [
  loadMulawCache().catch(function(e) {
    console.error('[Mulaw Cache] Init error:', e.message);
  }),
];
if (DEEPFILTER_ENABLED) {
  bootTasks.push(preloadDeepFilter().catch(function(e) {
    console.error('[DeepFilter] Model load failed:', e.message);
    console.error('[DeepFilter] Denoise will be disabled until model is available');
  }));
} else {
  console.log('[DeepFilter] Preload skipped — DEEPFILTER_ENABLED=false');
}
Promise.all(bootTasks).then(function() {
  startServer();
});
