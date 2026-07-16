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

app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(express.static(path.join(__dirname, 'public')));

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
const { listCalls, getCallBySid, insertCallStart, updateCallEnd, updateCallLatency, updateRecordingUrl } = require('./db');
const { createDeepFilter, preloadDeepFilter, isDeepFilterReady } = require('./deepFilterWrapper');

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

  function clearSilenceTimer() { if (silenceTimer) { clearTimeout(silenceTimer); silenceTimer = null; } }

  function sendAudioToTwilio(audioBuffer) {
    if (ws.readyState !== WebSocket.OPEN || !streamSid) return;
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
      if (callLastConfidence !== null && callLastConfidence < CONFIDENCE_THRESHOLD
          && criticalIntents.includes(result.intent) && !safetyConfirmPending) {
        safetyConfirmPending = true;
        const confirmText = result.intent === 'SAFE_CONFIRMATION'
          ? "Sorry, it's a bit hard to hear you. Can you confirm clearly that you are safe?"
          : "Sorry, I didn't catch that clearly. How many more minutes do you need?";
        console.log('[Confidence] Low confidence (' + callLastConfidence.toFixed(2) + ') for', result.intent, '— asking to confirm');
        callLastConfidence = null;
        await speakText(confirmText, null, false);
        sessionState = 'LISTENING';
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
        deepgramWs.send(Buffer.from(msg.media.payload, 'base64'));
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
  let snrBlocked = false;
  let selectedModel = 'claude-haiku-4-5';
  let ttsMode = 'elevenlabs';
  let lastMediaAt = 0;
  let lastConfidence = null;
  let safetyConfirmPending = false;
  let clientE2eMs = null;
  let deepFilter = null;
  let dfPreferred = true;  // Noise Cancel ON by default
  let dfEnabled = true;
  const SILENCE_MS = 4000;

  function send(obj) { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj)); }
  function clearSilenceTimer() { if (silenceTimer) { clearTimeout(silenceTimer); silenceTimer = null; } }

  function ensureDeepFilter() {
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

  // Activate Noise Cancel immediately on connect (no toggle required)
  if (ensureDeepFilter()) {
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
      if (lastConfidence !== null && lastConfidence < CONFIDENCE_THRESHOLD
          && criticalIntents.includes(result.intent) && !safetyConfirmPending) {
        safetyConfirmPending = true;
        const confirmText = result.intent === 'SAFE_CONFIRMATION'
          ? "Sorry, it's a bit hard to hear you. Can you confirm clearly that you are safe?"
          : "Sorry, I didn't catch that clearly. How many more minutes do you need?";
        console.log('[Confidence] Low confidence (' + lastConfidence.toFixed(2) + ') for', result.intent, '— asking to confirm');
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
          dfPreferred = msg.enabled !== false; // default ON unless explicitly false
          ensureDeepFilter();
          if (!dfPreferred) dfEnabled = false;
          console.log('[DeepFilter]', dfPreferred ? 'preferred ON' : 'preferred OFF', '| active=', dfEnabled);
          send({ type: 'log', message: 'Noise cancellation ' + (dfEnabled ? 'enabled' : 'disabled') });
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
          console.log('[Server] Audio file upload started:', msg.filename, msg.duration + 's');
          isReceivingAudioFile = true;
          audioFileChunks = [];
          send({ type: 'log', message: 'Receiving audio file: ' + msg.filename });
        }

        if (msg.type === 'audio_file_end' && isReceivingAudioFile) {
          isReceivingAudioFile = false;
          if (audioFileChunks.length === 0) return;
          const fullBuffer = Buffer.concat(audioFileChunks);
          audioFileChunks = [];

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
              console.log('[Deepgram] File transcript:', fileTranscript);
              send({ type: 'stage', stage: 'stt', status: 'done' });
              await handleTranscript(fileTranscript, null);
            } else {
              console.warn('[Deepgram] Empty transcript. alt=', JSON.stringify(alt));
              send({ type: 'log', message: 'No transcript from Deepgram' });
              send({ type: 'stage', stage: 'stt', status: 'error' });
            }
          } catch (e) {
            console.error('[Deepgram File Error]', e.message);
            if (e.response) console.error('[Deepgram File Error] body:', JSON.stringify(e.response.data).substring(0, 400));
            send({ type: 'log', message: 'Deepgram error: ' + e.message });
            send({ type: 'stage', stage: 'stt', status: 'error' });
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
          if (dfEnabled && deepFilter) {
            deepFilter.processPcm16(raw).then(function(cleanBuf) {
              sendAudioToDeepgram(cleanBuf);
            }).catch(function(e) {
              console.error('[DeepFilter] process error:', e.message);
              sendAudioToDeepgram(raw);
            });
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
  'gemini-2.0-flash-live-001': { input: 2.10, output: 8.50 },
  'gemini-3.1-flash-live-preview': { input: 2.10, output: 8.50 },
  'gemini-2.0-flash': { input: 2.10, output: 8.50 },
  'gemini-2.0-flash-001': { input: 2.10, output: 8.50 },
  'gemini-3-flash-preview': { input: 2.10, output: 8.50 },
};

const COMPARE_MODEL_META = {
  'claude-haiku-4-5': { type: 'claude', label: 'Claude Haiku 4.5' },
  'claude-sonnet-4-6': { type: 'claude', label: 'Claude Sonnet 4.6' },
  'gemini-2.0-flash-live-001': { type: 'gemini', label: 'Gemini 2.0 Flash Live' },
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

/** Normalize Gemini usageMetadata — do not subtract modality details (e.g. audio). */
function parseGeminiUsageMetadata(usageMetadata) {
  const u = usageMetadata || {};
  const input = Number(u.promptTokenCount) || 0;
  const output = Number(u.candidatesTokenCount) || 0;
  const total = (typeof u.totalTokenCount === 'number' && Number.isFinite(u.totalTokenCount))
    ? u.totalTokenCount
    : (input + output);
  // Log details for debugging but never subtract them from the totals we report
  if (Array.isArray(u.promptTokensDetails) && u.promptTokensDetails.length) {
    console.log('[Compare] Gemini promptTokensDetails:', JSON.stringify(u.promptTokensDetails));
  }
  return {
    input_tokens: input,
    output_tokens: output,
    total_tokens: total,
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

async function classifyWithClaudeOnce(transcript, model, opts) {
  const realSttMs = opts && opts.realSttMs != null ? opts.realSttMs : null;
  const sttMs = realSttMs != null ? realSttMs : 300;
  const sttSource = realSttMs != null ? 'real' : 'estimated';

  const llmStart = Date.now();
  const result = await classifyIntent(transcript, model, { skipFastPath: true });
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

// Benchmark/compare uses non-live generateContent (text JSON), not Live audio.
// Live models (bidiGenerateContent) are audio-first and reject TEXT modality;
// REST avoids WebSocket + audio synthesis overhead so latency is apples-to-apples with Claude.
// UI still labels them as Live siblings; server_gemini.js keeps Live AUDIO for voice calls.
// Each UI id maps to an ordered list of REST models (primary first, fallbacks for quota/availability).
// Compare uses 2.5 Flash REST with thinkingBudget: 0 (no thinking overhead).
const GEMINI_REST_MODEL_MAP = {
  'gemini-2.0-flash-live-001': ['gemini-2.5-flash'],
  'gemini-3.1-flash-live-preview': ['gemini-2.5-flash'],
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

async function classifyOnce(transcript, model, opts) {
  if (model.indexOf('claude-') === 0) return classifyWithClaudeOnce(transcript, model, opts);
  if (resolveGeminiRestModels(model)) return classifyWithGeminiOnce(transcript, model, opts);
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
        // Gemini compare is LLM-only; no cascaded STT/TTS breakdown.
        totalLatency = r.latency;
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
        llm_ms: llmMs,
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
    avg_stt_ms: isGemini ? null : (sttTimes.length ? Math.round(avgNum(sttTimes)) : null),
    stt_source: isGemini ? null : (lastOk.stt_source || null),
    avg_llm_ms: isGemini ? null : (llmTimes.length ? Math.round(avgNum(llmTimes)) : null),
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
      const r = await classifyOnce(transcript, model);
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
  const transcript = (req.body && req.body.transcript || '').trim();
  const models = (req.body && req.body.models) || [];
  let runs = parseInt(req.body && req.body.runs, 10);
  const expected = (req.body && req.body.expected) || null;
  // Mic/audio-file Deepgram latency from client; fall back to legacy stt_ms key
  const realSttMs = normalizeClientSttMs(
    (req.body && req.body.real_stt_ms != null) ? req.body.real_stt_ms : (req.body && req.body.stt_ms)
  );
  const compareOpts = realSttMs != null ? { realSttMs: realSttMs } : {};

  if (!transcript) return res.status(400).json({ error: 'transcript is required' });
  if (!Array.isArray(models) || models.length === 0) {
    return res.status(400).json({ error: 'models[] is required' });
  }

  if (!Number.isFinite(runs) || runs < 1) runs = 1;
  if (runs > 10) runs = 10;

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
    return runModelComparison(transcript, model, runs, expected, compareOpts)
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

  res.json({
    transcript: transcript,
    expected: expected,
    runs: runs,
    real_stt_ms: realSttMs,
    stt_ms: realSttMs != null ? realSttMs : 300,
    stt_source: realSttMs != null ? 'real' : 'estimated',
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
  if (runs > 10) runs = 10;

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
    console.log('Cascaded Pipeline: Deepgram nova-2 -> Claude -> ElevenLabs\n');
  });
}
Promise.all([
  loadMulawCache().catch(function(e) {
    console.error('[Mulaw Cache] Init error:', e.message);
  }),
  preloadDeepFilter().catch(function(e) {
    console.error('[DeepFilter] Model load failed:', e.message);
    console.error('[DeepFilter] Denoise will be disabled until model is available');
  }),
]).then(function() {
  startServer();
});
