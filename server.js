require('dotenv').config();

const fs = require('fs');
const path = require('path');

const express = require('express');
const WebSocket = require('ws');
const http = require('http');
const axios = require('axios');
const Anthropic = require('@anthropic-ai/sdk');
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

const SYSTEM_PROMPT = `Aware360 safety system. Missed check-in alert. Classify worker response.

Return ONLY JSON: {"intent":"<SAFE_CONFIRMATION|DELAYED_CHECKIN|REQUEST_HUMAN|UNRECOGNISED>","confidence":<0.0-1.0>,"response":"<reply>","resolved":<bool>,"escalate":<bool>,"delay_minutes":<null|number>,"distress":<null|"low"|"medium"|"high">,"distress_signals":<null|string>}

INTENTS:
SAFE_CONFIRMATION: "yes/fine/ok/safe/all good" → resolved:true, response:"Got it — I've reset your safety timer. Stay safe."
DELAYED_CHECKIN: needs more time → resolved:true, delay_minutes:15 or stated number, response:"Understood. I've extended your check-in timer. Please check in when ready."
REQUEST_HUMAN: wants human/help → escalate:true, response:"Of course. I'm flagging this for a live agent — someone will call you back shortly."
UNRECOGNISED: unclear → resolved:false, response:"I'm sorry, I didn't catch that. Are you safe, need more time, or need a human agent?"

DISTRESS:
low: "I think/maybe/not sure" only
medium: "worried/scared/afraid/nervous/something wrong" → escalate:true, REQUEST_HUMAN
high: "help/hurt/pain/fell/fall/broke/bleeding/dizzy/injured/stuck/trapped/SOS/emergency" → REQUEST_HUMAN

CRITICAL: ANY physical indicator (fell, broke, hurt, pain, dizzy) = REQUEST_HUMAN regardless of reassurance like "I'm okay I promise". "worried/scared/afraid" even with "I'm fine" = distress:medium = REQUEST_HUMAN.

Examples:
"I think I'm fine but I'm a bit worried" → REQUEST_HUMAN, distress:medium
"I just fell down but I'm okay I promise" → REQUEST_HUMAN, distress:high`;

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

const DEEPGRAM_BASE_PARAMS = {
  model: 'nova-3', language: 'en-US', punctuate: 'true',
  smart_format: 'true', interim_results: 'true',
  utterance_end_ms: '1000', vad_events: 'true'
};

const DEEPGRAM_KEYTERMS = '&keyterm=help&keyterm=SOS&keyterm=emergency&keyterm=hurt&keyterm=pain&keyterm=injured&keyterm=injury&keyterm=stuck&keyterm=trapped&keyterm=dizzy&keyterm=bleeding&keyterm=fallen&keyterm=fell&keyterm=scared&keyterm=afraid&keyterm=agent&keyterm=human&keyterm=danger&keyterm=accident';

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

// ─── TwiML webhook ───
app.post('/incoming-call', function(req, res) {
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

  const stream = anthropic.messages.stream({
    model: model || 'claude-haiku-4-5',
    max_tokens: 200,
    system: SYSTEM_PROMPT,
    messages: [{ role: 'user', content: 'Worker said: "' + transcript + '"' }]
  });

  let fullText = '';
  let usage = { input_tokens: 0, output_tokens: 0 };

  for await (const chunk of stream) {
    if (chunk.type === 'content_block_delta' && chunk.delta?.type === 'text_delta') {
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
  const parsed = parseIntentJson(fullText);
  parsed._usage = usage;
  return parsed;
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
  let postSpeechStart = null;
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
      const fast = fastPathIntent(finalTranscript);
      let result;
      if (fast) {
        result = fast;
        callLlmMs = 0;
      } else {
        const llmStart = Date.now();
        result = await classifyIntent(finalTranscript, 'claude-haiku-4-5', { skipFastPath: true });
        callLlmMs = Date.now() - llmStart;
      }
      if (result.confidence < 0.45) {
        result.intent = 'UNRECOGNISED'; result.resolved = false; result.escalate = false;
        result.response = CACHE_RESPONSES.unrecognised;
      }
      result = applyDistressLogic(result);
      applyCachedResponse(result);
      callFinalIntent = result.intent;
      console.log('[Twilio Intent]', result.intent, '| conf:', result.confidence, '| distress:', result.distress || 'none', '| LLM:', callLlmMs === 0 ? 'Fast-path' : callLlmMs + 'ms');
      const cacheKey = INTENT_CACHE_MAP[result.intent];
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
        if (msg.type === 'SpeechStarted') {
          postSpeechStart = Date.now();
          console.log('[Twilio] Speech started detected');
        }
        if (msg.type === 'UtteranceEnd') {
          postSpeechStart = Date.now();
        }
        if (msg.type === 'Results' && msg.channel && msg.channel.alternatives && msg.channel.alternatives[0]) {
          const text = msg.channel.alternatives[0].transcript;
          if (text && text.trim()) {
            if (msg.is_final && msg.speech_final && sessionState === 'LISTENING') {
              clearSilenceTimer();
              if (!postSpeechStart) postSpeechStart = Date.now(); // fallback: speech_final anı
              const sttMs = Date.now() - postSpeechStart;
              postSpeechStart = null;
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
  let clientMimeType = 'audio/webm';
  let isReceivingAudioFile = false;
  let audioFileChunks = [];
  let snrBlocked = false;
  let selectedModel = 'claude-haiku-4-5';
  let ttsMode = 'elevenlabs';
  let postSpeechStart = null;
  const SILENCE_MS = 4000;

  function send(obj) { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj)); }
  function clearSilenceTimer() { if (silenceTimer) { clearTimeout(silenceTimer); silenceTimer = null; } }

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
      let result = await classifyIntent(finalTranscript, selectedModel);
      const llmMs = Date.now() - llmStart;
      result = applyDistressLogic(result);
      if (result.confidence < 0.45 && !result.escalate) {
        result.intent = 'UNRECOGNISED'; result.resolved = false; result.escalate = false;
        result.response = CACHE_RESPONSES.unrecognised;
      } else if (result.confidence < 0.85) {
        result.low_confidence = true;
      }
      applyCachedResponse(result);
      console.log('[Intent]', result.intent, '| conf:', result.confidence, '| distress:', result.distress || 'none', '| LLM:', llmMs + 'ms');
      send({ type: 'intent', data: result });
      if (result._usage) {
        send({ type: 'token_usage', input: result._usage.input_tokens, output: result._usage.output_tokens, model: selectedModel });
      }
      send({ type: 'stage', stage: 'llm', status: 'done', ms: llmMs });
      send({ type: 'stage', stage: 'tts', status: 'processing' });
      const ttsStart = Date.now();
      if (ttsMode === 'browser') {
        send({ type: 'stage', stage: 'tts', status: 'done', ms: 0 });
        send({ type: 'response', text: result.response, audio: null, resolved: result.resolved, escalate: result.escalate });
      } else {
        const cacheKey = INTENT_CACHE_MAP[result.intent];
        const useCache = cacheKey && audioCache[cacheKey];
        if (useCache) {
          console.log('[Cache] Hit:', cacheKey);
          send({ type: 'stage', stage: 'tts', status: 'done', ms: Date.now() - ttsStart });
          send({ type: 'response', text: result.response, audio: audioCache[cacheKey], resolved: result.resolved, escalate: result.escalate });
        } else {
          const apiKey = process.env.ELEVENLABS_API_KEY;
          const voiceId = process.env.ELEVENLABS_VOICE_ID || 'EXAVITQu4vr4xnSDxMaL';
          let firstChunk = true; let chunkIndex = 0;
          try {
            send({ type: 'audio_stream_start', text: result.response, resolved: result.resolved, escalate: result.escalate });
            for await (const chunk of streamTTS(result.response, apiKey, voiceId)) {
              if (firstChunk) { send({ type: 'stage', stage: 'tts', status: 'done', ms: Date.now() - ttsStart }); firstChunk = false; }
              send({ type: 'audio_chunk', chunk: chunk, index: chunkIndex++ });
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
    const params = new URLSearchParams({ ...DEEPGRAM_BASE_PARAMS });
    if (isOgg) { params.set('encoding', 'opus'); params.set('container', 'ogg'); }
    const dgUrl = 'wss://api.deepgram.com/v1/listen?' + params.toString() + DEEPGRAM_KEYTERMS;
    console.log('[Deepgram] URL:', dgUrl);
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
        if (msg.type === 'SpeechStarted') postSpeechStart = null;
        if (msg.type === 'Results' && msg.channel && msg.channel.alternatives && msg.channel.alternatives[0]) {
          const text = msg.channel.alternatives[0].transcript;
          if (text && text.trim()) {
            if (msg.is_final && !msg.speech_final) postSpeechStart = Date.now();
            send({ type: 'interim_transcript', text: text, is_final: msg.is_final });
            if (msg.is_final && msg.speech_final && sessionState === 'LISTENING') {
              clearSilenceTimer();
              const sttMs = postSpeechStart ? Date.now() - postSpeechStart : 300;
              postSpeechStart = null;
              transcript = text;
              const toProcess = transcript; transcript = '';
              await handleTranscript(toProcess, sttMs);
            }
          }
        }
        if (msg.type === 'UtteranceEnd' && transcript && sessionState === 'LISTENING') {
          clearSilenceTimer();
          const sttMs = postSpeechStart ? Date.now() - postSpeechStart : 300;
          postSpeechStart = null;
          const toProcess = transcript; transcript = '';
          await handleTranscript(toProcess, sttMs);
        }
      } catch (e) {}
    });
    deepgramWs.on('error', function(e) { console.error('[Deepgram] Error:', e.message); send({ type: 'stage', stage: 'stt', status: 'error' }); });
    deepgramWs.on('close', function(code, reason) { console.log('[Deepgram] Closed', code, reason ? reason.toString() : ''); send({ type: 'stage', stage: 'stt', status: 'disconnected' }); });
  }

  ws.on('error', function(e) { console.error('[WS Error]', e.message); });

  ws.on('message', async function(data) {
    try {
      if (typeof data === 'string' || (data instanceof Buffer && data[0] === 0x7b)) {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'mime_type') { clientMimeType = msg.mimeType; }
        if (msg.type === 'set_model') { selectedModel = msg.model; console.log('[Model]', selectedModel); }
        if (msg.type === 'tts_mode') { ttsMode = msg.mode; }
        if (msg.type === 'start') {
          send({ type: 'log', message: 'Session started — Missed Check-In alert for Industrial Worker' });
          sessionState = 'GREETING';
          send({ type: 'stage', stage: 'stt', status: 'connecting' });
          send({ type: 'stage', stage: 'tts', status: 'processing' });
          startDeepgramStream();
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

          const wavBuffer = createWavBuffer(fullBuffer);
          console.log('[Server] Audio file received, size:', fullBuffer.length, 'bytes, sending to Deepgram...');
          send({ type: 'log', message: 'Sending to Deepgram STT...' });
          send({ type: 'stage', stage: 'stt', status: 'processing' });
          try {
            const dgApiKey = process.env.DEEPGRAM_API_KEY;
            const axios = require('axios');
            const response = await axios.post(
              'https://api.deepgram.com/v1/listen?model=nova-2&language=en-US&punctuate=true&smart_format=true',
              wavBuffer,
              { headers: { 'Authorization': 'Token ' + dgApiKey, 'Content-Type': 'audio/wav' } }
            );
            const fileTranscript = response.data?.results?.channels?.[0]?.alternatives?.[0]?.transcript;
            if (fileTranscript) {
              console.log('[Deepgram] File transcript:', fileTranscript);
              send({ type: 'stage', stage: 'stt', status: 'done' });
              await handleTranscript(fileTranscript, null);
            } else {
              send({ type: 'log', message: 'No transcript from Deepgram' });
              send({ type: 'stage', stage: 'stt', status: 'error' });
            }
          } catch (e) {
            console.error('[Deepgram File Error]', e.message);
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
          if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) deepgramWs.send(data);
          else audioBuffer = Buffer.concat([audioBuffer, data]);
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
  const sttMs = 300;

  const llmStart = Date.now();
  const result = await classifyIntent(transcript, model, { skipFastPath: true });
  const llmMs = Date.now() - llmStart;
  console.log('[Compare LLM]', model, llmMs + 'ms', result._usage?.cache_read_input_tokens ? '(cache hit)' : '(cache miss)');

  const usage = result._usage || { input_tokens: 0, output_tokens: 0 };
  delete result._usage;
  const applied = applyDistressLogic({ ...result });

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
    llm_ms: llmMs,
    tts_ms: ttsMs,
    intent: applied.intent,
    confidence: typeof result.confidence === 'number' ? result.confidence : 0,
    response: applied.response,
    resolved: !!applied.resolved,
    escalate: !!applied.escalate,
    delay_minutes: applied.delay_minutes ?? null,
    distress: result.distress || null,
    distress_signals: result.distress_signals || null,
    input_tokens: usage.input_tokens || 0,
    output_tokens: usage.output_tokens || 0,
  };
}

// Benchmark/compare uses non-live generateContent (text JSON), not Live audio.
// Live models (bidiGenerateContent) are audio-first and reject TEXT modality;
// REST avoids WebSocket + audio synthesis overhead so latency is apples-to-apples with Claude.
// UI still labels them as Live siblings; server_gemini.js keeps Live AUDIO for voice calls.
// Each UI id maps to an ordered list of REST models (primary first, fallbacks for quota/availability).
const GEMINI_REST_MODEL_MAP = {
  'gemini-2.0-flash-live-001': ['gemini-2.5-flash', 'gemini-2.0-flash', 'gemini-2.0-flash-001'],
  'gemini-3.1-flash-live-preview': ['gemini-2.5-flash', 'gemini-2.0-flash', 'gemini-2.0-flash-001'],
  'gemini-2.0-flash': ['gemini-2.5-flash', 'gemini-2.0-flash', 'gemini-2.0-flash-001'],
  'gemini-2.0-flash-001': ['gemini-2.5-flash', 'gemini-2.0-flash-001', 'gemini-2.0-flash'],
  'gemini-3-flash-preview': ['gemini-2.5-flash', 'gemini-2.0-flash'],
};

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
          const response = await axios.post(url, {
            system_instruction: { parts: [{ text: SYSTEM_PROMPT }] },
            contents: [{ role: 'user', parts: [{ text: 'Worker said: "' + transcript + '"' }] }],
            generationConfig: {
              responseMimeType: 'application/json',
              temperature: 0,
            },
          }, {
            headers: { 'Content-Type': 'application/json' },
            timeout: 20000,
          });
          const latency = Date.now() - start;
          const data = response.data || {};
          const text = (((data.candidates || [])[0] || {}).content || {}).parts
            ? data.candidates[0].content.parts.map(function(p) { return p.text || ''; }).join('')
            : '';
          const usage = data.usageMetadata || {};
          const parsed = parseIntentJson(text);
          const applied = applyDistressLogic({ ...parsed });
          return {
            latency: latency,
            intent: applied.intent,
            confidence: typeof parsed.confidence === 'number' ? parsed.confidence : 0,
            response: applied.response,
            resolved: !!applied.resolved,
            escalate: !!applied.escalate,
            delay_minutes: applied.delay_minutes ?? null,
            distress: parsed.distress || null,
            distress_signals: parsed.distress_signals || null,
            input_tokens: usage.promptTokenCount || 0,
            output_tokens: usage.candidatesTokenCount || 0,
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

async function runModelComparison(transcript, model, runs, expected) {
  const meta = COMPARE_MODEL_META[model] || { label: model, type: 'unknown' };
  const runRows = [];

  for (let i = 0; i < runs; i++) {
    try {
      const r = await classifyOnce(transcript, model);
      const cost = calcTokenCost(model, r.input_tokens, r.output_tokens);
      const isGemini = String(model).indexOf('gemini') === 0;
      let sttMs = null;
      let llmMs = null;
      let ttsMs = null;
      let totalLatency = r.latency;

      if (isGemini) {
        // Gemini compare is LLM-only; no cascaded STT/TTS breakdown.
        totalLatency = r.latency;
      } else {
        sttMs = r.stt_ms ?? null;
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
      await new Promise(function(r) { setTimeout(r, 500); });
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
  const confidences = okRuns.map(function(r) { return r.confidence; });
  const inputTokens = okRuns.map(function(r) { return r.input_tokens; });
  const outputTokens = okRuns.map(function(r) { return r.output_tokens; });
  const costs = okRuns.map(function(r) { return r.cost; });
  const sttTimes = okRuns.map(function(r) { return r.stt_ms; }).filter(function(v) { return v !== null && v !== undefined; });
  const llmTimes = okRuns.map(function(r) { return r.llm_ms; }).filter(function(v) { return v !== null && v !== undefined; });
  const ttsTimes = okRuns.map(function(r) { return r.tts_ms; }).filter(function(v) { return v !== null && v !== undefined; });
  const isGemini = String(model).indexOf('gemini') === 0;
  const avgInput = avgNum(inputTokens);
  const avgOutput = avgNum(outputTokens);

  return {
    ok: true,
    model: model,
    label: meta.label,
    intent: lastOk.intent,
    avg_confidence: avgNum(confidences),
    avg_latency: Math.round(avgNum(latencies)),
    avg_stt_ms: isGemini ? null : (sttTimes.length ? Math.round(avgNum(sttTimes)) : null),
    avg_llm_ms: isGemini ? null : (llmTimes.length ? Math.round(avgNum(llmTimes)) : null),
    avg_tts_ms: isGemini ? null : (ttsTimes.length ? Math.round(avgNum(ttsTimes)) : null),
    min_latency: Math.min.apply(null, latencies),
    max_latency: Math.max.apply(null, latencies),
    avg_input_tokens: Math.round(avgInput),
    avg_output_tokens: Math.round(avgOutput),
    avg_tokens: Math.round(avgInput + avgOutput),
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

  if (!transcript) return res.status(400).json({ error: 'transcript is required' });
  if (!Array.isArray(models) || models.length === 0) {
    return res.status(400).json({ error: 'models[] is required' });
  }

  if (!Number.isFinite(runs) || runs < 1) runs = 5;
  if (runs > 10) runs = 10;

  const invalid = models.filter(function(m) {
    return !(COMPARE_MODEL_META[m] || m.indexOf('claude-') === 0 || resolveGeminiRestModels(m));
  });
  if (invalid.length) {
    return res.status(400).json({ error: 'Unsupported models: ' + invalid.join(', ') });
  }

  const settled = await Promise.all(models.map(function(model) {
    return runModelComparison(transcript, model, runs, expected)
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

  res.json({
    transcript: transcript,
    expected: expected,
    runs: runs,
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
      if (chunk.type === 'message_start' && chunk.message?.usage) {
        inputTokens = chunk.message.usage.input_tokens || 0;
      }
      if (chunk.type === 'message_delta' && chunk.usage) {
        outputTokens = chunk.usage.output_tokens || 0;
      }
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
        const usage = data.usageMetadata || {};
        inputTokens = usage.promptTokenCount || 0;
        outputTokens = usage.candidatesTokenCount || 0;
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
    avg_tokens: Math.round(avgInput + avgOutput),
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

  if (!Number.isFinite(runs) || runs < 1) runs = 3;
  if (runs > 5) runs = 5;

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
  const { RecordingUrl, RecordingDuration, RecordingSid, CallSid, RecordingStatus } = req.body || {};
  console.log('[Recording] Status:', RecordingStatus, '| Duration:', RecordingDuration + 's', '| SID:', RecordingSid);
  if (RecordingStatus === 'completed' && RecordingUrl && CallSid) {
    const url = RecordingUrl.endsWith('.mp3') ? RecordingUrl : RecordingUrl + '.mp3';
    try {
      updateRecordingUrl(CallSid, url);
      console.log('[Recording] Saved URL for call:', CallSid);
      console.log('[Recording] URL:', url);
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
loadMulawCache().then(function() {
  startServer();
}).catch(function(e) {
  console.error('[Mulaw Cache] Init error:', e.message);
  // Cache başarısız olsa da server başlasın
  startServer();
});
