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

const SYSTEM_PROMPT = `Aware360 safety system. Worker missed check-in. Classify their response AND detect distress signals.

Return ONLY this JSON: {"intent":"<SAFE_CONFIRMATION|DELAYED_CHECKIN|REQUEST_HUMAN|UNRECOGNISED>","confidence":<0.0-1.0>,"response":"<reply>","resolved":<bool>,"escalate":<bool>,"delay_minutes":<null|number>,"distress":<null|"low"|"medium"|"high">,"distress_signals":<null|string>}

INTENT RULES:
SAFE_CONFIRMATION: clear safety confirmation ("yes", "yep", "fine", "ok", "okay", "good", "safe", "I'm fine", "all good", "no problem") -> resolved:true, confidence:0.95, response:"Got it — I've reset your safety timer. Stay safe."
DELAYED_CHECKIN: needs more time -> resolved:true, delay_minutes:number or 15, response:"Understood. I've extended your check-in timer by [X] minutes. Please check in when ready."
REQUEST_HUMAN: wants human/help/SOS -> escalate:true, response:"Of course. I'm flagging this for a live agent — someone will call you back shortly. Keep your phone available."
UNRECOGNISED: unclear/ambiguous -> resolved:false, escalate:false, response:"I'm sorry, I didn't catch that. Are you safe, need more time, or need a human agent?"

DISTRESS DETECTION:
- distress:null — no signals
- distress:"low" — hesitation, uncertainty ("I think", "maybe", "not sure")
- distress:"medium" — stress words ("stuck", "trapped", "can't", "worried", "scared", "problem")
- distress:"high" — emergency words ("help", "hurt", "pain", "emergency", "accident", "injured", "danger", "SOS")

DISTRESS ESCALATION: distress "high" -> always escalate:true. distress "medium" -> escalate:true unless SAFE_CONFIRMATION with confidence > 0.85.
distress_signals: brief description or null.

Confidence: >0.90 clear | <0.60 hesitation/unclear -> UNRECOGNISED. When in doubt, ask again.

CRITICAL SAFETY RULE — OVERRIDE EVERYTHING ELSE:
If the worker's message contains ANY of these physical indicators, ALWAYS return REQUEST_HUMAN regardless of positive statements:
- Fall/falling: "fell", "fall", "fallen", "tripped", "slipped"
- Pain: "hurts", "pain", "ache", "sore", "burning"
- Injury: "broke", "broken", "cut", "bleeding", "injured", "hurt"
- Dizziness: "dizzy", "faint", "blurry", "can't see"
- Entrapment: "stuck", "trapped", "can't move", "pinned"
- Fear: "scared", "afraid", "worried", "panicking"

"I just fell down but I'm okay" -> REQUEST_HUMAN (fell = physical incident)
"I'm fine I promise" alone with NO physical indicators -> SAFE_CONFIRMATION
The presence of reassurance ("I promise", "I'm okay") does NOT cancel physical incident indicators.`;

const WORKER_INTRO = `Hello, this is the Aware360 Monitoring Center. We noticed you missed your scheduled check-in. Are you okay? Please say "I'm safe", "I need more time", or "connect me to an agent".`;

const INTENT_CACHE_MAP = {
  'SAFE_CONFIRMATION': 'safe_confirmation',
  'REQUEST_HUMAN': 'request_human',
  'UNRECOGNISED': 'unrecognised',
};

const PUBLIC_URL = process.env.PUBLIC_URL;
if (!PUBLIC_URL) throw new Error('PUBLIC_URL is not set in .env');
const WS_URL = PUBLIC_URL.replace('https://', 'wss://').replace('http://', 'ws://');

// ─── TwiML webhook ───
app.post('/incoming-call', function(req, res) {
  const caller = (req.body && req.body.From) ? req.body.From : 'unknown';
  console.log('[Twilio] Incoming call from:', caller);
  const wsUrl = WS_URL + '/twilio';
  res.type('text/xml');
  res.send('<?xml version="1.0" encoding="UTF-8"?><Response><Connect><Stream url="' + wsUrl + '"><Parameter name="caller" value="' + caller + '"/></Stream></Connect></Response>');
});

// ─── Shared functions ───
async function classifyIntent(transcript, model) {
  const stream = anthropic.messages.stream({
    model: model || 'claude-haiku-4-5',
    max_tokens: 300,
    system: SYSTEM_PROMPT,
    messages: [{ role: 'user', content: 'Worker said: "' + transcript + '"' }]
  });
  let fullText = '';
  let usage = { input_tokens: 0, output_tokens: 0 };
  for await (const chunk of stream) {
    if (chunk.type === 'content_block_delta' && chunk.delta && chunk.delta.type === 'text_delta') {
      fullText += chunk.delta.text;
    }
    if (chunk.type === 'message_delta' && chunk.usage) {
      usage.output_tokens = chunk.usage.output_tokens || 0;
    }
    if (chunk.type === 'message_start' && chunk.message && chunk.message.usage) {
      usage.input_tokens = chunk.message.usage.input_tokens || 0;
    }
  }
  const clean = fullText.trim();
  let parsed;
  try { parsed = JSON.parse(clean); } catch (e) {
    const match = clean.match(/\{[\s\S]*\}/);
    if (match) parsed = JSON.parse(match[0]);
    else throw new Error('Invalid JSON: ' + clean);
  }
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
    result.escalate = true;
    result.resolved = false;
    if (result.intent !== 'REQUEST_HUMAN') {
      result.response = 'I can hear this may be urgent. I am connecting you to a live agent right now. Please stay on the line.';
    }
    console.log('[DISTRESS HIGH] Auto-escalating | signals:', result.distress_signals);
  } else if (result.distress === 'medium' && !(result.intent === 'SAFE_CONFIRMATION' && result.confidence > 0.85)) {
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
  let deepgramWs = null;
  let sessionState = 'GREETING';
  let unrecognisedCount = 0;
  let utteranceStartTime = null;
  let silenceTimer = null;

  function clearSilenceTimer() { if (silenceTimer) { clearTimeout(silenceTimer); silenceTimer = null; } }

  function sendAudioToTwilio(audioBuffer) {
    if (ws.readyState !== WebSocket.OPEN || !streamSid) return;
    ws.send(JSON.stringify({
      event: 'media',
      streamSid: streamSid,
      media: { payload: audioBuffer.toString('base64') }
    }));
  }

  async function speakText(text, cacheKey) {
  try {
    let mp3Buffer;
    // Twilio için her zaman ElevenLabs'tan canlı üret — cache mulaw dönüşümünde sorun çıkarıyor
    mp3Buffer = await synthesizeSpeech(text);
    console.log('[Twilio TTS] Generated fresh audio, bytes:', mp3Buffer.length);
      const mulawBuffer = await mp3ToMulaw(mp3Buffer);
      sendAudioToTwilio(mulawBuffer);
    } catch (e) {
      console.error('[Twilio TTS Error]', e.message);
    }
  }

  async function handleTwilioTranscript(finalTranscript) {
    if (!finalTranscript || finalTranscript.trim() === '') return;
    console.log('[Twilio Transcript]', finalTranscript);
    sessionState = 'PROCESSING';
    try {
      let result = await classifyIntent(finalTranscript, 'claude-haiku-4-5');
      if (result.confidence < 0.45) {
        result.intent = 'UNRECOGNISED'; result.resolved = false; result.escalate = false;
        result.response = "I'm sorry, I didn't catch that. Are you safe, need more time, or need a human agent?";
      }
      result = applyDistressLogic(result);
      console.log('[Twilio Intent]', result.intent, '| conf:', result.confidence, '| distress:', result.distress || 'none');
      const cacheKey = INTENT_CACHE_MAP[result.intent];
      await speakText(result.response, cacheKey);
      if (result.intent === 'UNRECOGNISED') {
        unrecognisedCount++;
        if (unrecognisedCount > 1) {
          await speakText("I'm unable to process your response. A human agent will follow up shortly.", null);
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
    const params = new URLSearchParams({
      model: 'nova-3', language: 'en-US', punctuate: 'true',
      smart_format: 'true', interim_results: 'true',
      utterance_end_ms: '1200', vad_events: 'true',
      encoding: 'mulaw', sample_rate: '8000', channels: '1'
    });
    deepgramWs = new WebSocket('wss://api.deepgram.com/v1/listen?' + params.toString() + '&keyterm=help&keyterm=SOS&keyterm=emergency&keyterm=hurt&keyterm=pain&keyterm=injured&keyterm=injury&keyterm=stuck&keyterm=trapped&keyterm=dizzy&keyterm=bleeding&keyterm=fallen&keyterm=fell&keyterm=scared&keyterm=afraid&keyterm=agent&keyterm=human&keyterm=danger&keyterm=accident', {
      headers: { Authorization: 'Token ' + dgApiKey }
    });
    deepgramWs.on('open', function() { console.log('[Twilio Deepgram] Connected'); });
    deepgramWs.on('message', async function(data) {
      try {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'Results' && msg.channel && msg.channel.alternatives && msg.channel.alternatives[0]) {
          const text = msg.channel.alternatives[0].transcript;
          if (text && text.trim() && msg.is_final && msg.speech_final && sessionState === 'LISTENING') {
            clearSilenceTimer();
            await handleTwilioTranscript(text);
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
        streamSid = msg.start.streamSid;
        console.log('[Twilio] Stream started | SID:', streamSid);
        startTwilioDeepgram();
        sessionState = 'GREETING';
        setTimeout(async function() {
          await speakText(WORKER_INTRO, 'greeting');
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
  let selectedModel = 'claude-haiku-4-5';
  let ttsMode = 'elevenlabs';
  let utteranceStartTime = null;
  const SILENCE_MS = 4000;

  function send(obj) { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj)); }
  function clearSilenceTimer() { if (silenceTimer) { clearTimeout(silenceTimer); silenceTimer = null; } }

  async function handleTranscript(finalTranscript, sttMs) {
    if (!finalTranscript || finalTranscript.trim() === '') return;
    console.log('[Transcript]', finalTranscript, '| STT:', sttMs + 'ms');
    send({ type: 'transcript', text: finalTranscript });
    if (sttMs) send({ type: 'stt_latency', ms: sttMs });
    sessionState = 'PROCESSING';
    send({ type: 'stage', stage: 'llm', status: 'processing' });
    const llmStart = Date.now();
    try {
      let result = await classifyIntent(finalTranscript, selectedModel);
      const llmMs = Date.now() - llmStart;
      if (result.confidence < 0.45) {
        result.intent = 'UNRECOGNISED'; result.resolved = false; result.escalate = false;
        result.response = "I'm sorry, I didn't catch that clearly. Could you say if you're safe, need more time, or need a human agent?";
      } else if (result.confidence < 0.85) {
        result.low_confidence = true;
      }
      result = applyDistressLogic(result);
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
        if (cacheKey && audioCache[cacheKey]) {
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
    const params = new URLSearchParams({
      model: 'nova-3', language: 'en-US', punctuate: 'true',
      smart_format: 'true', interim_results: 'true',
      utterance_end_ms: '1200', vad_events: 'true'
    });
    if (isOgg) { params.set('encoding', 'opus'); params.set('container', 'ogg'); }
    deepgramWs = new WebSocket('wss://api.deepgram.com/v1/listen?' + params.toString() + '&keyterm=help&keyterm=SOS&keyterm=emergency&keyterm=hurt&keyterm=pain&keyterm=injured&keyterm=injury&keyterm=stuck&keyterm=trapped&keyterm=dizzy&keyterm=bleeding&keyterm=fallen&keyterm=fell&keyterm=scared&keyterm=afraid&keyterm=agent&keyterm=human&keyterm=danger&keyterm=accident', {
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
        if (msg.type === 'SpeechStarted') utteranceStartTime = null;
        if (msg.type === 'Results' && msg.channel && msg.channel.alternatives && msg.channel.alternatives[0]) {
          const text = msg.channel.alternatives[0].transcript;
          if (text && text.trim()) {
            if (!utteranceStartTime) utteranceStartTime = Date.now();
            send({ type: 'interim_transcript', text: text, is_final: msg.is_final });
            if (msg.is_final && msg.speech_final && sessionState === 'LISTENING') {
              clearSilenceTimer();
              const sttMs = utteranceStartTime ? Date.now() - utteranceStartTime : null;
              utteranceStartTime = null;
              transcript = text;
              const toProcess = transcript; transcript = '';
              await handleTranscript(toProcess, sttMs);
            }
          }
        }
        if (msg.type === 'UtteranceEnd' && transcript && sessionState === 'LISTENING') {
          clearSilenceTimer();
          const sttMs = utteranceStartTime ? Date.now() - utteranceStartTime : null;
          utteranceStartTime = null;
          const toProcess = transcript; transcript = '';
          await handleTranscript(toProcess, sttMs);
        }
      } catch (e) {}
    });
    deepgramWs.on('error', function(e) { console.error('[Deepgram] Error:', e.message); send({ type: 'stage', stage: 'stt', status: 'error' }); });
    deepgramWs.on('close', function(code, reason) { console.log('[Deepgram] Closed', code, reason.toString()); send({ type: 'stage', stage: 'stt', status: 'disconnected' }); });
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
        if (msg.type === 'mock_transcript' && sessionState === 'LISTENING') await handleTranscript(msg.text, null);
        if (msg.type === 'stop') {
          if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) deepgramWs.close();
          clearSilenceTimer(); sessionState = 'ENDED';
        }
      } else {
        if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) deepgramWs.send(data);
        else audioBuffer = Buffer.concat([audioBuffer, data]);
      }
    } catch (e) { console.error('[WS Message Error]', e.message, e.stack); }
  });

  ws.on('close', function() {
    console.log('[WS] Client disconnected');
    clearSilenceTimer();
    if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) deepgramWs.close();
  });
});

app.get('/health', function(req, res) {
  res.json({ status: 'ok', ngrok: PUBLIC_URL, twilio_ws: PUBLIC_URL.replace('https://', 'wss://') + '/twilio', apis: {
    deepgram: !!process.env.DEEPGRAM_API_KEY && process.env.DEEPGRAM_API_KEY !== 'your_deepgram_key',
    anthropic: !!process.env.ANTHROPIC_API_KEY && process.env.ANTHROPIC_API_KEY !== 'your_anthropic_key',
    elevenlabs: !!process.env.ELEVENLABS_API_KEY && process.env.ELEVENLABS_API_KEY !== 'your_elevenlabs_key'
  }});
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, function() {
  console.log('\n🎙️  Voice AI Demo: http://localhost:' + PORT);
  console.log('📞  Twilio webhook: ' + PUBLIC_URL + '/incoming-call');
  console.log('🔌  Twilio WS path: ' + PUBLIC_URL.replace('https://', 'wss://') + '/twilio');
  console.log('Cascaded Pipeline: Deepgram Nova-3 -> Claude -> ElevenLabs\n');
});