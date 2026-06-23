const fs = require('fs');
const path = require('path');

const envFile = fs.readFileSync('.env', 'utf8');
envFile.split('\n').forEach(line => {
  const [key, ...val] = line.trim().split('=');
  if (key && val.length) process.env[key] = val.join('=');
});

const express = require('express');
const WebSocket = require('ws');
const http = require('http');
const axios = require('axios');
const Anthropic = require('@anthropic-ai/sdk');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.json());
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
      console.log(`[Cache] Loaded: ${key}`);
    }
  });
  return cache;
}
const audioCache = loadAudioCache();

const SYSTEM_PROMPT = `You are an automated safety monitoring system for Aware360, a lone worker safety platform handling a MISSED CHECK-IN alert for an industrial/field worker.

Respond ONLY with a JSON object:
{
  "intent": "<SAFE_CONFIRMATION|DELAYED_CHECKIN|REQUEST_HUMAN|UNRECOGNISED>",
  "confidence": <0.0-1.0>,
  "response": "<spoken response>",
  "resolved": <true|false>,
  "escalate": <true|false>,
  "delay_minutes": <null|number>
}

Rules:
- SAFE_CONFIRMATION: "yes/fine/good/safe/okay/yep" → resolved:true, response:"Got it — I've reset your safety timer. Stay safe."
- DELAYED_CHECKIN: "late/minutes/time/reset/almost done" → resolved:true, delay_minutes: number or 15, response:"Understood. I've extended your check-in timer by [X] minutes. Please check in when ready."
- REQUEST_HUMAN: "person/human/agent/help/SOS" → escalate:true, response:"Of course. I'm flagging this for a live agent — someone will call you back shortly. Keep your phone available."
- UNRECOGNISED: anything else → response:"I'm sorry, I didn't catch that. Are you safe, need more time, or need a human agent?"

Return ONLY valid JSON.`;

const WORKER_INTRO = `Hello, this is the Aware360 Monitoring Center. We noticed you missed your scheduled check-in. Are you okay? Please say "I'm safe", "I need more time", or "connect me to an agent".`;

const INTENT_CACHE_MAP = {
  'SAFE_CONFIRMATION': 'safe_confirmation',
  'REQUEST_HUMAN': 'request_human',
  'UNRECOGNISED': 'unrecognised',
};

// Stream Claude response sentence by sentence
async function* streamClassifyIntent(transcript, model) {
  const stream = anthropic.messages.stream({
    model: model || 'claude-haiku-4-5',
    max_tokens: 256,
    system: SYSTEM_PROMPT,
    messages: [{ role: 'user', content: `Worker said: "${transcript}"` }]
  });

  let fullText = '';
  for await (const chunk of stream) {
    if (chunk.type === 'content_block_delta' && chunk.delta?.type === 'text_delta') {
      fullText += chunk.delta.text;
    }
  }

  // Parse JSON from full response
  const clean = fullText.trim();
  try {
    yield JSON.parse(clean);
  } catch (e) {
    const match = clean.match(/\{[\s\S]*\}/);
    if (match) yield JSON.parse(match[0]);
    else throw new Error('Invalid JSON: ' + clean);
  }
}

// Stream ElevenLabs TTS, yield base64 chunks
async function* streamTTS(text, apiKey, voiceId) {
  const response = await axios.post(
    `https://api.elevenlabs.io/v1/text-to-speech/${voiceId}/stream`,
    {
      text,
      model_id: 'eleven_flash_v2_5',
      voice_settings: { stability: 0.5, similarity_boost: 0.75 },
      optimize_streaming_latency: 4
    },
    {
      headers: { 'xi-api-key': apiKey, 'Content-Type': 'application/json', 'Accept': 'audio/mpeg' },
      responseType: 'stream'
    }
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

wss.on('connection', (ws) => {
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

  async function handleTranscript(finalTranscript, sttMs) {
    if (!finalTranscript || finalTranscript.trim() === '') return;
    console.log(`[Transcript] ${finalTranscript} | STT: ${sttMs}ms`);
    send({ type: 'transcript', text: finalTranscript });
    if (sttMs) send({ type: 'stt_latency', ms: sttMs });

    sessionState = 'PROCESSING';
    send({ type: 'stage', stage: 'llm', status: 'processing' });
    const llmStart = Date.now();

    try {
      // Stream LLM response
      let result = null;
      for await (const parsed of streamClassifyIntent(finalTranscript, selectedModel)) {
        result = parsed;
      }
      const llmMs = Date.now() - llmStart;

      // Confidence score gating
      const HIGH_CONF = 0.85;
      const LOW_CONF = 0.60;
      if (result.confidence < LOW_CONF) {
        console.log(`[Intent] Low confidence (${result.confidence}) — treating as UNRECOGNISED`);
        result.intent = 'UNRECOGNISED';
        result.resolved = false;
        result.escalate = false;
        result.response = "I'm sorry, I didn't catch that clearly. Could you say if you're safe, need more time, or need a human agent?";
      } else if (result.confidence < HIGH_CONF) {
        console.log(`[Intent] Medium confidence (${result.confidence}) — proceeding with caution`);
        result.low_confidence = true;
      }

      console.log(`[Intent] ${result.intent} | conf: ${result.confidence} | LLM: ${llmMs}ms`);
      send({ type: 'intent', data: result });
      send({ type: 'stage', stage: 'llm', status: 'done', ms: llmMs });

      // TTS - use cache or stream
      send({ type: 'stage', stage: 'tts', status: 'processing' });
      const ttsStart = Date.now();

      if (ttsMode === 'browser') {
        send({ type: 'stage', stage: 'tts', status: 'done', ms: 0 });
        send({ type: 'response', text: result.response, audio: null, resolved: result.resolved, escalate: result.escalate });
      } else {
        const cacheKey = INTENT_CACHE_MAP[result.intent];
        if (cacheKey && audioCache[cacheKey]) {
          // Cache hit — send as single chunk instantly
          console.log(`[Cache] Hit: ${cacheKey}`);
          const ttsMs = Date.now() - ttsStart;
          send({ type: 'stage', stage: 'tts', status: 'done', ms: ttsMs });
          send({ type: 'response', text: result.response, audio: audioCache[cacheKey], resolved: result.resolved, escalate: result.escalate });
        } else {
          // Stream TTS chunks
          const apiKey = process.env.ELEVENLABS_API_KEY;
          const voiceId = process.env.ELEVENLABS_VOICE_ID || 'EXAVITQu4vr4xnSDxMaL';
          let firstChunk = true;
          let chunkIndex = 0;

          try {
            send({ type: 'audio_stream_start', text: result.response, resolved: result.resolved, escalate: result.escalate });
            for await (const chunk of streamTTS(result.response, apiKey, voiceId)) {
              if (firstChunk) {
                const ttsMs = Date.now() - ttsStart;
                send({ type: 'stage', stage: 'tts', status: 'done', ms: ttsMs });
                console.log(`[TTS] First chunk: ${ttsMs}ms`);
                firstChunk = false;
              }
              send({ type: 'audio_chunk', chunk, index: chunkIndex++ });
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
    if (!dgApiKey || dgApiKey === 'your_deepgram_key') {
      send({ type: 'log', message: 'Deepgram API key not set — mock STT mode' }); return;
    }
    const isOgg = clientMimeType.includes('ogg');
    const params = new URLSearchParams({
      model: 'nova-3',
      language: 'en-US',
      punctuate: 'true',
      smart_format: 'true',
      interim_results: 'true',
      utterance_end_ms: '1200',
      vad_events: 'true',
    });
    if (isOgg) { params.set('encoding', 'opus'); params.set('container', 'ogg'); }
    const dgWsUrl = 'wss://api.deepgram.com/v1/listen?' + params.toString();
    console.log('[Deepgram] Connecting, mimeType:', clientMimeType);
    deepgramWs = new WebSocket(dgWsUrl, { headers: { Authorization: `Token ${dgApiKey}` } });

    deepgramWs.on('open', () => {
      console.log('[Deepgram] Connected');
      send({ type: 'stage', stage: 'stt', status: 'connected' });
      if (audioBuffer.length > 0) { deepgramWs.send(audioBuffer); audioBuffer = Buffer.alloc(0); }
    });

    deepgramWs.on('message', async (data) => {
      try {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'SpeechStarted') { utteranceStartTime = Date.now(); }
        if (msg.type === 'Results' && msg.channel?.alternatives?.[0]) {
          const text = msg.channel.alternatives[0].transcript;
          if (text && text.trim()) {
            if (!utteranceStartTime) utteranceStartTime = Date.now();
            send({ type: 'interim_transcript', text, is_final: msg.is_final });
            if (msg.is_final && msg.speech_final) {
              transcript = text;
              if (sessionState === 'LISTENING') {
                clearSilenceTimer();
                const sttMs = utteranceStartTime ? Date.now() - utteranceStartTime : null;
                utteranceStartTime = null;
                const toProcess = transcript; transcript = '';
                await handleTranscript(toProcess, sttMs);
              }
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

    deepgramWs.on('error', (e) => { console.error('[Deepgram] Error:', e.message); send({ type: 'stage', stage: 'stt', status: 'error' }); });
    deepgramWs.on('close', (code, reason) => { console.log('[Deepgram] Closed', code, reason.toString()); send({ type: 'stage', stage: 'stt', status: 'disconnected' }); });
  }

  function resetSilenceTimer() {
    clearSilenceTimer();
    silenceTimer = setTimeout(async () => {
      if (transcript && transcript.trim() && sessionState === 'LISTENING') {
        const sttMs = utteranceStartTime ? Date.now() - utteranceStartTime : null;
        utteranceStartTime = null;
        const toProcess = transcript; transcript = '';
        await handleTranscript(toProcess, sttMs);
      }
    }, SILENCE_MS);
  }

  function clearSilenceTimer() { if (silenceTimer) { clearTimeout(silenceTimer); silenceTimer = null; } }

  ws.on('message', async (data) => {
    try {
      if (typeof data === 'string' || (data instanceof Buffer && data[0] === 0x7b)) {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'mime_type') { clientMimeType = msg.mimeType; console.log('[Client] mimeType:', clientMimeType); }
        if (msg.type === 'set_model') { selectedModel = msg.model; console.log('[Model] Selected:', selectedModel); }
        if (msg.type === 'tts_mode') { ttsMode = msg.mode; console.log('[TTS] Mode:', ttsMode); }
        if (msg.type === 'start') {
          send({ type: 'log', message: 'Session started — Missed Check-In alert for Industrial Worker' });
          send({ type: 'stage', stage: 'stt', status: 'connecting' });
          startDeepgramStream();
          sessionState = 'GREETING';
          setTimeout(async () => {
            send({ type: 'stage', stage: 'tts', status: 'processing' });
            let introAudio = null;
            if (ttsMode === 'elevenlabs') {
              introAudio = audioCache['greeting'] || null;
              if (introAudio) { console.log('[Cache] Using cached greeting'); }
              else {
                try {
                  const apiKey = process.env.ELEVENLABS_API_KEY;
                  const voiceId = process.env.ELEVENLABS_VOICE_ID || 'EXAVITQu4vr4xnSDxMaL';
                  const chunks = [];
                  for await (const chunk of streamTTS(WORKER_INTRO, apiKey, voiceId)) chunks.push(chunk);
                  introAudio = chunks.join('');
                } catch (e) { console.error('[TTS Error]', e.message); }
              }
            }
            send({ type: 'stage', stage: 'tts', status: 'done' });
            send({ type: 'greeting', text: WORKER_INTRO, audio: introAudio });
            sessionState = 'LISTENING';
          }, 500);
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
    } catch (e) { console.error('[WS Message Error]', e.message); }
  });

  ws.on('close', () => {
    console.log('[WS] Client disconnected');
    clearSilenceTimer();
    if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) deepgramWs.close();
  });
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok', apis: {
    deepgram: !!process.env.DEEPGRAM_API_KEY && process.env.DEEPGRAM_API_KEY !== 'your_deepgram_key',
    anthropic: !!process.env.ANTHROPIC_API_KEY && process.env.ANTHROPIC_API_KEY !== 'your_anthropic_key',
    elevenlabs: !!process.env.ELEVENLABS_API_KEY && process.env.ELEVENLABS_API_KEY !== 'your_elevenlabs_key'
  }});
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n🎙️  Voice AI Demo running at http://localhost:${PORT}`);
  console.log('Cascaded Pipeline: Deepgram Nova-3 → Claude → ElevenLabs (streaming)\n');
});