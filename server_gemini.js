const fs = require('fs');
const path = require('path');

require('dotenv').config();

const express = require('express');
const WebSocket = require('ws');
const http = require('http');
const axios = require('axios');
const twilio = require('twilio');
const { insertCallStart, updateCallEnd, updateRecordingUrl, updateCallSessionState, listCalls, getCallBySid } = require('./db');
const { createNoiseEstimator } = require("./noiseEstimator");
const { createSileroVad } = require("./sileroVad");
const { frameStatsMulaw } = require("./audioLevel"); 

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ noServer: true });

app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(express.static(path.join(__dirname, 'public')));

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const GEMINI_MODEL = 'gemini-3.1-flash-live-preview';

const twilioClient = process.env.TWILIO_ACCOUNT_SID && process.env.TWILIO_AUTH_TOKEN
  ? twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN)
  : null;

const amdRedirectedCalls = new Set();

function buildTwilioWebhookUrl(req, pathname) {
  let base;
  if (process.env.NGROK_HOST) {
    base = 'https://' + String(process.env.NGROK_HOST).replace(/^https?:\/\//, '');
  } else {
    base = process.env.GEMINI_PUBLIC_URL || process.env.PUBLIC_URL || '';
  }
  base = String(base).replace(/\/$/, '');
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

const SYSTEM_PROMPT = `CRITICAL LANGUAGE RULE: This call is in English only. The worker speaks English. Always transcribe and interpret audio as English, even if the audio quality is poor, words are unclear, or the accent is unfamiliar. Never output text in Korean, Japanese, Chinese, or any language other than English. If you cannot understand the worker's English, respond with the UNCLEAR script in English.

SYSTEM: language=en-US. All input and output in English only.

You are an automated safety monitoring system for Aware360...

Your persona: "Aware360 Monitoring Center" — efficient, neutral, direct.

When the worker speaks:
1. Listen carefully to their response
2. Classify their intent: safe confirmation, needs more time, wants human agent, or unclear
3. Respond appropriately and concisely

Rules:
- SAFE: "Got it — I've reset your safety timer. Stay safe."
- DELAYED: "Understood. I've extended your check-in timer. Please check in when ready."  
- HUMAN AGENT: "Of course. I'm flagging this for a live agent — someone will call you back shortly."
- UNCLEAR: Always respond with exactly this phrase, nothing else: "Sorry, I didn't catch that. Could you repeat?"

CRITICAL SAFETY RULE: If the worker mentions ANY physical injury, pain, fall, or distress — even with positive statements — immediately escalate to human agent.

Keep responses short (under 15 words). Be direct and professional.

TOOL USE (MANDATORY):
- After EVERY worker utterance, FIRST call report_worker_intent with the intent and your confidence (0 to 1) BEFORE speaking.
Confidence reflects how clearly you HEARD the audio - if it was noisy, partial or unclear, report below 0.6.
- If status is "ok", reply with the normal script.
- Right after you say a closing line (timer reset, timer extended, agent escalation, unreachable), call finish_call with the outcome.

IMPORTANT: Only greet the worker ONCE at the very start of the conversation. Do not repeat the greeting.
CRITICAL: After the greeting, do NOT speak, acknowledge, or produce ANY output until the worker speaks. Do not say "standing by", "acknowledged", "waiting", or anything else. Complete silence until worker responds.`;

const WORKER_INTRO = `Hello, this is the Aware360 Monitoring Center. We noticed you missed your scheduled check-in. Are you okay? Please say "I'm safe", "I need more time", or "connect me to an agent".`;

let cachedGreetingChunks = null;
let cachedGreetingMimeType = null;

wss.on('connection', function(ws) {
  console.log('[Gemini WS] Client connected');

  let geminiWs = null;
  let aiTranscriptBuffer = '';
  let sessionState = 'GREETING';
  let clientMimeType = 'audio/webm';
  let actualSampleRate = 16000;
  let greetingSent = false;
  let isCachingGreeting = false;
  let workerSpeechEndTime = null; let turnStartTime = null;
  let workerSpeechActive = false;
  let workerSilenceTimer = null;
  let lastAudioSentTime = null;
  let setupTime = null;
  let firstAudioTime = null;
  let listeningTransitionTimer = null;

  function send(obj) {
    if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
  }

  function scheduleListeningTransition(reason) {
    if (listeningTransitionTimer) {
      clearTimeout(listeningTransitionTimer);
      listeningTransitionTimer = null;
    }
    listeningTransitionTimer = setTimeout(function() {
      sessionState = 'LISTENING';
      console.log('[Gemini] Auto-transition to LISTENING');
      listeningTransitionTimer = null;
    }, 2000);
  }

  function connectGemini() {
    const geminiUrl = `wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=${GEMINI_API_KEY}`;

    geminiWs = new WebSocket(geminiUrl);

    geminiWs.on('open', function() {
      console.log('[Gemini] Connected at', Date.now());

      const setup = {
  setup: {
    model: `models/${GEMINI_MODEL}`,
    generation_config: {
      response_modalities: ['AUDIO'],
      speech_config: {
        voice_config: {
          prebuilt_voice_config: {
            voice_name: 'Charon'
          }
        }
      }
    },
    realtime_input_config: {
  automatic_activity_detection: {
    disabled: false,
    end_of_speech_sensitivity: "END_SENSITIVITY_LOW",
    start_of_speech_sensitivity: "START_SENSITIVITY_LOW",
    silence_duration_ms: 1500,
    prefix_padding_ms: 200,
  }
},
    // language_codes removed — caused WebSocket 1007 on Gemini Live setup
    input_audio_transcription: {},
    output_audio_transcription: {},
    system_instruction: {
      parts: [{ text: SYSTEM_PROMPT }]
    }
  }
};
      geminiWs.send(JSON.stringify(setup));
    });

    geminiWs.on('message', async function(data) {
      try {
        const msg = JSON.parse(data.toString());
console.log('[Gemini RAW]', JSON.stringify(msg).substring(0, 300));

        // Setup complete
        if (msg.setupComplete) {
  console.log('[Gemini] Setup complete at', Date.now());
  setupTime = Date.now();
  send({ type: 'stage', stage: 'stt', status: 'connected' });
  send({ type: 'stage', stage: 'llm', status: 'connected' });
  send({ type: 'stage', stage: 'tts', status: 'connected' });

  if (!greetingSent) {
  greetingSent = true;

  if (cachedGreetingChunks) {
    console.log('[Gemini] Using cached greeting,', cachedGreetingChunks.length, 'chunks');
    for (const chunk of cachedGreetingChunks) {
      send({ type: 'audio_chunk', chunk, index: 0, mimeType: cachedGreetingMimeType });
      await new Promise(r => setTimeout(r, 20));
    }
    send({ type: 'audio_stream_end' });
    send({ type: 'greeting', text: WORKER_INTRO, audio: null });
    // Stay in GREETING until client has time to play audio — no playback_done wait
    scheduleListeningTransition('cached greeting');
    return; // cache hit — do not send greetingMsg to Gemini
  } else {
    console.log('[Gemini] No cache, generating greeting fresh');
    cachedGreetingChunks = [];
    isCachingGreeting = true;
    const greetingMsg = {
      client_content: {
        turns: [{
          role: 'user',
          parts: [{ text: `[SYSTEM] Greet the worker now with exactly this message, word for word, and nothing else: "${WORKER_INTRO}"` }]
        }],
        turn_complete: true
      }
    };
    geminiWs.send(JSON.stringify(greetingMsg));
    // Stay GREETING until turnComplete + 2s timeout
  }
}
}

        // Audio response from Gemini
        if (msg.serverContent) {
  const content = msg.serverContent;

  if (content.modelTurn && content.modelTurn.parts) {
    for (const part of content.modelTurn.parts) {
      if (part.inlineData && part.inlineData.mimeType && part.inlineData.mimeType.startsWith('audio/')) {
  if (turnStartTime && !firstAudioTime) {
    firstAudioTime = Date.now();
    const ttfa = firstAudioTime - turnStartTime;
    console.log('[Gemini] TTFA (end-of-speech → first audio chunk):', ttfa + 'ms');
    send({ type: 'gemini_turn_latency', ms: ttfa });
    turnStartTime = null;
    lastAudioSentTime = null;
  }
  console.log('[Gemini] Audio chunk received, mimeType:', part.inlineData.mimeType, 'size:', part.inlineData.data.length);
  if (isCachingGreeting) {
    cachedGreetingChunks.push(part.inlineData.data);
    cachedGreetingMimeType = part.inlineData.mimeType;
  }
  send({ type: 'audio_chunk', chunk: part.inlineData.data, index: 0, mimeType: part.inlineData.mimeType });
}
      if (part.text) {
        console.log('[Gemini] Text:', part.text);
        send({ type: 'transcript', text: part.text, role: 'ai' });
      }
    }
  }

  if (content.outputTranscription && content.outputTranscription.text) {
  aiTranscriptBuffer += content.outputTranscription.text;
}

          if (content.turnComplete) {
            if (aiTranscriptBuffer) {
    send({ type: 'transcript', text: aiTranscriptBuffer, role: 'ai' });
    aiTranscriptBuffer = '';
  }
            send({ type: 'audio_stream_end' });
            if (sessionState === 'GREETING' || isCachingGreeting) {
              send({ type: 'greeting', text: WORKER_INTRO, audio: null });
              isCachingGreeting = false;
              console.log('[Gemini] Greeting audio done — auto LISTENING in 2s');
              scheduleListeningTransition('greeting turnComplete');
            }
          }
        }

        // Input transcription (what worker said)
if (msg.serverContent && msg.serverContent.inputTranscription) {
  console.log('[Gemini] Worker said:', msg.serverContent.inputTranscription.text);
  send({ type: 'transcript', text: msg.serverContent.inputTranscription.text, role: 'worker' });
  if (sessionState === 'LISTENING') {
    // inputTranscription fires when Gemini detects end-of-speech — use this as turn start
    turnStartTime = Date.now();
    firstAudioTime = null;
  }
}

        if (msg.usageMetadata && turnStartTime) {
          const totalTurnMs = Date.now() - turnStartTime;
          console.log('[Gemini] Full turn duration:', totalTurnMs + 'ms (not shown in UI — using TTFA instead)');
        }

        if (msg.usageMetadata) {
          const promptTokens = msg.usageMetadata.promptTokenCount || 0;
          const responseTokens = msg.usageMetadata.responseTokenCount || 0;
          send({ type: 'gemini_token_usage', input: promptTokens, output: responseTokens });
        }
      } catch(e) {
        console.error('[Gemini] Message error:', e.message);
      }
    });

    geminiWs.on('error', function(e) {
      console.error('[Gemini] Error:', e.message);
      send({ type: 'stage', stage: 'stt', status: 'error' });
    });

    geminiWs.on('close', function(code, reason) {
      console.log('[Gemini] Closed', code, reason.toString());
    });
  }

  ws.on('message', async function(data) {
    try {
      let isJson = false;
      let msg = null;
      try {
        msg = JSON.parse(data.toString());
        isJson = true;
      } catch (e) {
        isJson = false;
      }

      if (isJson) {
        if (msg.type === 'mime_type') {
          clientMimeType = msg.mimeType;
        }

        if (msg.type === 'gemini_sample_rate') {
          actualSampleRate = msg.rate;
          console.log('[Gemini] Client sample rate:', actualSampleRate);
        }

        if (msg.type === 'start') {
          console.log('[Gemini] start received at', Date.now());
          send({ type: 'log', message: 'Gemini Live API session started' });
          send({ type: 'stage', stage: 'stt', status: 'connecting' });
          connectGemini();
        }

        if (msg.type === 'mock_transcript' && sessionState === 'LISTENING') {
          if (geminiWs && geminiWs.readyState === WebSocket.OPEN) {
            const textMsg = {
              client_content: {
                turns: [{
                  role: 'user',
                  parts: [{ text: msg.text }]
                }],
                turn_complete: true
              }
            };
            geminiWs.send(JSON.stringify(textMsg));
          }
        }

        if (msg.type === 'stop') {
          if (listeningTransitionTimer) {
            clearTimeout(listeningTransitionTimer);
            listeningTransitionTimer = null;
          }
          if (geminiWs && geminiWs.readyState === WebSocket.OPEN) geminiWs.close();
          sessionState = 'ENDED';
        }

      } else {
        // Binary audio — file uploads are large PCM blobs; mic chunks are small.
        // Gate file sends on LISTENING (retry up to 3× with 500ms wait).
        const buf = Buffer.from(data);
        const CHUNK_SIZE = 3200; // ~100ms @ 16kHz mono 16-bit
        const isFileUpload = buf.length > CHUNK_SIZE;

        if (isFileUpload) {
          let ready = sessionState === 'LISTENING';
          for (let attempt = 0; !ready && attempt < 3; attempt++) {
            console.log('[Gemini] File upload waiting for LISTENING (attempt ' + (attempt + 1) + '/3), state=' + sessionState);
            await new Promise(function(r) { setTimeout(r, 500); });
            ready = sessionState === 'LISTENING';
          }
          if (!ready) {
            console.warn('[Gemini] Dropping file upload — not LISTENING after 3 attempts (state=' + sessionState + ')');
            return;
          }
        } else if (sessionState !== 'LISTENING') {
          // Drop live mic audio during GREETING / non-listening states
          return;
        }

        if (geminiWs && geminiWs.readyState === WebSocket.OPEN) {
          for (let offset = 0; offset < buf.length; offset += CHUNK_SIZE) {
            const slice = buf.subarray(offset, Math.min(offset + CHUNK_SIZE, buf.length));
            const audioMsg = {
              realtime_input: {
                audio: {
                  data: slice.toString('base64'),
                  mimeType: 'audio/pcm;rate=' + actualSampleRate
                }
              }
            };
            geminiWs.send(JSON.stringify(audioMsg));
            lastAudioSentTime = Date.now();
            if (offset + CHUNK_SIZE < buf.length) {
              await new Promise(function(r) { setTimeout(r, 20); });
            }
          }
          // Signal end of audio so Gemini commits the turn (file uploads especially)
          if (isFileUpload) {
            geminiWs.send(JSON.stringify({ realtimeInput: { audioStreamEnd: true } }));
            console.log('[Gemini] File upload done — sent audioStreamEnd');
          }
        }
      }
    } catch(e) {
      console.error('[WS Error]', e.message);
    }
  });

  ws.on('close', function() {
    console.log('[Gemini WS] Client disconnected');
    if (listeningTransitionTimer) {
      clearTimeout(listeningTransitionTimer);
      listeningTransitionTimer = null;
    }
    if (workerSilenceTimer) clearTimeout(workerSilenceTimer);
    if (geminiWs && geminiWs.readyState === WebSocket.OPEN) geminiWs.close();
  });

  ws.on('error', function(e) {
    console.error('[WS Error]', e.message);
  });
});

// ─── Twilio WebSocket (Gemini pipeline) ───────────────────────────────────────
const { spawn } = require('child_process');
const twilioWss = new WebSocket.Server({ noServer: true });

server.on('upgrade', function(request, socket, head) {
  const pathname = new URL(request.url, 'http://localhost').pathname;
  if (pathname === '/twilio-gemini') {
    twilioWss.handleUpgrade(request, socket, head, function(ws) {
      twilioWss.emit('connection', ws, request);
    });
  } else {
    // Browser WebSocket (any other path)
    wss.handleUpgrade(request, socket, head, function(ws) {
      wss.emit('connection', ws, request);
    });
  }
});

// ─── Pure JS mulaw codecs (no ffmpeg — instant, ordered) ─────────────────────
const MULAW_BIAS = 0x84;
const MULAW_MAX = 32635;

function linearToMulawSample(sample) {
  let sign = (sample >> 8) & 0x80;
  if (sign) sample = -sample;
  if (sample > MULAW_MAX) sample = MULAW_MAX;
  sample += MULAW_BIAS;
  let exponent = 7;
  for (let mask = 0x4000; (sample & mask) === 0 && exponent > 0; mask >>= 1) exponent--;
  const mantissa = (sample >> (exponent + 3)) & 0x0F;
  return ~(sign | (exponent << 4) | mantissa) & 0xFF;
}

function mulawToLinearSample(mulaw) {
  mulaw = ~mulaw & 0xFF;
  const sign = mulaw & 0x80;
  const exponent = (mulaw >> 4) & 0x07;
  const mantissa = mulaw & 0x0F;
  let sample = ((mantissa << 3) + MULAW_BIAS) << exponent;
  sample -= MULAW_BIAS;
  return sign ? -sample : sample;
}

// Gemini PCM 24kHz → Twilio mulaw 8kHz (downsample 3:1 + encode)
function pcmToMulaw(pcm24kBuffer) {
  const samples24k = pcm24kBuffer.length >> 1;
  const samples8k = Math.floor(samples24k / 3);
  const out = Buffer.alloc(samples8k);
  for (let i = 0; i < samples8k; i++) {
    // Average 3 samples for basic anti-aliasing
    const s0 = pcm24kBuffer.readInt16LE(i * 6);
    const s1 = (i * 6 + 2 < pcm24kBuffer.length - 1) ? pcm24kBuffer.readInt16LE(i * 6 + 2) : s0;
    const s2 = (i * 6 + 4 < pcm24kBuffer.length - 1) ? pcm24kBuffer.readInt16LE(i * 6 + 4) : s1;
    const avg = Math.round((s0 + s1 + s2) / 3);
    out[i] = linearToMulawSample(avg);
  }
  return out;
}

// Twilio mulaw 8kHz → Gemini PCM 16kHz (decode + upsample 1:2)
function mulawToPcm(mulawBuffer) {
  const out = Buffer.alloc(mulawBuffer.length * 4); // each mulaw byte → 2 samples × 2 bytes
  for (let i = 0; i < mulawBuffer.length; i++) {
    const sample = mulawToLinearSample(mulawBuffer[i]);
    const next = (i + 1 < mulawBuffer.length) ? mulawToLinearSample(mulawBuffer[i + 1]) : sample;
    const mid = Math.round((sample + next) / 2); // linear interpolation
    out.writeInt16LE(sample, i * 4);
    out.writeInt16LE(mid, i * 4 + 2);
  }
  return out;
}

const TWILIO_WORKER_INTRO = `Hello, this is the Aware360 Monitoring Center. We noticed you missed your scheduled check-in. Are you okay?`;

twilioWss.on('connection', function(ws) {
  console.log('[Twilio-Gemini WS] Connected');
  let streamSid = null;
  let callLog = {
    callSid: null,
    streamSid: null,
    workerPhone: null,
    startTime: new Date().toISOString(),
    transcript: [],
    finalState: null,
    finalIntent: null,
    recordingUrl: null,
    silencePromptCount: 0,
    ttfa_ms: null,
  };
  let geminiWs = null;
  let sessionState = 'GREETING';
  let pcmBuffer = Buffer.alloc(0);
  let flushInterval = null;
  let silenceInterval = null;
  let twilioAiTranscript = '';
  let workerSilenceTimer = null;
  let silencePromptCount = 0;
  let twilioSetupTime = null;
  let twilioFirstAudioTime = null;
  const noise = createNoiseEstimator({ frameMs: 20 });
  const vad = createSileroVad({ sampleRate: 8000 });
  let audioClockMs = 0;
  let agentPlaybackUntil = 0;

  function isSpuriousUnclearResponse(text) {
    const t = (text || '').toLowerCase();
    return t.includes('sorry') && t.includes('repeat')
      && !t.includes('aware360') && !t.includes('monitoring center');
  }

  function startAudioFlush() {
    if (flushInterval) return;
    flushInterval = setInterval(function() {
      if (pcmBuffer.length > 0
        && geminiWs && geminiWs.readyState === WebSocket.OPEN
        && sessionState === 'LISTENING') {
        const toSend = pcmBuffer;
        pcmBuffer = Buffer.alloc(0);
        geminiWs.send(JSON.stringify({
          realtime_input: {
            audio: {
              data: toSend.toString('base64'),
              mimeType: 'audio/pcm;rate=16000'
            }
          }
        }));
      }
    }, 100);
  }

  function setCallOutcome(state, intent) {
    callLog.finalState = state;
    if (intent) callLog.finalIntent = intent;
  }

  function startWorkerSilenceTimer() {
    clearWorkerSilenceTimer();
    workerSilenceTimer = setTimeout(() => {
      if (sessionState !== 'LISTENING') return;
      silencePromptCount++;
      if (silencePromptCount === 1) {
        console.log('[Twilio-Gemini] 8s silence — prompting worker');
        if (geminiWs && geminiWs.readyState === WebSocket.OPEN) {
          geminiWs.send(JSON.stringify({
            client_content: {
              turns: [{ role: 'user', parts: [{ text: '[SYSTEM] The worker has been silent for 8 seconds. Say exactly: "Are you still there? Please respond if you can hear me."' }] }],
              turn_complete: true
            }
          }));
        }
        startWorkerSilenceTimer();
      } else {
        console.log('[Twilio-Gemini] 16s total silence — likely voicemail/unreachable, ending call');
        sessionState = 'ENDED';
        setCallOutcome('NO_ANSWER', callLog.finalIntent || 'UNRECOGNISED');
        clearWorkerSilenceTimer();
        if (geminiWs && geminiWs.readyState === WebSocket.OPEN) {
          geminiWs.send(JSON.stringify({
            client_content: {
              turns: [{ role: 'user', parts: [{ text: '[SYSTEM] No response received. Say exactly: "We could not reach you. A monitoring agent will follow up shortly. Goodbye."' }] }],
              turn_complete: true
            }
          }));
        }
        // Wait for farewell to play before closing
        setTimeout(() => {
          console.log('[Twilio-Gemini] Closing call (no response)');
          if (geminiWs && geminiWs.readyState === WebSocket.OPEN) geminiWs.close();
          if (ws.readyState === WebSocket.OPEN) ws.close();
        }, 8000);
      }
    }, 8000);
  }

  function clearWorkerSilenceTimer() {
    if (workerSilenceTimer) { clearTimeout(workerSilenceTimer); workerSilenceTimer = null; }
  }

  const MULAW_SILENCE = Buffer.alloc(160, 0x7F);

  function startSilenceKeepalive() {
    silenceInterval = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN && streamSid && sessionState === 'GREETING') {
        ws.send(JSON.stringify({
          event: 'media',
          streamSid: streamSid,
          media: { payload: MULAW_SILENCE.toString('base64') }
        }));
      } else {
        clearInterval(silenceInterval);
        silenceInterval = null;
      }
    }, 20);
  }

  function sendAudioToTwilio(mulawBuffer) {
    if (ws.readyState !== WebSocket.OPEN || !streamSid) return;
    const durMs = mulawBuffer.length / 8;
    agentPlaybackUntil = Math.max(agentPlaybackUntil, Date.now()) + durMs;
    ws.send(JSON.stringify({
      event: 'media',
      streamSid: streamSid,
      media: { payload: mulawBuffer.toString('base64') }
    }));
  }

  function connectGeminiTwilio() {
    const geminiUrl = `wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=${GEMINI_API_KEY}`;
    geminiWs = new WebSocket(geminiUrl);

    geminiWs.on('open', function() {
      console.log('[Twilio-Gemini] Gemini connected');
      geminiWs.send(JSON.stringify({
        setup: {
          model: `models/${GEMINI_MODEL}`,
          generation_config: {
            response_modalities: ['AUDIO'],
            speech_config: {
              voice_config: { prebuilt_voice_config: { voice_name: 'Charon' } },
            }
          },
          realtime_input_config: {
  automatic_activity_detection: {
    disabled: false,
    end_of_speech_sensitivity: "END_SENSITIVITY_LOW",
    start_of_speech_sensitivity: "START_SENSITIVITY_LOW",
    silence_duration_ms: 1500,
    prefix_padding_ms: 200,
  }
},
          input_audio_transcription: {},
          output_audio_transcription: {},
          system_instruction: { parts: [{ text: SYSTEM_PROMPT }] },
          tools: [{
            function_declarations: [
              {
                name: "report_worker_intent",
                description: "Report the classification of the worker\'s latest utterance. Call after EVERY worker utterance, before speaking.",
                parameters: {
                  type: "OBJECT",
                  properties: {
                    intent: { type: "STRING", enum: ["SAFE_CONFIRMATION", "DELAYED_CHECKIN", "REQUEST_HUMAN", "DISTRESS", "UNRECOGNISED"] },
                    confidence: { type: "NUMBER", description: "How clearly you heard AND understood the worker, 0 to 1."},
                    distress: { type: "BOOLEAN", description: "True if any injury, pain, fall or distress was mentioned."}
                  },
                  required: ["intent", "confidence"]
                }
              },
              {
                name: "finish_call",
                description: "Call right after saying a closing line, when the conversation is complete.",
                parameters: {
                  type: "OBJECT",
                  properties: {
                    outcome: { type: "STRING", enum: ["RESOLVED", "ESCALATED", "NO_ANSWER"]},
                    intent: { type: "STRING", enum: ["SAFE_CONFIRMATION", "DELAYED_CHECKIN", "REQUEST_HUMAN", "DISTRESS", "UNRECOGNISED"]}
                  },
                  required: ["outcome"]
                }
              }
            ]
          }]
        }
      }));
    });

    geminiWs.on('message', async function(data) {
      try {
        const msg = JSON.parse(data.toString());

        if (msg.error) {
          console.error('[Twilio-Gemini] API ERROR:', JSON.stringify(msg.error));
        }

        if (msg.setupComplete) {
          console.log('[Twilio-Gemini] Setup complete — sending greeting');
          twilioSetupTime = Date.now();
          pcmBuffer = Buffer.alloc(0);
          twilioAiTranscript = '';
          sessionState = 'GREETING';
          geminiWs.send(JSON.stringify({
            client_content: {
              turns: [{ role: 'user', parts: [{ text: `[SYSTEM] Say this exact greeting word for word and nothing else: "${TWILIO_WORKER_INTRO}"` }] }],
              turn_complete: true
            }
          }));
        }

        if (msg.serverContent) {
          const content = msg.serverContent;

          // Worker spoke — reset silence timer (only after greeting)
          if (content.inputTranscription && content.inputTranscription.text) {
            if (sessionState !== 'LISTENING') {
              console.log('[Twilio-Gemini] Ignoring worker audio during GREETING:', content.inputTranscription.text);
            } else {
            console.log('[Twilio-Gemini] Worker said:', content.inputTranscription.text);
            const workerText = content.inputTranscription.text;
            callLog.transcript.push({ role: 'worker', text: workerText, time: new Date().toISOString() });
            const wt = workerText.toLowerCase();
            if (/\bhuman\b|\bagent\b|connect me|need help|sos\b|help me/.test(wt)) {
              setCallOutcome('ESCALATED', 'REQUEST_HUMAN');
            } else if (/\bi'?m fine\b|\bi am fine\b|\ball good\b|\bsafe\b/.test(wt) && !/\bbut\b|\bnot\b|\bworried\b|\bhurt\b/.test(wt)) {
              callLog.finalIntent = callLog.finalIntent || 'SAFE_CONFIRMATION';
            } else if (/\blate\b|\bmore time\b|\bminutes?\b|\bdelay\b/.test(wt)) {
              callLog.finalIntent = callLog.finalIntent || 'DELAYED_CHECKIN';
            }
            silencePromptCount = 0;
            callLog.silencePromptCount = silencePromptCount;
            startWorkerSilenceTimer();
            }
          }

          // Collect AI transcript for resolution detection
          if (content.outputTranscription && content.outputTranscription.text) {
            twilioAiTranscript += content.outputTranscription.text;
          }

          if (content.modelTurn && content.modelTurn.parts) {
            for (const part of content.modelTurn.parts) {
              if (part.inlineData && part.inlineData.mimeType && part.inlineData.mimeType.startsWith('audio/')) {
                if (sessionState === 'GREETING' && isSpuriousUnclearResponse(twilioAiTranscript)) {
                  continue;
                }
                try {
                  if (!twilioFirstAudioTime) {
                    twilioFirstAudioTime = Date.now();
                    const ttfa = twilioFirstAudioTime - twilioSetupTime;
                    console.log('[Twilio-Gemini] FIRST AUDIO CHUNK | Time-to-first-audio (TTFA):', ttfa + 'ms');
                    callLog.ttfa_ms = ttfa;
                  }
                  if (silenceInterval) { clearInterval(silenceInterval); silenceInterval = null; }
                  const audioBuf = Buffer.from(part.inlineData.data, 'base64');
                  const mulawBuf = pcmToMulaw(audioBuf);
                  sendAudioToTwilio(mulawBuf);
                } catch(e) { console.error('[Twilio-Gemini] PCM→mulaw error:', e.message); }
              }
            }
          }
          if (content.turnComplete) {
            if (sessionState === "LISTENING") {
              const nm = noise.endTurn();
            console.log('[Gemini Noise] Turn committed | snr=' + (nm.snrDb != null ? nm.snrDb.toFixed(1) : 'n/a')
                + ' | floor=' + (nm.floorDbfs != null ? nm.floorDbfs.toFixed(1) : 'n/a') + ' | noisy=' + nm.noisy);
            if (!callLog.noiseTurns) callLog.noiseTurns = [];
            callLog.noiseTurns.push({
              time: new Date().toISOString(),
              snrDb: nm.snrDb,
              noiseLevelDbfs: nm.noiseLevelDbfs,
              speechDbfs: nm.speechDbfs,
              noisy: nm.noisy,
              snrReliable: nm.snrReliable
            });
          }
            // Check for resolution phrases after each AI turn (post-greeting)
            if (sessionState === 'LISTENING' && twilioAiTranscript) {
              const t = twilioAiTranscript.toLowerCase();
              console.log('[Twilio-Gemini] AI said:', twilioAiTranscript.trim());
              callLog.transcript.push({ role: 'ai', text: twilioAiTranscript.trim(), time: new Date().toISOString() });
              twilioAiTranscript = '';
              const resolutionPhrases = [
                'reset your safety timer',
                'extended your check-in timer',
                'flagging this for a live agent',
                'someone will call you back',
                'monitoring agent will follow up',
                'could not reach you'
              ];
              if (resolutionPhrases.some(p => t.includes(p))) {
                console.log('[Twilio-Gemini] Resolution detected — ending call in 4s');
                if (t.includes('could not reach you') || t.includes('monitoring agent will follow up')) {
                  setCallOutcome('NO_ANSWER', callLog.finalIntent || 'UNRECOGNISED');
                } else if (t.includes('flagging this for a live agent') || t.includes('someone will call you back')) {
                  setCallOutcome('ESCALATED', 'REQUEST_HUMAN');
                } else if (t.includes('extended your check-in timer')) {
                  setCallOutcome('RESOLVED', 'DELAYED_CHECKIN');
                } else if (t.includes('reset your safety timer')) {
                  setCallOutcome('RESOLVED', 'SAFE_CONFIRMATION');
                } else if (!callLog.finalState) {
                  setCallOutcome('RESOLVED', callLog.finalIntent || null);
                }
                clearWorkerSilenceTimer();
                setTimeout(() => {
                  console.log('[Twilio-Gemini] Closing call');
                  if (geminiWs && geminiWs.readyState === WebSocket.OPEN) geminiWs.close();
                  if (ws.readyState === WebSocket.OPEN) ws.close();
                }, 4000);
              }
            }
            if (sessionState === 'GREETING') {
              if (isSpuriousUnclearResponse(twilioAiTranscript)) {
                console.log('[Twilio-Gemini] Ignoring spurious pre-greeting response, re-sending intro');
                twilioAiTranscript = '';
                geminiWs.send(JSON.stringify({
                  client_content: {
                    turns: [{ role: 'user', parts: [{ text: `[SYSTEM] Say this exact greeting word for word and nothing else: "${TWILIO_WORKER_INTRO}"` }] }],
                    turn_complete: true
                  }
                }));
                return;
              }
              console.log('[Twilio-Gemini] Greeting audio done, waiting 2s before listening...');
              setTimeout(() => {
                pcmBuffer = Buffer.alloc(0);
                twilioAiTranscript = '';
                sessionState = 'LISTENING';
                console.log('[Twilio-Gemini] Now listening');
                startAudioFlush();
                startWorkerSilenceTimer();
              }, 2000);
            }
          }
        }
        if (msg.toolCall && msg.toolCall.functionCalls) {
          const responses = [];
          for (const fc of msg.toolCall.functionCalls) {
            if (fc.name === "report_worker_intent") {
              const args = fc.args || {};
              const conf = typeof args.confidence === "number" ? args.confidence : null;
              console.log('[Gemini Tool] intent:', args.intent, '| conf:', conf, '| distress:', args.distress);
              if (args.intent && args.intent !== "UNRECOGNISED") callLog.finalIntent = args.intent;
            } else if (fc.name === "finish_call") {
              const args = fc.args || {};
              console.log('[Gemini Tool] finish_call:', args.outcome, '|', args.intent);
              setCallOutcome(args.outcome || 'RESOLVED', args.intent || callLog.finalIntent);
              responses.push({ id: fc.id, name: fc.name, response: { status: "ok" } });
              clearWorkerSilenceTimer();
              setTimeout(() => {
                console.log("[Twilio-Gemini] Closing call (finish_call)");
                if (geminiWs && geminiWs.readyState === WebSocket.OPEN) geminiWs.close();
                if (ws.readyState === WebSocket.OPEN) ws.close();
              }, 4000);
            } else {
              responses.push({ id: fc.id, name: fc.name, response: { status: "ok" } });
            }
          }
          if ( responses.length && geminiWs && geminiWs.readyState === WebSocket.OPEN) {
            geminiWs.send(JSON.stringify({ tool_response: { function_responses: responses} }));
          }
        }
      } catch(e) { console.error('[Twilio-Gemini] Message error:', e.message); }
    });

    geminiWs.on('error', function(e) { console.error('[Twilio-Gemini] Error:', e.message); });
    geminiWs.on('close', function(code, reason) { console.log('[Twilio-Gemini] Gemini closed | code:', code, '| reason:', reason ? reason.toString() : 'none'); });
  }

  ws.on('message', async function(data) {
    try {
      const msg = JSON.parse(data.toString());

      if (msg.event === 'connected') { console.log('[Twilio-Gemini] Stream connected'); }

      if (msg.event === 'start') {
        streamSid = msg.start.streamSid;
        callLog.callSid = msg.start.callSid || streamSid;
        callLog.streamSid = streamSid;
        callLog.workerPhone = (msg.start.customParameters && msg.start.customParameters.caller)
          || msg.start.to
          || null;
        console.log('[Twilio-Gemini] Stream started | SID:', streamSid, '| Call:', callLog.callSid);

        try {
          insertCallStart({
            callSid: callLog.callSid,
            streamSid: streamSid,
            workerPhone: callLog.workerPhone,
            startTime: callLog.startTime,
            modelUsed: GEMINI_MODEL,
          });
        } catch (e) {
          console.error('[Twilio-Gemini] DB insert error:', e.message);
        }

        connectGeminiTwilio();
        startSilenceKeepalive();
      }

      if (msg.event === 'media') {
        const mulawChunk = Buffer.from(msg.media.payload, 'base64');
        noise.setAgentSpeaking(Date.now() < agentPlaybackUntil + 200);
        vad.processMulaw(mulawChunk);
        noise.pushFrame({ ...frameStatsMulaw(mulawChunk), speechProb: vad.prob() }, audioClockMs);
        audioClockMs += mulawChunk.length / 8;
        if (sessionState === "LISTENING") {
          try {
            const pcmChunk = mulawToPcm(mulawChunk);
            pcmBuffer = Buffer.concat([pcmBuffer, pcmChunk]);
          } catch (e) {
            console.error("[DEBUG] mulawToPcm error:", e.message);
          }
        }
      }

      if (msg.event === 'stop') {
        console.log('[Twilio-Gemini] Stream stopped');
        if (flushInterval) { clearInterval(flushInterval); flushInterval = null; }
        if (geminiWs && geminiWs.readyState === WebSocket.OPEN) geminiWs.close();
      }
    } catch(e) { console.error('[Twilio-Gemini WS Error]', e.message); }
  });

  ws.on('close', function() {
    console.log('[Twilio-Gemini WS] Disconnected');
    if (callLog.callSid) amdRedirectedCalls.delete(callLog.callSid);
    clearWorkerSilenceTimer();
    if (silenceInterval) { clearInterval(silenceInterval); silenceInterval = null; }
    if (flushInterval) { clearInterval(flushInterval); flushInterval = null; }
    if (geminiWs && geminiWs.readyState === WebSocket.OPEN) geminiWs.close();

    // Save call log to file + database
    try {
      const logsDir = path.join(__dirname, 'call_logs');
      if (!fs.existsSync(logsDir)) fs.mkdirSync(logsDir);
      callLog.endTime = new Date().toISOString();
      callLog.silencePromptCount = silencePromptCount;
      const duration = Math.round((new Date(callLog.endTime) - new Date(callLog.startTime)) / 1000);
      let finalSessionState = callLog.finalState || (silencePromptCount >= 2 ? 'NO_ANSWER' : 'UNKNOWN');
      if (callLog.callSid) {
        const existing = getCallBySid(callLog.callSid);
        if (existing && existing.session_state === 'VOICEMAIL') {
          finalSessionState = 'VOICEMAIL';
        }
      }

      const filename = path.join(logsDir, `call_${callLog.callSid || Date.now()}.json`);
      fs.writeFileSync(filename, JSON.stringify(callLog, null, 2));
      console.log('[Twilio-Gemini] Call log saved:', filename);

      if (callLog.callSid) {
        updateCallEnd({
          callSid: callLog.callSid,
          streamSid: callLog.streamSid,
          workerPhone: callLog.workerPhone,
          endTime: callLog.endTime,
          durationSeconds: duration,
          sessionState: finalSessionState,
          finalIntent: callLog.finalIntent,
          recordingUrl: callLog.recordingUrl,
          transcript: callLog.transcript,
          silenceCount: callLog.silencePromptCount,
          ttfaMs: callLog.ttfa_ms,
        });
        console.log('[Twilio-Gemini] Call saved to DB:', callLog.callSid, '|', finalSessionState);
      }
    } catch(e) {
      console.error('[Twilio-Gemini] Log save error:', e.message);
    }
  });
});

// ─── Twilio TwiML endpoint ────────────────────────────────────────────────────
app.post('/outbound-gemini', function(req, res) {
  const url = buildTwilioWebhookUrl(req, '/outbound-gemini');
  if (!validateTwilioSignature(req, res, url)) return;

  const baseUrl = process.env.GEMINI_PUBLIC_URL || process.env.PUBLIC_URL || '';
  const wsUrl = baseUrl.replace('https://', 'wss://').replace(/\/$/, '') + '/twilio-gemini';
  const workerPhone = (req.body && (req.body.To || req.body.to)) || '';
  console.log('[TwiML] Stream URL:', wsUrl, '| To:', workerPhone);
  res.set('Content-Type', 'text/xml');
  res.send(`<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="${wsUrl}">
      <Parameter name="caller" value="${workerPhone}"/>
    </Stream>
  </Connect>
  <Pause length="30"/>
</Response>`);
});

app.post('/call-status', function(req, res) {
  const url = buildTwilioWebhookUrl(req, '/call-status');
  if (!validateTwilioSignature(req, res, url)) return;

  console.log('[Call Status]', req.body.CallStatus, '| SID:', req.body.CallSid);
  res.sendStatus(200);
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

app.post('/amd-status', function(req, res) {
  const url = buildTwilioWebhookUrl(req, '/amd-status');
  if (!validateTwilioSignature(req, res, url)) return;

  const { CallSid, AnsweredBy } = req.body || {};
  console.log('[AMD] Call:', CallSid, '| Answered by:', AnsweredBy);

  if (AnsweredBy && AnsweredBy.startsWith('machine') && CallSid) {
    if (amdRedirectedCalls.has(CallSid)) {
      return res.sendStatus(200);
    }
    amdRedirectedCalls.add(CallSid);

    console.log('[AMD] Voicemail detected — will leave message');
    try {
      updateCallSessionState(CallSid, 'VOICEMAIL');
      console.log('[AMD] DB updated: VOICEMAIL for', CallSid);
    } catch (e) {
      console.error('[AMD] DB error:', e.message);
    }

    if (twilioClient) {
      const baseUrl = process.env.GEMINI_PUBLIC_URL || process.env.PUBLIC_URL || '';
      twilioClient.calls(CallSid).update({
        url: `${baseUrl.replace(/\/$/, '')}/voicemail-twiml`,
        method: 'POST',
      }).then(() => {
        console.log('[AMD] Redirected to voicemail TwiML for call:', CallSid);
      }).catch((err) => {
        console.error('[AMD] Redirect error:', err.message);
        amdRedirectedCalls.delete(CallSid);
      });
    } else {
      console.error('[AMD] Twilio client not configured');
    }
  } else if (AnsweredBy === 'unknown') {
    console.log('[AMD] Unknown — not redirecting, Gemini flow continues');
  } else if (AnsweredBy === 'human') {
    console.log('[AMD] Human answered — Gemini flow continues');
  }

  res.sendStatus(200);
});

app.post('/voicemail-twiml', function(req, res) {
  const url = buildTwilioWebhookUrl(req, '/voicemail-twiml');
  if (!validateTwilioSignature(req, res, url)) return;

  const voicemailMessage = 'Hello, this is an automated safety check from Aware360. ' +
    'We noticed you missed your scheduled check-in. ' +
    'Please call back or check in through the app as soon as possible. ' +
    'If this is an emergency, please contact your supervisor. Thank you.';

  res.set('Content-Type', 'text/xml');
  res.send(`<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Pause length="2"/>
  <Say voice="Polly.Joanna" language="en-US">${voicemailMessage}</Say>
  <Pause length="1"/>
  <Hangup/>
</Response>`);
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
  res.json({
    status: 'ok',
    mode: 'gemini',
    apis: { gemini: !!GEMINI_API_KEY },
    twilio_ws: (process.env.PUBLIC_URL || '').replace('https://', 'wss://') + '/twilio-gemini',
    twilio_twiml: (process.env.PUBLIC_URL || '') + '/outbound-gemini'
  });
});

const PORT = process.env.GEMINI_PORT || 3001;
server.listen(PORT, function() {
  console.log(`\n🤖  Gemini Live Demo running at http://localhost:${PORT}`);
  console.log(`Model: ${GEMINI_MODEL}`);
  console.log(`Twilio WS: ${(process.env.PUBLIC_URL || '').replace('https://', 'wss://')}/twilio-gemini`);
  console.log(`Twilio TwiML: ${process.env.PUBLIC_URL || ''}/outbound-gemini\n`);
});